use crate::destinations::clickhouse::{Clickhouse, StorageResult, StorageError};
use crate::destinations::clickhouse::primitives::{GenericQuery, WriteQuery};
use crate::destinations::clickhouse::ddl;

use std::fmt::Debug;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use twox_hash::XxHash64;
use tokio::sync::{mpsc, oneshot};
use itertools::Itertools;
use chrono::DateTime;
use log;

/// Number of events which need to accumulate in cache before write queries happen
const CACHE_THRESHOLD: usize = 1000;

/// Interesting Clickhouse error codes
const ERR_NO_SUCH_TABLE: i64 = 60;
const ERR_NO_SUCH_COLUMN_IN_TABLE: i64 = 16;

/// Convenience type: a row, as submitted by store_*()
pub type Row = HashMap<String, Option<String>>;

/// A cache insert command for the cache channel
#[derive(Debug)]
pub struct CacheInsert {
    pub table: String,
    pub row: Row,
}

/// A cache entry
#[derive(Debug)]
struct CacheEntry {
    /// The table it targets
    table: String,
    /// The columns available in rows
    columns: Vec<String>,
    /// Said rows
    rows: Vec<Vec<Option<String>>>,
}

/// The cache itself, a fancy Hashmap with a fast hash
type CacheMap = HashMap<String, CacheEntry, BuildHasherDefault<XxHash64>>;

impl Clickhouse {
    /// Inserts a row into the cache, sending it over the mpsc channel to the cache task
    pub async fn insert(&self, table: String, row: Row) -> () {
        self.cache.send(CacheInsert { table, row }).await.expect("failed to push row to cache channel");
    }

    /// Tries to write a cache entry to Clickhouse
    async fn try_write(&self, cache_entry: &CacheEntry) -> StorageResult {
        /* Prepare the actual query and a channel to send rows */
        let columns = cache_entry.columns.join(", ");
        let sql = format!("INSERT INTO {} ({})", cache_entry.table, columns);
        let (rows_tx, rows_rx) = mpsc::channel::<Vec<Option<String>>>(32); // TODO 32?
        let (write_tx, write_rx) = oneshot::channel::<StorageResult>();
        let write_query = WriteQuery {
            query: sql,
            rows_channel: rows_rx,
            return_tx: write_tx
        };

        log::debug!("will try to write cache entry ({} row(s)) for columns: {}", cache_entry.rows.len(), &columns);

        /* Send the query to the query task */
        self.query.send(GenericQuery::Write(write_query)).await.expect("failed to send write query across query channel");
        let mut rows_cloned = cache_entry.rows.clone();
        while let Some(row) = rows_cloned.pop() {
            /* Send each row over the row channel */
            rows_tx.send(row).await.expect("failed to send row for write query");
        }

        /* This terminates the channel and lets the query task know we're done */
        drop(rows_tx);

        /* Wait for the query task's response */
        write_rx.await.expect("failed to receive write result from query channel")
    }

    /// Runs the cache channel, which owns the actual cache hashmap
    pub async fn run_cache_channel(&self, mut rx: mpsc::Receiver<CacheInsert>) {
        let mut cache: CacheMap = Default::default();
        let mut cache_size = 0 as usize;

        while let Some(insert) = rx.recv().await {
            /* Sort row values by column name */
            let sorted_row = insert.row.iter().sorted_by(|kv1, kv2| kv1.0.cmp(&kv2.0)).collect_vec();
            let columns = sorted_row.iter().map(|kv| kv.0.clone()).collect::<Vec<String>>();
            let values = sorted_row.iter().map(|kv| kv.1.clone()).collect::<Vec<Option<String>>>();

            /* Find a cache entry for those columns, create it if necessary */
            let cache_key = insert.table.clone() + "," + &columns.join(",");
            let entry = cache.entry(cache_key).or_insert_with(|| { CacheEntry {
                table: insert.table.clone(),
                columns,
                rows: vec![]
            }});
            cache_size += 1;
            entry.rows.push(values); /* add the row to cache */

            if cache_size < CACHE_THRESHOLD {
                continue;
            }

            log::debug!("cache size threshold reached, will write to Clickhouse");

            for cache_entry in cache.values() {
                /* Try to write the cache entry to Clickhouse */
                let write_result = self.try_write(cache_entry).await;
                if let Err(StorageError::QueryFailure(code, _, _)) = write_result {
                    /* The table does not exist (track) : create it and try again */
                    if code == ERR_NO_SUCH_TABLE {
                        log::debug!("missing table: {}, will try to create", cache_entry.table);

                        self.ddl(Self::entry_to_table_ddl(&cache_entry)).await
                            .expect("failed to create table dynamically");

                        let rewrite_result = self.try_write(cache_entry).await;
                        if let Err(e) = rewrite_result {
                            log::error!("failed to insert rows into newly-created table {}, will skip block: {}",
                                        cache_entry.table, e.to_string());
                        }
                    }

                    /* At least one column is missing, add it and try again */
                    if code == ERR_NO_SUCH_COLUMN_IN_TABLE {
                        /* Figure out which columns are missing */
                        let current_columns = self.describe_table(&cache_entry.table).await.expect("failed to describe table");
                        let missing_columns = cache_entry.columns.iter().enumerate()
                            .filter(|(_, c)| !current_columns.keys().contains(c))
                            .map(|(i, _)| i).collect_vec();

                        /* Create them */
                        for column_idx in missing_columns {
                            let inferred_type = Self::infer_vec_type(cache_entry.rows.iter().map(|r| &r[column_idx]));
                            let sql = format!("ALTER TABLE {} ADD COLUMN {} {}",
                                              cache_entry.table, cache_entry.columns[column_idx], inferred_type);
                            log::debug!("creating missing column: {}.{} {}", cache_entry.table, cache_entry.columns[column_idx], inferred_type);
                            self.ddl(sql).await.expect("failed to alter table dynamically");
                        }

                        /* Try again */
                        let rewrite_result = self.try_write(cache_entry).await;
                        if let Err(e) = rewrite_result {
                            log::error!("failed to insert rows after table alterations on {}, will skip block: {}",
                                        cache_entry.table, e.to_string());
                        }
                    }
                }
            }

            cache.clear();
            cache_size = 0;

            log::debug!("in-memory cache cleared");
        }
    }

    /// Designs a table which who be able to store a cache entry's rows
    fn entry_to_table_ddl(entry: &CacheEntry) -> String {
        let fields_sql = entry.columns.iter().enumerate()
            .map(|(i, c)| format!("{} {}", c, Self::infer_vec_type(entry.rows.iter().map(|r| &r[i]))))
            .join(", ");

        format!(
            "CREATE TABLE {} ({} {} {}) ENGINE = ReplicatedMergeTree ENGINE = ReplacingMergeTree PARTITION BY toDate(received_at) ORDER BY (received_at, id)",
            entry.table,
            ddl::EVENT_BASICS.to_string(),
            ddl::CONTEXT.to_string(),
            fields_sql
        )
    }

    /// Infer a column's type using known values
    fn infer_vec_type<'a, I>(values: I) -> String
    where
        I: Iterator<Item=&'a Option<String>>
    {
        values.map(|v| Self::infer_value_type(&v))
            .max_by(|v1, v2| v1.0.cmp(&v2.0)) /* pick broadest type */
            .expect("trying to infer a column type without sample values").1
    }

    /// Infers a value's Clickhouse type
    /// Each type is returned with a "breadth" value: low breadth
    /// means the type is very specific, high breadth means you could
    /// store many other types in there
    fn infer_value_type(value: &Option<String>) -> (u8, String) {
        match value {
            Some(v) => {
                let parsed = if let Ok(_) = v.parse::<i64>() {
                    (1, "Nullable(Int64)")
                } else if let Ok(_) = v.parse::<f64>() {
                    (2, "Nullable(Float64)")
                } else if v == "true" || v == "false" {
                    (3, "Nullable(Boolean)")
                } else if let Ok(_) = DateTime::parse_from_rfc2822(&v) {
                    (4, "Nullable(DateTime64(1))")
                } else if let Ok(_) = DateTime::parse_from_rfc3339(&v) {
                    (5, "Nullable(DateTime64(1))")
                } else {
                    (6, "Nullable(String)")
                };
                (parsed.0, parsed.1.into())
            },
            None => (0, "Nullable(String)".into())
        }
    }
}
