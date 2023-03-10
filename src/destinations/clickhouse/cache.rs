use crate::destinations::clickhouse::{Clickhouse, StorageResult, StorageError};
use crate::destinations::clickhouse::primitives::{GenericQuery, InsertQuery};

use std::fmt::Debug;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use twox_hash::XxHash64;
use tokio::sync::{mpsc, oneshot};
use itertools::Itertools;
use log;

/// Number of events which need to accumulate in cache before write queries happen
const CACHE_THRESHOLD: usize = 1000;

/// Interesting Clickhouse error codes
const ERR_NO_SUCH_TABLE: i64 = 60;
const ERR_NO_SUCH_COLUMN_IN_TABLE: i64 = 16;

/// Convenience type: a row, as submitted by store_*()
pub type Row = HashMap<String, Option<String>>;

/// An insert command for the cache channel
#[derive(Debug)]
pub struct CacheInsert {
    pub table: String,
    pub row: Row,
}

/// An insert cache entry
#[derive(Debug)]
struct InsertEntry {
    /// The table it targets
    table: String,
    /// The columns available in rows
    columns: Vec<String>,
    /// Said rows
    rows: Vec<Vec<Option<String>>>,
}

/// The insert cache itself, a fancy Hashmap with a fast hash
type InsertCacheMap = HashMap<String, InsertEntry, BuildHasherDefault<XxHash64>>;

impl Clickhouse {
    /// Inserts a row into the cache, sending it over the mpsc channel to the cache task
    pub async fn insert(&self, table: String, row: Row) -> () {
        self.cache.send(CacheInsert { table, row }).await
            .expect("failed to push row to cache channel");
    }

    /// Registersto write a cache entry to Clickhouse
    async fn try_write(&self, cache_entry: &InsertEntry) -> StorageResult {
        /* Prepare the actual query and a channel to send rows */
        let columns = cache_entry.columns.join(", ");
        let sql = format!("INSERT INTO {} ({})", cache_entry.table, columns);
        let (rows_tx, rows_rx) = mpsc::channel::<Vec<Option<String>>>(32); // TODO 32?
        let (write_tx, write_rx) = oneshot::channel::<StorageResult>();
        let write_query = InsertQuery {
            query: sql,
            rows_channel: rows_rx,
            return_tx: write_tx
        };

        log::debug!("will try to write cache entry ({} row(s)) for columns: {}", cache_entry.rows.len(), &columns);

        /* Send the query to the query task */
        self.query.send(GenericQuery::Insert(write_query)).await.expect("failed to send write query across query channel");
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
        let mut insert_cache: InsertCacheMap = Default::default();
        let mut cache_size = 0 as usize;

        while let Some(insert) = rx.recv().await {
            /* Sort row values by column name */
            let sorted_row = insert.row.iter().sorted_by(|kv1, kv2| kv1.0.cmp(&kv2.0)).collect_vec();
            let columns = sorted_row.iter().map(|kv| kv.0.clone()).collect::<Vec<String>>();
            let values = sorted_row.iter().map(|kv| kv.1.clone()).collect::<Vec<Option<String>>>();

            /* Find a cache entry for those columns, create it if necessary */
            let cache_key = insert.table.clone() + "," + &columns.join(",");
            let entry = insert_cache.entry(cache_key).or_insert_with(|| { InsertEntry {
                table: insert.table.clone(),
                columns,
                rows: vec![]
            }});
            entry.rows.push(values); /* add the row to cache */
            cache_size += 1;

            if cache_size < CACHE_THRESHOLD {
                continue;
            }

            log::debug!("cache size threshold reached, will write to Clickhouse");
            self.flush_insert_cache(&insert_cache).await;
            insert_cache.clear();
            cache_size = 0;
            log::debug!("in-memory cache cleared");
        }
    }

    /// Flushes the insert cache to Clickhouse once the threshold has been reached
    async fn flush_insert_cache(&self, insert_cache: &InsertCacheMap) {
        for cache_entry in insert_cache.values() {
            /* Try to write the cache entry to Clickhouse */
            let mut write_result = self.try_write(cache_entry).await;

            if let Err(StorageError::QueryFailure(code, _, _)) = write_result {
                /* The table does not exist (track) : create it and try again */
                if code == ERR_NO_SUCH_TABLE {
                    log::debug!("missing table: {}, will try to create", cache_entry.table);
                    self.nio(Self::get_basic_table_ddl(&cache_entry.table)).await
                        .expect("failed to create table dynamically");
                    write_result = self.try_write(cache_entry).await;
                }
            }

            if let Err(StorageError::QueryFailure(code, _, msg)) = write_result {
                /* At least one column is missing, add it and try again
                 * This is very likely to happen after CREATE TABLE above */
                if code == ERR_NO_SUCH_COLUMN_IN_TABLE {
                    /* Infer types for all columns in this entry */
                    let entry_columns = cache_entry.columns.iter().enumerate()
                        .map(|(i, c)| (c.clone(), Self::infer_vec_type(cache_entry.rows.iter().map(|r| &r[i]))))
                        .collect::<HashMap<String, String>>();

                    /* Extend the table if necessary */
                    if let Err(e) = self.extend_existing_table(&cache_entry.table, entry_columns).await {
                        log::error!("failed to extend table {}: {}", cache_entry.table, e.to_string());
                        continue;
                    }

                    /* Try storing again */
                    if let Err(e) = self.try_write(cache_entry).await {
                        log::error!("failed to insert rows after table alterations on {}, will skip block: {}",
                                    cache_entry.table, e.to_string());
                    }
                } else {
                    log::error!("unexpected Clickhouse error: {} {}", code, msg);
                }
            }
        }
    }
}
