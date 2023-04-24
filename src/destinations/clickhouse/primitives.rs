use crate::events::common::CommonFields;
use crate::destinations::clickhouse::grpc;
use crate::destinations::clickhouse::{Clickhouse, StorageResult, StorageError};

use std::fmt::Debug;
use std::collections::HashMap;
use tokio;
use tokio::sync::{mpsc, oneshot};
use async_stream::stream;

/// Network timeout when communicating with Clickhouse
const NETWORK_TIMEOUT: u64 = 5;

/// A query without I/O (no input rows like INSERT or results like SELECT)
#[derive(Debug)]
pub struct NonInteractiveQuery {
    /// The query string
    pub query: String,
    /// A channel for the response
    pub return_tx: oneshot::Sender<StorageResult>,
}

/// Convenience type: rows as return by the query task for a SELECT
pub type ResultSet = Vec<Vec<String>>;
/// Convenience type: return type for a read query message
pub type SelectResult = Result<ResultSet, StorageError>;
/// A read query (SELECT)
#[derive(Debug)]
pub struct SelectQuery {
    /// The query string
    query: String,
    /// A channel for the response
    return_tx: oneshot::Sender<SelectResult>,
}

/// Convenience type: row receiver (INSERTs)
type InsertQueryReceiver = mpsc::Receiver<Vec<Option<String>>>;
/// An INSERT query
#[derive(Debug)]
pub struct InsertQuery {
    /// The query string
    pub query: String,
    /// A channel to receive rows as a stream
    pub rows_channel: InsertQueryReceiver,
    /// A channel for the response
    pub return_tx: oneshot::Sender<StorageResult>,
}

/// An enum for any query sent over the channel
#[derive(Debug)]
pub enum GenericQuery {
    NIO(NonInteractiveQuery),
    Select(SelectQuery),
    Insert(InsertQuery),
}

/// Struct containing basic activity statistics from Clickhouse
pub struct BasicMetrics {
    pub active_queries: Option<u64>,
    pub active_mutations: Option<u64>,
    pub delayed_inserts: Option<u64>,
    pub async_insert_active_threads: Option<u64>,
    pub pending_async_inserts: Option<u64>,
}

impl Clickhouse {
    /// Convenience function: prepares a QueryInfo object for any query string
    fn prepare_base(&self, query_str: String) -> grpc::QueryInfo {
        let mut query_info = grpc::QueryInfo::default();
        query_info.user_name = self.username.clone();
        query_info.password = self.password.clone();
        query_info.database = self.database.clone();
        query_info.query = query_str;
        query_info
    }

    /// Convenience function: prepares a QueryInfo object for a SELECT
    fn prepare_select(&self, query: &SelectQuery) -> grpc::QueryInfo {
        self.prepare_base(format!("{} FORMAT TSV", query.query.clone()))
    }

    /// Convenience function: prepares a QueryInfo object for an INSERT
    fn prepare_insert(&self, query: &InsertQuery) -> grpc::QueryInfo {
        let mut query_info = self.prepare_base(format!("{} FORMAT TSV", query.query.clone()));
        query_info.input_data_delimiter = vec![10u8];
        query_info.next_query_info = true;
        query_info
    }

    /// Convenience function: returns a query timeout future
    fn network_timeout_future() -> tokio::time::Sleep {
        tokio::time::sleep(tokio::time::Duration::from_secs(NETWORK_TIMEOUT))
    }

    /// Executes a non-interactive query and returns the response
    async fn nio_query(client: &mut grpc::Client, query: grpc::QueryInfo) -> StorageResult {
        let query_future = client.execute_query(query.clone());
        let execute = tokio::select! {
            _ = Self::network_timeout_future() => return Err(StorageError::Connectivity("query timeout".into())),
            exec_result = query_future => exec_result.map_err(|e| StorageError::Connectivity(e.to_string()))?,
        };

        match &execute.get_ref().exception {
            Some(e) => Err(StorageError::QueryFailure(e.code as i64, query.query.clone(), e.display_text.clone())),
            None => Ok(())
        }
    }

    /// Executes a read query (SELECT)
    ///
    /// Stilgar hardly even does SELECTs and only does so on small
    /// result sets. For this reason, this function does not stream the
    /// results: it waits for everything and returns the lot as a Vec
    async fn run_select_query(client: &mut grpc::Client, query: grpc::QueryInfo) -> SelectResult {
        let query_future = client.execute_query_with_stream_output(query.clone());
        let mut execute = tokio::select! {
            _ = Self::network_timeout_future() => return Err(StorageError::Connectivity("query timeout".into())),
            exec_result = query_future => exec_result.map_err(|e| StorageError::Connectivity(e.to_string()))?,
        };

        let stream = execute.get_mut();
        let mut rows: ResultSet = vec!();

        /* Fetch blocks from Clickhouse */
        while let Some(block) = stream.message().await.map_err(|e| StorageError::Connectivity(e.to_string()))? {
            if let Some(e) = block.exception {
                return Err(StorageError::QueryFailure(e.code as i64, query.query.clone(), e.display_text.clone()));
            }

            /* TSV parsing: Clickhouse handles the escaping nicely, we only need to watch out for tabs
             * TODO: reverse the escaping after parsing? */
            let block_payload = String::from_utf8(block.output)
                .map_err(|e| StorageError::QueryFailure(0, e.to_string(), query.query.clone()))?;
            let mut block_rows: ResultSet = block_payload
                .split('\n')
                .filter(|line| line.len() > 0)
                .map(|row| row.split('\t').map(|column| String::from(column)).collect())
                .collect();
            rows.append(&mut block_rows);
        }

        Ok(rows)
    }

    /// Executes a write query (INSERT)
    ///
    /// Writing can involve a lot more rows than reading, so Stilgar
    /// uses a stream here. The user task is expected to provide a
    /// mpsc channel over which it will send the rows one by one.
    async fn run_insert_query(
        client: &mut grpc::Client,
        initial: grpc::QueryInfo,
        mut rows_channel: InsertQueryReceiver) -> StorageResult {
        let query_str = initial.query.clone();

        /* Transform the channel into a stream understood by the gRPC functions
         * There's a bit of trickery here to manage EOF properly */
        let stream = stream! {
            let mut current = Some(initial);
            let mut next = rows_channel.recv().await;

            while let Some(send_now) = &current {
                let mut query_info = send_now.clone();
                query_info.next_query_info = match next {
                    Some(_) => true,
                    None => false
                };

                yield query_info;

                match next {
                    Some(row) => {
                        let mut next_current = grpc::QueryInfo::default();

                        /* Prepare the TSV block: escaping just what the Clickhouse docs recommend we do */
                        let row_tsv = row.into_iter()
                            .map(|o| o.map(|s| s.replace("\t", "\\t").replace("\n", "\\n").replace("\\", "\\\\")))
                            .map(|o| o.unwrap_or(String::from("\\N")))
                            .collect::<Vec<_>>().join("\t");

                        next_current.input_data = row_tsv.into();
                        current = Some(next_current);
                        next = rows_channel.recv().await;
                    },
                    None => {
                        current = None;
                    }
                }
            };
        };

        /* Actually run the query */
        let query_future = client.execute_query_with_stream_input(stream);
        let execute = tokio::select! {
            _ = Self::network_timeout_future() => return Err(StorageError::Connectivity("query timeout".into())),
            exec_result = query_future => exec_result.map_err(|e| StorageError::Connectivity(e.to_string()))?,
        };

        match &execute.get_ref().exception {
            Some(e) => Err(StorageError::QueryFailure(e.code as i64, query_str, e.display_text.clone())),
            None => Ok(())
        }
    }

    async fn get_client(url: String) -> Result<grpc::Client, StorageError> {
        tokio::select! {
            _ = Self::network_timeout_future() => return Err(StorageError::Connectivity("connect timeout".into())),
            connect_result = grpc::Client::connect(url) => connect_result.map_err(|e| StorageError::Connectivity(e.to_string())),
        }
    }

    /// Query task: receives query messages and processes them
    pub async fn run_query_channel(&self, url: String, mut rx: mpsc::Receiver<GenericQuery>) {
        log::debug!("running clickhouse channel for {}", self.database);
        let mut client = Self::get_client(url.clone()).await.expect("failed to connect to Clickhouse");
        log::debug!("connection to {} established", url);

        while let Some(command) = rx.recv().await {
            let error = match command {
                GenericQuery::NIO(ddl) => {
                    let result = Self::nio_query(&mut client, self.prepare_base(ddl.query)).await;
                    let error = result.as_ref().err().map(|e| e.clone());
                    ddl.return_tx.send(result).expect("failed to forward read query result to querying task");
                    error
                },
                GenericQuery::Select(read) => {
                    let result = Self::run_select_query(&mut client, self.prepare_select(&read)).await;
                    let error = result.as_ref().err().map(|e| e.clone());
                    read.return_tx.send(result).expect("failed to forward read query result to querying task");
                    error
                },
                GenericQuery::Insert(write) => {
                    let result = Self::run_insert_query(&mut client, self.prepare_insert(&write), write.rows_channel).await;
                    let error = result.as_ref().err().map(|e| e.clone());
                    write.return_tx.send(result).expect("failed to forward write query result to querying task");
                    error
                },
            };

            if let Some(err) = error {
                if let StorageError::Connectivity(_) = err {
                    match Self::get_client(url.clone()).await {
                        Ok(new_client) => {
                            client = new_client;
                            log::info!("successfully reconnected to clickhouse");
                        },
                        Err(e) => log::warn!("failed to reconnect to clickhouse, will keep current connection: {}", e)
                    }
                }
            }
        }
    }

    /// Convenience function: run a non-interactive query, forget about messages and channels
    pub async fn nio(&self, query: String) -> StorageResult {
        let (tx, rx) = oneshot::channel::<StorageResult>();
        self.query.send(GenericQuery::NIO(NonInteractiveQuery { query , return_tx: tx })).await
            .expect("failed to communicate non-interactive query to the clickhouse query task");
        rx.await.expect("failed to receive non-interactive query result from the clickhouse query task")
    }

    /// Convenience function: run a SELECT, forget about messages and channels
    pub async fn select(&self, query: String) -> SelectResult {
        let (tx, rx) = oneshot::channel::<SelectResult>();
        self.query.send(GenericQuery::Select(SelectQuery { query, return_tx: tx })).await
            .expect("failed to communicate read query to the clickhouse channel runner");
        rx.await.expect("failed to receive read query result from clickhouse channel runner")
    }

    /// Check that a table exists in Clickhouse
    pub async fn table_exists(&self, table_name: &str) -> Result<bool, StorageError> {
        let query = format!(
            "SELECT 1 FROM system.tables WHERE database = '{}' AND name = '{}'",
            self.database, table_name
        );

        match self.select(query).await?.get(0) {
            Some(r) => Ok(r.get(0).map(|r| r == "1").unwrap_or(false)),
            None => Ok(false)
        }
    }

    /// Fetches a name/type mapping for the columns of a table
    pub async fn describe_table(&self, table_name: &str) -> Result<HashMap<String, String>, StorageError> {
        let query = format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}'",
            self.database, table_name
        );

        Ok(
            self.select(query).await?.into_iter()
                .map(|row| (row[0].clone(), row[1].clone()))
                .collect()
        )
    }

    /// Fetches a name/type mapping for the columns of a table
    pub async fn table_is_aggregating(&self, table_name: &str) -> Result<bool, StorageError> {
        let query = format!(
            "SELECT engine FROM system.tables WHERE database = '{}' AND table = '{}'",
            self.database, table_name
        );

        let engines = self.select(query.clone()).await?;
        if engines.is_empty() {
            return Err(StorageError::QueryFailure(0, format!("table {} does not exist", table_name), query));
        }

        Ok(engines[0][0].starts_with("Aggregating"))
    }

    /// Fetches some basic activity statistics
    pub async fn get_basic_metrics(&self) -> Result<BasicMetrics, StorageError> {
        let query = format!(
            "SELECT metric, value FROM system.metrics WHERE metric IN ('Query', 'PartMutation', 'DelayedInserts', 'AsynchronousInsertThreadsActive', 'PendingAsyncInsert')"
        );

        let stats: HashMap<String, String> = self.select(query).await?.into_iter()
            .map(|row| (row[0].clone(), row[1].clone()))
            .collect();

        Ok(BasicMetrics {
            active_queries: stats.get("Query").map(|s| s.parse::<u64>().ok()).flatten(),
            active_mutations: stats.get("PartMutation").map(|s| s.parse::<u64>().ok()).flatten(),
            delayed_inserts: stats.get("DelayedInserts").map(|s| s.parse::<u64>().ok()).flatten(),
            async_insert_active_threads: stats.get("AsynchronousInsertThreadsActive").map(|s| s.parse::<u64>().ok()).flatten(),
            pending_async_inserts: stats.get("PendingAsyncInsert").map(|s| s.parse::<u64>().ok()).flatten(),
        })
    }

    fn map_context_entries(entries: &HashMap<String, serde_json::Value>, prefix: &str) -> HashMap<String, Option<String>> {
        entries.iter().map(|(k, v)| (format!("context_{}_{}", prefix, k), Self::json_to_string(v))).collect()
    }

    /// Maps known event fields (CommonFields) to string for TSV input
    pub fn map_common_fields(common: &CommonFields) -> HashMap<String, Option<String>> {
        let mut common_hashmap = HashMap::from([
            ("anonymous_id".into(), Some(common.anonymous_id.clone())),
            ("channel".into(), Some(common.channel.clone())),
            ("received_at".into(), Some(common.received_at.expect("missing received_at field in event after processing").timestamp().to_string())),
            ("original_timestamp".into(), Some(common.original_timestamp.timestamp().to_string())),
            ("sent_at".into(), common.sent_at.map(|d| d.timestamp().to_string())),
            ("id".into(), Some(common.message_id.clone())),
            ("context_locale".into(), common.context.locale.clone()),
            ("context_timezone".into(), common.context.timezone.clone()),
            ("context_user_agent".into(), common.context.user_agent.clone()),
        ]);

        common_hashmap.extend(Self::map_context_entries(&common.context.app, "app"));
        common_hashmap.extend(Self::map_context_entries(&common.context.campaign, "campaign"));
        common_hashmap.extend(Self::map_context_entries(&common.context.device, "device"));
        common_hashmap.extend(Self::map_context_entries(&common.context.library, "library"));
        common_hashmap.extend(Self::map_context_entries(&common.context.network, "network"));
        common_hashmap.extend(Self::map_context_entries(&common.context.os, "os"));
        common_hashmap.extend(Self::map_context_entries(&common.context.screen, "screen"));

        common_hashmap
    }

    /// Convert any JSON value to a string for TSV input
    pub fn json_to_string(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Null => None,
            v => Some(v.to_string())
        }
    }
}
