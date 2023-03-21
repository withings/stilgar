use crate::events::common::CommonFields;
use crate::destinations::clickhouse::grpc;
use crate::destinations::clickhouse::{Clickhouse, StorageResult, StorageError};

use std::fmt::Debug;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use async_stream::stream;
use serde_json;

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

    /// Executes a non-interactive query and returns the response
    async fn nio_query(client: &mut grpc::Client, query: grpc::QueryInfo) -> StorageResult {
        let execute = client.execute_query(query.clone()).await
            .map_err(|e| StorageError::Connectivity(e.to_string()))?;

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
        let mut execute = client.execute_query_with_stream_output(query.clone()).await
            .map_err(|e| StorageError::Connectivity(e.to_string()))?;
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
        let execute = client.execute_query_with_stream_input(stream).await
            .map_err(|e| StorageError::Connectivity(e.to_string()))?;

        match &execute.get_ref().exception {
            Some(e) => Err(StorageError::QueryFailure(e.code as i64, query_str, e.display_text.clone())),
            None => Ok(())
        }
    }

    /// Query task: receives query messages and processes them
    pub async fn run_query_channel(&self, mut client: grpc::Client, mut rx: mpsc::Receiver<GenericQuery>) {
        log::debug!("running clickhouse channel for {}", self.database);

        while let Some(command) = rx.recv().await {
            match command {
                GenericQuery::NIO(ddl) => {
                    let query = self.prepare_base(ddl.query);
                    ddl.return_tx.send(Self::nio_query(&mut client, query).await)
                        .expect("failed to forward read query result to querying task");
                },
                GenericQuery::Select(read) => {
                    let query = self.prepare_select(&read);
                    read.return_tx.send(Self::run_select_query(&mut client, query).await)
                        .expect("failed to forward read query result to querying task");
                },
                GenericQuery::Insert(write) => {
                    let query = self.prepare_insert(&write);
                    write.return_tx.send(Self::run_insert_query(&mut client, query, write.rows_channel).await)
                        .expect("failed to forward write query result to querying task");
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

    /// Maps known event fields (CommonFields) to string for TSV input
    pub fn map_common_fields(common: &CommonFields) -> HashMap<String, Option<String>> {
        HashMap::from([
            ("anonymous_id".into(), Some(common.anonymous_id.clone())),
            ("channel".into(), Some(common.channel.clone())),
            ("received_at".into(), Some(common.received_at.expect("missing received_at field in event after processing").timestamp().to_string())),
            ("original_timestamp".into(), Some(common.original_timestamp.timestamp().to_string())),
            ("sent_at".into(), Some(common.sent_at.timestamp().to_string())),
            ("id".into(), Some(common.message_id.clone())),

            ("context_app_name".into(), common.context.app.as_ref().map(|a| a.name.clone())),
            ("context_app_version".into(), common.context.app.as_ref().map(|a| a.version.clone())),
            ("context_app_build".into(), common.context.app.as_ref().map(|a| a.build.as_ref().map(|b| b.clone())).flatten()),

            ("context_campaign_name".into(), common.context.campaign.as_ref().map(|c| c.name.as_ref().map(|n| n.clone())).flatten()),
            ("context_campaign_source".into(), common.context.campaign.as_ref().map(|c| c.source.as_ref().map(|s| s.clone())).flatten()),
            ("context_campaign_medium".into(), common.context.campaign.as_ref().map(|c| c.medium.as_ref().map(|m| m.clone())).flatten()),
            ("context_campaign_term".into(), common.context.campaign.as_ref().map(|c| c.term.as_ref().map(|t| t.clone())).flatten()),
            ("context_campaign_content".into(), common.context.campaign.as_ref().map(|c| c.content.as_ref().map(|ct| ct.clone())).flatten()),

            ("context_device_type".into(), common.context.device.as_ref().map(|d| d.device_type.clone())),
            ("context_device_id".into(), common.context.device.as_ref().map(|d| d.id.clone())),
            ("context_device_advertising_id".into(), common.context.device.as_ref().map(|d| d.advertising_id.clone())),
            ("context_device_ad_tracking_enabled".into(), common.context.device.as_ref().map(|d| d.ad_tracking_enabled.to_string())),
            ("context_device_manufacturer".into(), common.context.device.as_ref().map(|d| d.manufacturer.clone())),
            ("context_device_model".into(), common.context.device.as_ref().map(|d| d.model.clone())),
            ("context_device_name".into(), common.context.device.as_ref().map(|d| d.name.clone())),

            ("context_library_name".into(), Some(common.context.library.name.clone())),
            ("context_library_version".into(), Some(common.context.library.name.clone())),

            ("context_locale".into(), Some(common.context.locale.clone())),

            ("context_network_bluetooth".into(), common.context.network.as_ref().map(|n| n.bluetooth.clone())),
            ("context_network_carrier".into(), common.context.network.as_ref().map(|n| n.carrier.clone())),
            ("context_network_cellular".into(), common.context.network.as_ref().map(|n| n.cellular.clone())),
            ("context_network_wifi".into(), common.context.network.as_ref().map(|n| n.wifi.clone())),

            ("context_os_name".into(), common.context.os.as_ref().map(|o| o.name.clone())),
            ("context_os_version".into(), common.context.os.as_ref().map(|o| o.version.clone())),

            ("context_screen_density".into(), Some(common.context.screen.density.to_string())),
            ("context_screen_width".into(), Some(common.context.screen.width.to_string())),
            ("context_screen_height".into(), Some(common.context.screen.height.to_string())),
            ("context_screen_inner_width".into(), common.context.screen.inner_width.as_ref().map(|w| w.to_string())),
            ("context_screen_inner_height".into(), common.context.screen.inner_height.as_ref().map(|w| w.to_string())),

            ("context_timezone".into(), common.context.timezone.as_ref().map(|t| t.clone())),
            ("context_user_agent".into(), common.context.user_agent.as_ref().map(|u| u.clone())),
        ]).into()
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
