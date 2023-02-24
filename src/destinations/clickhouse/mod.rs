mod ddl;
mod grpc;
mod primitives;
mod cache;

use crate::destinations::{Destination, StorageResult, StorageError};
use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;
use crate::config::Settings;

use std::sync::Arc;
use std::fmt::Display;
use async_trait::async_trait;
use tokio::sync::mpsc;
use log;

/// Clickhouse destination
pub struct Clickhouse {
    query: mpsc::Sender<primitives::GenericQuery>,
    cache: mpsc::Sender<cache::CacheInsert>,
    database: String,
    username: String,
    password: String,
}

/// Implementing the Destination trait for Clickhouse
#[async_trait]
impl Destination for Clickhouse {
    /// Create a Clickhouse destination and connects to the database
    async fn new(settings: &Settings) -> Result<Arc<Self>, StorageError> {
        let host = settings
            .get("host").ok_or(StorageError::Initialisation("missing host parameter".to_string()))?
            .as_str().ok_or(StorageError::Initialisation("host parameter should be a string".to_string()))?;
        let port = settings
            .get("port")
            .map(|v| v.as_str())
            .flatten().unwrap_or("9100");
        let username = settings
            .get("user").ok_or(StorageError::Initialisation("missing user parameter".to_string()))?
            .as_str().ok_or(StorageError::Initialisation("user parameter should be a string".to_string()))?;
        let password = settings
            .get("password").ok_or(StorageError::Initialisation("missing password parameter".to_string()))?
            .as_str().ok_or(StorageError::Initialisation("password parameter should be a string".to_string()))?;
        let database = settings
            .get("database").ok_or(StorageError::Initialisation("missing database parameter".to_string()))?
            .as_str().ok_or(StorageError::Initialisation("database parameter should be a string".to_string()))?;

        let client = grpc::Client::connect(format!("http://{}:{}", host, port)).await
            .map_err(|e| StorageError::Connectivity(e.to_string()))?;

        log::debug!("connected to clickhouse at {}:{}", host, port);

        let (query_tx, query_rx) = mpsc::channel(32); // TODO 32?
        let (cache_tx, cache_rx) = mpsc::channel(32); // TODO 32?
        let clickhouse = Self {
            query: query_tx,
            cache: cache_tx,
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
        };
        let clickhouse_arc = Arc::new(clickhouse);

        /* Query task: owns the TCP stream, takes in queries as messages */
        let clickhouse_query = clickhouse_arc.clone();
        tokio::task::spawn(async move {
            clickhouse_query.run_query_channel(client, query_rx).await;
        });

        /* Cache task: owns the cache, takes in write requests are messages */
        let clickhouse_cache = clickhouse_arc.clone();
        tokio::task::spawn(async move {
            clickhouse_cache.run_cache_channel(cache_rx).await;
        });

        /* Create default tables if they don't exist yet */
        clickhouse_arc.create_tables().await?;

        Ok(clickhouse_arc)
    }

    /// TODO: process alias event
    async fn alias(&self, alias: &Alias) -> StorageResult {
        Ok(())
    }

    /// TODO: process group event
    async fn group(&self, group: &Group) -> StorageResult {
        Ok(())
    }

    /// TODO: process identify event
    async fn identify(&self, identify: &Identify) -> StorageResult {
        Ok(())
    }

    /// Sends a page event to cache
    async fn store_page(&self, page: &Page) -> StorageResult {
        let mut kv = Self::map_common_fields(&page.common);
        kv.insert("name".into(), page.name.as_ref().map(|n| n.clone()));
        for (key, value) in &page.properties {
            kv.insert(key.into(), Self::json_to_string(value));
        }
        self.insert("pages".into(), kv).await;
        Ok(())
    }

    /// TODO: process screen event
    async fn store_screen(&self, screen: &Screen) -> StorageResult {
        Ok(())
    }

    /// TODO: process track event
    async fn store_track(&self, track: &Track) -> StorageResult {
        Ok(())
    }
}

impl Display for Clickhouse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "clickhouse:{}", self.database)
    }
}
