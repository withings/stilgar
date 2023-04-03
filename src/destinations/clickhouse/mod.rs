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
use regex::Regex;

/// Clickhouse destination
pub struct Clickhouse {
    query: mpsc::Sender<primitives::GenericQuery>,
    cache: mpsc::Sender<cache::CacheInsert>,
    database: String,
    username: String,
    password: String,
    cache_threshold: usize,
    max_table_expansion: usize,
    max_table_width: usize,
    identifier_regex: Regex,
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
        let cache_threshold = settings
            .get("cache_threshold")
            .map(|v| v.as_u64().map(|u| u as usize))
            .unwrap_or(Some(cache::DEFAULT_CACHE_THRESHOLD))
            .ok_or(StorageError::Initialisation("cache_threshold parameter should be a positive number".to_string()))?;
        let max_table_expansion = settings
            .get("max_table_expansion")
            .map(|v| v.as_u64().map(|u| u as usize))
            .unwrap_or(Some(cache::DEFAULT_MAX_TABLE_EXPANSION))
            .ok_or(StorageError::Initialisation("max_table_expansion parameter should be a positive number".to_string()))?;
        let max_table_width = settings
            .get("max_table_width")
            .map(|v| v.as_u64().map(|u| u as usize))
            .unwrap_or(Some(cache::DEFAULT_MAX_TABLE_WIDTH))
            .ok_or(StorageError::Initialisation("max_table_width parameter should be a positive number".to_string()))?;

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
            cache_threshold,
            max_table_expansion,
            max_table_width,
            identifier_regex: Regex::new(r"^[a-zA-Z_][0-9a-zA-Z_]*$").expect("invalid Clickhouse idenifier REGEX"),
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

    /// Sends an alias event to cache
    async fn alias(&self, alias: &Alias) -> StorageResult {
        let new_id = alias.user_id.as_ref().unwrap_or(&alias.common.anonymous_id);
        if new_id == &alias.previous_id {
            log::debug!("user IDs are identical in alias call, ignoring");
            return Ok(())
        }

        let mut kv = Self::map_common_fields(&alias.common);
        kv.insert("user_id".into(), alias.user_id.as_ref().map(|i| i.clone()));
        kv.insert("previous_id".into(), Some(alias.previous_id.clone()));
        self.insert("aliases".into(), kv).await;
        Ok(())
    }

    /// Sends a group mapping to cache
    async fn group(&self, group: &Group) -> StorageResult {
        let mut kv = Self::map_common_fields(&group.common);
        kv.insert("user_id".into(), group.user_id.as_ref().map(|i| i.clone()));
        kv.insert("group_id".into(), Some(group.group_id.clone()));
        for (key, value) in &group.traits {
            kv.insert(format!("context_traits_{}", key), Self::json_to_string(value));
        }
        self.insert("groups".into(), kv).await;
        Ok(())
    }

    /// Sends an identify event to cache and updates the users table
    async fn identify(&self, identify: &Identify) -> StorageResult {
        let mut kv = Self::map_common_fields(&identify.common);
        for (key, value) in &identify.common.context.traits {
            kv.insert(format!("context_traits_{}", key), Self::json_to_string(value));
        }
        let mut users_kv = kv.clone(); /* we don't want user_id in the users table */

        /* store the identify event itself, with 'user_id' set */
        kv.insert("user_id".into(), Some(identify.user_id.clone()));
        self.insert("identifies".into(), kv).await;

        /* upsert the user, with 'id' this time (table is an AggregatingMergeTree) */
        users_kv.insert("id".into(), Some(identify.user_id.clone()));
        self.insert("users".into(), users_kv).await;
        Ok(())
    }

    /// Sends a page event to cache
    async fn store_page(&self, page: &Page) -> StorageResult {
        let mut kv = Self::map_common_fields(&page.common);
        kv.insert("user_id".into(), page.user_id.as_ref().map(|i| i.clone()));
        kv.insert("name".into(), page.name.as_ref().map(|n| n.clone()));
        kv.insert("category".into(), page.category.as_ref().map(|n| n.clone()));
        for (key, value) in &page.properties {
            kv.insert(key.into(), Self::json_to_string(value));
        }
        self.insert("pages".into(), kv).await;
        Ok(())
    }

    /// Sends a screen event to cache
    async fn store_screen(&self, screen: &Screen) -> StorageResult {
        let mut kv = Self::map_common_fields(&screen.common);
        kv.insert("user_id".into(), screen.user_id.as_ref().map(|i| i.clone()));
        kv.insert("name".into(), screen.name.as_ref().map(|n| n.clone()));
        kv.insert("category".into(), screen.category.as_ref().map(|n| n.clone()));
        for (key, value) in &screen.properties {
            kv.insert(key.into(), Self::json_to_string(value));
        }
        self.insert("screens".into(), kv).await;
        Ok(())
    }

    /// Sends a custom (track) event to cache
    async fn store_track(&self, track: &Track) -> StorageResult {
        let mut track_kv = Self::map_common_fields(&track.common);
        track_kv.insert("event".into(), Some(track.event.clone()));
        track_kv.insert("user_id".into(), track.user_id.as_ref().map(|i| i.clone()));
        let mut subtrack_kv = track_kv.clone();
        self.insert("tracks".into(), track_kv).await;
        for (key, value) in &track.properties {
            subtrack_kv.insert(key.into(), Self::json_to_string(value));
        }
        self.insert(track.event.clone(), subtrack_kv).await; // TODO do not trust!
        Ok(())
    }
}

impl Display for Clickhouse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "clickhouse:{}", self.database)
    }
}
