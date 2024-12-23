/* stilgar - a lightweight, no-fuss, drop-in replacement for Rudderstack
 * Copyright (C) 2023 Withings
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>. */

mod ddl;
mod grpc;
mod primitives;
mod cache;

use crate::destinations::{Destination, StorageResult, StorageError, DestinationStatistics};
use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;
use crate::config::Settings;

use std::sync::Arc;
use std::fmt::Display;
use std::time::Duration;
use std::collections::HashSet;
use async_trait::async_trait;
use tokio::sync::{oneshot, mpsc};
use regex::Regex;
use humantime;
use log;

/// Clickhouse error codes which can trigger a forwarder backoff
const SUSPEND_ERROR_CODES: [i64; 35] = [
    3,    /* UNEXPECTED_END_OF_FILE */
    74,   /* CANNOT_READ_FROM_FILE_DESCRIPTOR */
    75,   /* CANNOT_WRITE_TO_FILE_DESCRIPTOR */
    76,   /* CANNOT_WRITE_TO_FILE_DESCRIPTOR */
    77,   /* CANNOT_CLOSE_FILE */
    87,   /* CANNOT_SEEK_THROUGH_FILE */
    88,   /* CANNOT_TRUNCATE_FILE */
    95,   /* CANNOT_READ_FROM_SOCKET */
    96,   /* CANNOT_WRITE_TO_SOCKET */
    159,  /* TIMEOUT_EXCEEDED */
    160,  /* TOO_SLOW */
    164,  /* READONLY */
    172,  /* CANNOT_CREATE_DIRECTORY */
    173,  /* CANNOT_ALLOCATE_MEMORY */
    198,  /* DNS_ERROR */
    201,  /* QUOTA_EXCEEDED */
    202,  /* TOO_MANY_SIMULTANEOUS_QUERIES */
    203,  /* NO_FREE_CONNECTION */
    204,  /* CANNOT_FSYNC */
    209,  /* SOCKET_TIMEOUT */
    210,  /* NETWORK_ERROR */
    236,  /* ABORTED */
    241,  /* MEMORY_LIMIT_EXCEEDED */
    242,  /* TABLE_IS_READ_ONLY */
    243,  /* NOT_ENOUGH_SPACE */
    274,  /* AIO_READ_ERROR */
    275,  /* AIO_WRITE_ERROR */
    290,  /* LIMIT_EXCEEDED */
    298,  /* CANNOT_PIPE */
    299,  /* CANNOT_FORK */
    301,  /* CANNOT_CREATE_CHILD_PROCESS */
    303,  /* CANNOT_SELECT */
    335,  /* BARRIER_TIMEOUT */
    373,  /* SESSION_IS_LOCKED */
    1002, /* UNKNOWN_ERROR */
];

/// Clickhouse destination
pub struct Clickhouse {
    write_keys: HashSet<String>,
    query: mpsc::Sender<primitives::GenericQuery>,
    cache: mpsc::Sender<cache::CacheMessage>,
    database: String,
    username: String,
    password: String,
    cache_threshold: usize,
    cache_idle_timeout: Duration,
    max_table_expansion: usize,
    max_table_width: usize,
    identifier_regex: Regex,
}

/// Implementing the Destination trait for Clickhouse
#[async_trait]
impl Destination for Clickhouse {
    /// Create a Clickhouse destination and connects to the database
    async fn new(write_keys: HashSet<String>, settings: &Settings) -> Result<Arc<Self>, StorageError> {
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
        let cache_idle_timeout = settings
            .get("cache_idle_timeout")
            .map(|v| v.as_str().map(|s| humantime::parse_duration(s).ok()).flatten())
            .unwrap_or(Some(cache::DEFAULT_CACHE_IDLE_THRESHOLD))
            .ok_or(StorageError::Initialisation("cache_idle_timeout parameter should be a valid duration".to_string()))?;
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

        let url = format!("http://{}:{}", host, port);
        log::debug!("connected to clickhouse at {}", url);

        let (query_tx, query_rx) = mpsc::channel(32); // TODO 32?
        let (cache_tx, cache_rx) = mpsc::channel(32); // TODO 32?
        let clickhouse = Self {
            write_keys,
            query: query_tx,
            cache: cache_tx,
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            cache_threshold,
            cache_idle_timeout,
            max_table_expansion,
            max_table_width,
            identifier_regex: Regex::new(r"^[a-zA-Z_][0-9a-zA-Z_]*$").expect("invalid Clickhouse idenifier REGEX"),
        };
        let clickhouse_arc = Arc::new(clickhouse);

        /* Query task: owns the TCP stream, takes in queries as messages */
        let clickhouse_query = clickhouse_arc.clone();
        tokio::task::spawn(async move {
            clickhouse_query.run_query_channel(url, query_rx).await;
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

    /// Have the forwarder back off on connectivity issues and some return codes
    fn error_is_critical(&self, err: &StorageError) -> bool {
        match err {
            StorageError::Connectivity(_) => true,
            StorageError::QueryFailure(code, _, _) => SUSPEND_ERROR_CODES.contains(code),
            _ => false,
        }
    }

    /// Matches in the hashset without further processing
    fn matches_write_key(&self, subject_key: &String) -> bool {
        self.write_keys.contains(subject_key)
    }

    /// Provides statistics for /status
    async fn stats(&self) -> Result<DestinationStatistics, StorageError> {
        let metrics = self.get_basic_metrics().await;

        Ok(match metrics {
            Ok(m) => DestinationStatistics::from([
                ("status".into(), "OK".into()),
                ("active_queries".into(), serde_json::json!(m.active_queries)),
                ("active_mutations".into(), serde_json::json!(m.active_mutations)),
                ("delayed_inserts".into(), serde_json::json!(m.delayed_inserts)),
                ("async_insert_active_threads".into(), serde_json::json!(m.async_insert_active_threads)),
                ("pending_async_inserts".into(), serde_json::json!(m.pending_async_inserts)),
            ]),
            Err(e) => DestinationStatistics::from([
                ("status".into(), serde_json::json!(e.to_string()))
            ])
        })
    }

    /// Flushes the write cache upon request
    async fn flush(&self) {
        let (return_tx, return_rx) = oneshot::channel::<StorageResult>();
        self.cache.send(cache::CacheMessage::Flush(return_tx)).await.expect("failed to push flush message to cache channel");
        return_rx.await.ok(); /* flush is a last chance thing, we don't care if it fails */
    }

    /// Sends an alias event to cache
    async fn alias(&self, alias: &Alias) -> StorageResult {
        let new_id = alias.user_id.as_ref().unwrap_or(&alias.common.anonymous_id);
        if new_id == &alias.previous_id {
            log::debug!(
                mid = alias.common.message_id;
                "user IDs are identical in alias call, ignoring"
            );
            return Ok(())
        }

        let mut kv = Self::map_common_fields(&alias.common);
        kv.insert("user_id".into(), alias.user_id.as_ref().map(|i| i.clone()));
        kv.insert("previous_id".into(), Some(alias.previous_id.clone()));
        self.insert("aliases".into(), kv).await
    }

    /// Sends a group mapping to cache
    async fn group(&self, group: &Group) -> StorageResult {
        let mut kv = Self::map_common_fields(&group.common);
        kv.insert("user_id".into(), group.user_id.as_ref().map(|i| i.clone()));
        kv.insert("group_id".into(), Some(group.group_id.clone()));
        for (key, value) in &group.traits {
            kv.insert(format!("context_traits_{}", key), Self::json_to_string(value));
        }
        self.insert("groups".into(), kv).await
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
        self.insert("identifies".into(), kv).await?;

        /* upsert the user, with 'id' this time (table is an AggregatingMergeTree) */
        users_kv.insert("id".into(), Some(identify.user_id.clone()));
        self.insert("users".into(), users_kv).await
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
        self.insert("pages".into(), kv).await
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
        self.insert("screens".into(), kv).await
    }

    /// Sends a custom (track) event to cache
    async fn store_track(&self, track: &Track) -> StorageResult {
        let mut track_kv = Self::map_common_fields(&track.common);
        track_kv.insert("event".into(), Some(track.event.clone()));
        track_kv.insert("user_id".into(), track.user_id.as_ref().map(|i| i.clone()));
        let mut subtrack_kv = track_kv.clone();
        self.insert("tracks".into(), track_kv).await?;
        for (key, value) in &track.properties {
            subtrack_kv.insert(key.into(), Self::json_to_string(value));
        }
        self.insert(Self::make_valid_identifier(&track.event), subtrack_kv).await
    }
}

impl Display for Clickhouse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "clickhouse:{}", self.database)
    }
}
