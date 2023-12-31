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

pub mod blackhole;
pub mod clickhouse;

use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;

use crate::destinations::blackhole::Blackhole;
use crate::destinations::clickhouse::Clickhouse;
use crate::config;

use std::sync::Arc;
use std::fmt::Display;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use async_trait::async_trait;

/// Enum used by all destinations to report errors
/// Feel free to extend this as needed, but try to avoid being too
/// specific, as Stilgar's forwarded should stay simple and never
/// treat a destination type in a special fashion
#[derive(Error, Debug, Clone)]
pub enum StorageError {
    /// Error in ::new(), getting the destination ready
    #[error("failed to initialise destination: {0}")]
    Initialisation(String),

    /// Network connectivity issue
    #[error("connection issue: {0}")]
    Connectivity(String),

    /// Interaction/query failure, pass a code, query and error message
    #[error("query failed: {2} ({0}: {1})")]
    QueryFailure(i64, String, String),

    /// Growth control errors
    #[error("growth control error: {0}")]
    GrowthControl(String),
}

/// Convenience type: storage result (reply given to the forwarder when storing)
pub type StorageResult = Result<(), StorageError>;

/// Convenience types: destination statistics hashmap
pub type DestinationStatistics = HashMap<String, serde_json::Value>;

/// The Destination trait, all destinations must implement this
#[async_trait]
pub trait Destination: Display {
    async fn new(write_keys: HashSet<String>, settings: &config::Settings) -> Result<Arc<Self>, StorageError> where Self: Sized;

    fn error_is_critical(&self, err: &StorageError) -> bool;
    fn matches_write_key(&self, subject_key: &String) -> bool;

    async fn stats(&self) -> Result<DestinationStatistics, StorageError>;
    async fn flush(&self);

    async fn alias(&self, alias: &Alias) -> StorageResult;
    async fn group(&self, group: &Group) -> StorageResult;
    async fn identify(&self, identify: &Identify) -> StorageResult;
    async fn store_page(&self, page: &Page) -> StorageResult;
    async fn store_screen(&self, screen: &Screen) -> StorageResult;
    async fn store_track(&self, track: &Track) -> StorageResult;
}

/// Convenience type: destinations array
pub type Destinations = Vec<Arc<dyn Destination>>;

/// Provides an array of destination structs based on the configuration file
pub async fn init_destinations(destination_configs: &Vec<config::Destination>) -> Result<Destinations, StorageError> {
    let mut destinations: Vec<Arc<dyn Destination>> = vec!();
    for destination_config in destination_configs.iter() {
        let write_keys = destination_config.write_keys.clone();
        let settings = &destination_config.settings;
        match destination_config.destination_type.as_str() {
            "blackhole" => destinations.push(Blackhole::new(write_keys, settings).await?),
            "clickhouse" => destinations.push(Clickhouse::new(write_keys, settings).await?),
            other => return Err(StorageError::Initialisation(format!("unknown destination type: {}", other)))
        }
    }
    Ok(destinations)
}
