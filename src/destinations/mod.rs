pub mod blackhole;
pub mod clickhouse;

use crate::forwarder::SuspendTrigger;

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
use thiserror::Error;
use tokio::sync::mpsc;
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

/// The Destination trait, all destinations must implement this
#[async_trait]
pub trait Destination: Display {
    async fn new(settings: &config::Settings, suspend_trigger: SuspendTrigger) -> Result<Arc<Self>, StorageError> where Self: Sized;
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
pub async fn init_destinations(destination_configs: &Vec<config::Destination>, suspend_tx: mpsc::Sender<()>) -> Result<Destinations, StorageError> {
    let mut destinations: Vec<Arc<dyn Destination>> = vec!();
    for destination_config in destination_configs.iter() {
        match destination_config.destination_type.as_str() {
            "blackhole" => destinations.push(Blackhole::new(&destination_config.settings, suspend_tx.clone()).await?),
            "clickhouse" => destinations.push(Clickhouse::new(&destination_config.settings, suspend_tx.clone()).await?),
            other => return Err(StorageError::Initialisation(format!("unknown destination type: {}", other)))
        }
    }
    Ok(destinations)
}
