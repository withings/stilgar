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

/// Does nothing, needs nothing
pub struct Blackhole {}

impl Display for Blackhole {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str("blackhole")
    }
}

#[async_trait]
impl Destination for Blackhole {
    /// Returns pretty much nothing
    async fn new(_settings: &Settings) -> Result<Arc<Self>, StorageError> {
        Ok(Arc::new(Blackhole{}))
    }

    /// Nothing is critical
    fn error_is_critical(&self, _err: &StorageError) -> bool {
        false
    }

    /// Does nothing, successfully
    async fn alias(&self, _alias: &Alias) -> StorageResult {
        Ok(())
    }

    /// Does nothing, successfully
    async fn group(&self, _group: &Group) -> StorageResult {
        Ok(())
    }

    /// Does nothing, successfully
    async fn identify(&self, _identify: &Identify) -> StorageResult {
        Ok(())
    }

    /// Does nothing, successfully
    async fn store_page(&self, _page: &Page) -> StorageResult {
        Ok(())
    }

    /// Does nothing, successfully
    async fn store_screen(&self, _screen: &Screen) -> StorageResult {
        Ok(())
    }

    /// Does nothing, successfully
    async fn store_track(&self, _track: &Track) -> StorageResult {
        Ok(())
    }
}
