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
use std::collections::{HashMap, HashSet};
use async_trait::async_trait;
use serde_json;

/// Does nothing, needs nothing
pub struct Blackhole {
    write_keys: HashSet<String>,
}

impl Display for Blackhole {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str("blackhole")
    }
}

#[async_trait]
impl Destination for Blackhole {
    /// Returns pretty much nothing
    async fn new(write_keys: HashSet<String>, _settings: &Settings) -> Result<Arc<Self>, StorageError> {
        Ok(Arc::new(Blackhole {
            write_keys
        }))
    }

    /// Provides no statistics
    async fn stats(&self) -> Result<DestinationStatistics, StorageError> {
        Ok(HashMap::from([
            ("status".into(), serde_json::json!("OK"))
        ]))
    }

    /// Does nothing
    async fn flush(&self) {
        ()
    }

    /// Matches in the hashset without further processing
    fn matches_write_key(&self, subject_key: &String) -> bool {
        self.write_keys.contains(subject_key)
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
