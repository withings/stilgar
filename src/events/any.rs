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

use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

macro_rules! set_common_attribute {
    ($event:expr, $field:ident, $value:expr) => {
        match $event {
            AnyEvent::Alias(e) => e.common.$field = $value,
            AnyEvent::Group(e) => e.common.$field = $value,
            AnyEvent::Identify(e) => e.common.$field = $value,
            AnyEvent::Page(e) => e.common.$field = $value,
            AnyEvent::Screen(e) => e.common.$field = $value,
            AnyEvent::Track(e) => e.common.$field = $value,
        };
    };
}

pub(crate) use set_common_attribute;

/// Convenience enum: can accept any event
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum AnyEvent {
    #[serde(rename = "alias")]
    Alias(Alias),
    #[serde(rename = "group")]
    Group(Group),
    #[serde(rename = "identify")]
    Identify(Identify),
    #[serde(rename = "page")]
    Page(Page),
    #[serde(rename = "screen")]
    Screen(Screen),
    #[serde(rename = "track")]
    Track(Track),
}

/// A batch event, as sent to /v1/batch
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub batch: Vec<AnyEvent>,
    #[serde(default)]
    pub sent_at: Option<DateTime<Utc>>,
}

/// Convenience enum: accepts any event or a batch of events
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum EventOrBatch {
    Event(AnyEvent),
    Batch(Batch),
}

impl AnyEvent {
    pub fn message_id(&self) -> String {
        match self {
            AnyEvent::Alias(e) => e.common.message_id.clone(),
            AnyEvent::Group(e) => e.common.message_id.clone(),
            AnyEvent::Identify(e) => e.common.message_id.clone(),
            AnyEvent::Page(e) => e.common.message_id.clone(),
            AnyEvent::Screen(e) => e.common.message_id.clone(),
            AnyEvent::Track(e) => e.common.message_id.clone(),
        }
    }
}

impl EventOrBatch {
    pub fn message_id(&self) -> String {
        match self {
            EventOrBatch::Event(e) => e.message_id(),
            EventOrBatch::Batch(b) => b.batch[0].message_id(),
        }
    }
}
