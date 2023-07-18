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

use crate::events::context::Context;

use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

fn ok_or_default<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: de::Deserialize<'de> + Default,
    D: de::Deserializer<'de>,
{
    let v: serde_json::Value = de::Deserialize::deserialize(deserializer)?;
    Ok(T::deserialize(v).unwrap_or_default())
}

/// Fields common to all events
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommonFields {
    pub anonymous_id: String,
    pub channel: String,
    pub context: Context,
    #[serde(default)]
    pub received_at: Option<DateTime<Utc>>,
    #[serde(alias = "timestamp", deserialize_with = "ok_or_default")]
    pub original_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sent_at: Option<DateTime<Utc>>,
    pub integrations: HashMap<String, serde_json::Value>,
    pub message_id: String,
}
