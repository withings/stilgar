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

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Context fields
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(default)]
    pub app: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub campaign: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub device: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub library: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub network: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub os: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub screen: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub traits: HashMap<String, serde_json::Value>,
}
