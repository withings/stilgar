/// Fields common to all events

use crate::events::context::Context;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// Fields common to all events
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonFields {
    pub anonymous_id: String,
    pub channel: String,
    pub context: Context,
    #[serde(default)]
    pub received_at: Option<DateTime<Utc>>,
    pub original_timestamp: DateTime<Utc>,
    pub sent_at: DateTime<Utc>,
    pub integrations: HashMap<String, serde_json::Value>,
    pub message_id: String,
}
