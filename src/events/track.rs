use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// An track event, as sent to /v1/track
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Track {
    #[serde(flatten)]
    pub common: CommonFields,

    #[serde(default)]
    pub user_id: Option<String>,
    pub event: String,
    #[serde(default)]
    pub properties: HashMap<String, serde_json::Value>,
}
