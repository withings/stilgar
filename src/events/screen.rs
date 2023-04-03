use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// A screen event, as sent to /v1/screen
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Screen {
    #[serde(flatten)]
    pub common: CommonFields,

    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, serde_json::Value>,
}
