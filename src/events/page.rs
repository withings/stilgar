use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json;

/// A page event, as sent to /v1/page
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    #[serde(flatten)]
    pub common: CommonFields,

    #[serde(default)]
    pub name: Option<String>,
    pub properties: HashMap<String, serde_json::Value>,
}
