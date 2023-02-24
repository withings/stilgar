use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// A screen event, as sent to /v1/screen
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Screen {
    #[serde(flatten)]
    common: CommonFields,

    #[serde(default)]
    name: Option<String>,
    properties: HashMap<String, serde_json::Value>,
}
