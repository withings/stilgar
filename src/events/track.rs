use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// An track event, as sent to /v1/track
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Track {
    #[serde(flatten)]
    common: CommonFields,

    event: String,
    properties: HashMap<String, serde_json::Value>,
}
