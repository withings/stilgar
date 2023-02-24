use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// An identify event, as sent to /v1/identify
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identify {
    #[serde(flatten)]
    common: CommonFields,

    user_id: String,
    traits: HashMap<String, serde_json::Value>,
}
