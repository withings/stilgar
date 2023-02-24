/// Group event

use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// A group event, as sent to /v1/group
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Group {
    #[serde(flatten)]
    common: CommonFields,

    group_id: String,
    traits: HashMap<String, serde_json::Value>,
}
