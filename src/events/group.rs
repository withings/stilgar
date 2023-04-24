/// Group event

use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;

/// A group event, as sent to /v1/group
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Group {
    #[serde(flatten)]
    pub common: CommonFields,

    #[serde(default)]
    pub user_id: Option<String>,
    pub group_id: String,
    #[serde(default)]
    pub traits: HashMap<String, serde_json::Value>,
}
