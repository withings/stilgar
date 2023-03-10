use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};

/// An alias event, as sent to /v1/alias
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alias {
    #[serde(flatten)]
    pub common: CommonFields,

    #[serde(default)]
    pub user_id: Option<String>,
    pub previous_id: String,
}
