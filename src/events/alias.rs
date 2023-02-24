use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};

/// An alias event, as sent to /v1/alias
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alias {
    #[serde(flatten)]
    common: CommonFields,

    #[serde(default)]
    user_id: Option<String>,
    previous_id: String,
}
