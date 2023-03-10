use crate::events::common::CommonFields;

use serde::{Deserialize, Serialize};

/// An identify event, as sent to /v1/identify
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identify {
    #[serde(flatten)]
    pub common: CommonFields,
}
