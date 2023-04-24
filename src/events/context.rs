use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Context fields
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(default)]
    pub app: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub campaign: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub device: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub library: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub network: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub os: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub screen: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub traits: HashMap<String, serde_json::Value>,
}
