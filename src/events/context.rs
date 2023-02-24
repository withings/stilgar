use serde::{Deserialize, Serialize};

/// Context fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(default)]
    pub app: Option<App>,
    #[serde(default)]
    pub campaign: Option<Campaign>,
    #[serde(default)]
    pub device: Option<Device>,
    pub library: Library,
    pub locale: String,
    #[serde(default)]
    pub network: Option<Network>,
    #[serde(default)]
    pub os: Option<OS>,
    pub screen: Screen,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
}

/// App-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct App {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub build: Option<String>,
}

/// Campaign-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Campaign {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub medium: Option<String>,
    #[serde(default)]
    pub term: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
}

/// Device-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    #[serde(rename = "type")]
    pub device_type: String,
    pub id: String,
    pub advertising_id: String,
    pub ad_tracking_enabled: bool,
    pub manufacturer: String,
    pub model: String,
    pub name: String,
}

/// Library-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Library {
    pub name: String,
    pub version: String,
}

/// Network-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    pub bluetooth: String,
    pub carrier: String,
    pub cellular: String,
    pub wifi: String,
}

/// OS-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OS {
    pub name: String,
    pub version: String,
}

/// Screen-related fields
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Screen {
    pub density: u16,
    pub width: u16,
    pub height: u16,
    #[serde(default)]
    pub inner_width: Option<u16>,
    #[serde(default)]
    pub inner_height: Option<u16>,
}
