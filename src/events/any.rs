use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

macro_rules! set_common_attribute {
    ($event:expr, $field:ident, $value:expr) => {
        match $event {
            AnyEvent::Alias(alias) => alias.common.$field = $value,
            AnyEvent::Group(group) => group.common.$field = $value,
            AnyEvent::Identify(group) => group.common.$field = $value,
            AnyEvent::Page(group) => group.common.$field = $value,
            AnyEvent::Screen(group) => group.common.$field = $value,
            AnyEvent::Track(group) => group.common.$field = $value,
        };
    };
}

pub(crate) use set_common_attribute;

/// Convenience enum: can accept any event
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum AnyEvent {
    #[serde(rename = "alias")]
    Alias(Alias),
    #[serde(rename = "group")]
    Group(Group),
    #[serde(rename = "identify")]
    Identify(Identify),
    #[serde(rename = "page")]
    Page(Page),
    #[serde(rename = "screen")]
    Screen(Screen),
    #[serde(rename = "track")]
    Track(Track),
}

/// A batch event, as sent to /v1/batch
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub batch: Vec<AnyEvent>,
    #[serde(default)]
    pub sent_at: Option<DateTime<Utc>>,
}

/// Convenience enum: accepts any event or a batch of events
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventOrBatch {
    Event(AnyEvent),
    Batch(Batch),
}
