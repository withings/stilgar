use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;

use serde::{Deserialize, Serialize};

/// Convenience enum: can accept any event
#[derive(Serialize, Deserialize)]
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
pub struct Batch {
    pub batch: Vec<AnyEvent>,
}

/// Convenience enum: accepts any event or a batch of events
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventOrBatch {
    Event(AnyEvent),
    Batch(Batch),
}
