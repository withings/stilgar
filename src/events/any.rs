use crate::events::alias::Alias;
use crate::events::batch::Batch;
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
    AliasEvent(Alias),
    #[serde(rename = "batch")]
    BatchEvent(Batch),
    #[serde(rename = "group")]
    GroupEvent(Group),
    #[serde(rename = "identify")]
    IdentifyEvent(Identify),
    #[serde(rename = "page")]
    PageEvent(Page),
    #[serde(rename = "screen")]
    ScreenEvent(Screen),
    #[serde(rename = "track")]
    TrackEvent(Track),
}
