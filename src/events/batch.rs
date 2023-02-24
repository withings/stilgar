use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;

use serde::{Deserialize, Serialize};

/// A batch event, as sent to /v1/batch
#[derive(Serialize, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum BatchEvent {
    IdentifyEvent(Identify),
    TrackEvent(Track),
    PageEvent(Page),
    GroupEvent(Group),
    ScreenEvent(Screen),
}

/// Convenience type: the actual batch
pub type Batch = Vec<BatchEvent>;
