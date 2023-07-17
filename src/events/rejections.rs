/* stilgar - a lightweight, no-fuss, drop-in replacement for Rudderstack
 * Copyright (C) 2023 Withings
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>. */

use crate::events::any::{EventOrBatch, Batch};
use crate::events::alias::Alias;
use crate::events::group::Group;
use crate::events::identify::Identify;
use crate::events::page::Page;
use crate::events::screen::Screen;
use crate::events::track::Track;

use serde_json;

/// Attempts to parse a single event as various event types and explains why not
fn explain_event_rejection(event: serde_json::Value) -> Option<String> {
    println!("{}", event);
    match event.get("type").map(|t| t.as_str()).flatten() {
        Some(t) => {
            match t {
                "alias" => serde_json::from_value::<Alias>(event).map_err(|e| e.to_string()).err(),
                "group" => serde_json::from_value::<Group>(event).map_err(|e| e.to_string()).err(),
                "identify" => serde_json::from_value::<Identify>(event).map_err(|e| e.to_string()).err(),
                "page" => serde_json::from_value::<Page>(event).map_err(|e| e.to_string()).err(),
                "screen" => serde_json::from_value::<Screen>(event).map_err(|e| e.to_string()).err(),
                "track" => serde_json::from_value::<Track>(event).map_err(|e| e.to_string()).err(),
                t => Some(format!("unknown event type: {}", t))
            }
        },
        None => Some("missing type key in event".into()),
    }
}

/// Attempts to explain why a payload is being rejected, with log-friendly messages
pub fn explain_rejection(payload: &String) -> Vec<String> {
    let generic_json = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(j) => j,
        Err(_) => return vec!("invalid JSON structure".into())
    };

    let mut messages: Vec<String> = vec!();
    let global_message = match serde_json::from_str::<EventOrBatch>(&payload) {
        Ok(_) => return vec!(),
        Err(e) => e.to_string()
    };
    messages.push(global_message);

    match generic_json.as_object() {
        Some(generic_map) => {
            if let Err(e) = serde_json::from_value::<Batch>(generic_json.clone()) {
                messages.push(e.to_string());
            }

            if let Some(generic_batch) = generic_map.get("batch").map(|b| b.as_array()).flatten() {
                messages.extend(
                    generic_batch.iter().enumerate()
                        .map(|(i, v)| explain_event_rejection(v.clone()).map(|e| (i, e)))
                        .flatten()
                        .map(|(i, e)| format!("subevent {}: {}", i, e))
                );
            } else if let Some(_) = generic_map.get("type").map(|t| t.as_str()).flatten() {
                if let Some(e) = explain_event_rejection(generic_json.clone()) {
                    messages.push(e);
                }
            } else {
                messages.push("missing batch or type key".into())
            }
        },
        None => messages.push("root element is not a map".into())
    }

    messages.iter().enumerate()
        .map(|(i, m)| format!("({}) {}", i + 1, m))
        .collect::<Vec<String>>()
}
