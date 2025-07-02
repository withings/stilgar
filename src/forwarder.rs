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

use crate::events::any::{AnyEvent, EventOrBatch, set_common_attribute};

use crate::destinations::{Destinations, StorageResult, DestinationStatistics};
use crate::webstats::{WebStatsEvent, send_stats_event};

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tokio;
use tokio::sync::{oneshot, mpsc};
use tokio::time::Instant;
use log;

/// First backoff duration in seconds (increases exponentially)
const DEFAULT_BACKOFF: u64 = 2;
/// Maximum backoff duration in seconds
const MAX_BACKOFF: u64 = 300;
/// Duration (seconds) without a backoff request after which the backoff is reset to its default
const BACKOFF_RESET_AFTER: u64 = 30;

/// An event associated to a write key, for the forwarder
#[derive(Serialize, Deserialize, Debug)]
pub struct ForwarderEnvelope {
    pub write_key: String,
    pub event_or_batch: EventOrBatch,
}

/// An event storage message for the forwarder channel
#[derive(Debug)]
pub struct StatusRequestMessage {
    pub return_tx: oneshot::Sender<HashMap<String, DestinationStatistics>>,
}

/// A flush message for the forwarder channel
#[derive(Debug)]
pub struct FlushMessage {
    pub return_tx: oneshot::Sender<()>,
}

/// Admin messages sent to the forwarder channel
#[derive(Debug)]
pub enum ForwardingChannelAdminMessage {
    Stats(StatusRequestMessage),
    Flush(FlushMessage),
}

/// The forwarder channel, receiving events and passing them over to destinations
pub struct ForwardingChannel {
    destinations: Destinations,
    stats: mpsc::Sender<WebStatsEvent>,
    events_channel_tx: mpsc::Sender<ForwarderEnvelope>,
    events_channel_rx: mpsc::Receiver<ForwarderEnvelope>,
    admin_channel_tx: mpsc::Sender<ForwardingChannelAdminMessage>,
    admin_channel_rx: mpsc::Receiver<ForwardingChannelAdminMessage>,
}

impl ForwardingChannel {
    /// Takes ownership of the destinations and returns the channel struct
    pub fn new(max_queue_size: usize, destinations: Destinations, stats: mpsc::Sender<WebStatsEvent>) -> Self {
        let (events_channel_tx, events_channel_rx) = mpsc::channel::<ForwarderEnvelope>(max_queue_size);
        let (admin_channel_tx, admin_channel_rx) = mpsc::channel::<ForwardingChannelAdminMessage>(32);
        Self {
            destinations,
            stats,
            events_channel_tx,
            events_channel_rx,
            admin_channel_tx,
            admin_channel_rx,
        }
    }

    /// Clones the channel's sender for sharing with other components
    pub fn events_handle(&self) -> mpsc::Sender<ForwarderEnvelope> {
        self.events_channel_tx.clone()
    }

    /// Clones the channel's sender for sharing with other components
    pub fn admin_handle(&self) -> mpsc::Sender<ForwardingChannelAdminMessage> {
        self.admin_channel_tx.clone()
    }

    /// Runs the forwarding events channel, processes events
    pub async fn run_channel(&mut self) {
        let mut active_backoff: Option<Instant> = None;
        let mut last_backoff = Instant::now();
        let mut backoff_secs: u64 = DEFAULT_BACKOFF;

        loop {
            if active_backoff.map(|i| i > Instant::now()).unwrap_or(false) {
                active_backoff = None;
            }

            let (event_opt, admin_msg_opt) = match active_backoff {
                Some(d) => match tokio::time::timeout_at(d, self.admin_channel_rx.recv()).await {
                    Ok(a) => (None, a),
                    Err(_) => continue,
                },

                None => tokio::select! {
                    biased; /* events come much faster, always check for admin messages first */
                    admin_msg = self.admin_channel_rx.recv() => (None, admin_msg),
                    event = self.events_channel_rx.recv() => (event, None),
                },
            };

            match (event_opt, admin_msg_opt) {
                (Some(envelope), None) => {
                    if let Ok(_) = self.process_event(envelope).await {
                        if backoff_secs > DEFAULT_BACKOFF && last_backoff.elapsed().as_secs() > BACKOFF_RESET_AFTER {
                            log::info!("operations back to normal, resetting forwarder backoff");
                            backoff_secs = DEFAULT_BACKOFF;
                        }
                    } else {
                        log::warn!("at least 1 destination reported an error recently, backing off for {} seconds", backoff_secs);
                        last_backoff = Instant::now() + tokio::time::Duration::from_secs(backoff_secs);
                        active_backoff = Some(last_backoff);
                        backoff_secs = std::cmp::min(MAX_BACKOFF, backoff_secs * 2);
                    }
                },

                (None, Some(admin_msg)) => {
                    match admin_msg {
                        ForwardingChannelAdminMessage::Stats(stats_request) => {
                            stats_request.return_tx.send(self.gather_stats().await)
                                .expect("failed to respond to stats request message on oneshot channel")
                        },

                        ForwardingChannelAdminMessage::Flush(flush_request) => {
                            for destination in self.destinations.iter() {
                                destination.flush().await;
                            }
                            flush_request.return_tx.send(()).expect("failed to acknowledge flush request on oneshot channel");
                        },
                    }
                },

                _ => break
            }
        }
    }

    async fn process_event(&mut self, envelope: ForwarderEnvelope) -> StorageResult {
        /* If it's a batch event, split it and dispatch each subevent individually */
        match envelope.event_or_batch {
            EventOrBatch::Batch(mut batch_event) => {
                for subevent in batch_event.batch.iter_mut() {
                    set_common_attribute!(subevent, sent_at, batch_event.sent_at);
                    let new_envelope = ForwarderEnvelope {
                        write_key: envelope.write_key.clone(),
                        event_or_batch: EventOrBatch::Event(subevent.clone()),
                    };
                    self.dispatch_event(new_envelope).await?;
                }
                Ok(())
            },

            EventOrBatch::Event(_) => self.dispatch_event(envelope).await
        }
    }

    /// Forwards an event to matching destinations
    async fn dispatch_event(&mut self, envelope: ForwarderEnvelope) -> StorageResult{
        let mut store_result: StorageResult = Ok(());
        for destination in self.destinations.iter() {
            if !destination.matches_write_key(&envelope.write_key) {
                continue;
            }

            let mid = envelope.event_or_batch.message_id();
            let (storage_result, stats_event) = match &envelope.event_or_batch {
                EventOrBatch::Event(event) => match event {
                    AnyEvent::Alias(alias) => (destination.alias(alias).await, WebStatsEvent::AliasStored),
                    AnyEvent::Group(group) => (destination.group(group).await, WebStatsEvent::GroupStored),
                    AnyEvent::Identify(identify) => (destination.identify(identify).await, WebStatsEvent::IdentifyStored),
                    AnyEvent::Page(page) => (destination.store_page(page).await, WebStatsEvent::PageStored),
                    AnyEvent::Screen(screen) => (destination.store_screen(screen).await, WebStatsEvent::ScreenStored),
                    AnyEvent::Track(track) => (destination.store_track(track).await, WebStatsEvent::TrackStored),
                },
                _ => panic!("a batch event has made it through unsplit, this should not happen"),
            };

            if let Err(e) = storage_result {
                if destination.error_is_critical(&e) {
                    /* Delay further job processing on destination errors */
                    log::error!(mid; "critical destination error: {}: {}", destination, e);
                    store_result = Err(e);
                } else {
                    log::warn!(mid; "non-critical destination error: {}: {}", destination, e);
                }
            } else {
                log::debug!(mid; "forwarded to destination: {}", destination);
                send_stats_event(&self.stats, stats_event).await;
            }
        }
        store_result
    }

    /// Gathers statistics from all known destinations
    async fn gather_stats(&self) -> HashMap<String, DestinationStatistics> {
        let mut all_stats = HashMap::new();
        for destination in self.destinations.iter() {
            let destination_name = format!("{}", destination);
            let destination_stats = destination.stats().await;

            if let Err(err) = destination_stats.as_ref() {
                log::warn!("failed to get statistics for destination {}: {}", destination_name, err);
            }

            all_stats.insert(destination_name, destination_stats.unwrap_or(DestinationStatistics::new()));
        }
        all_stats
    }
}
