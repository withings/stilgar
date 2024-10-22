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
use std::time::{Instant, Duration};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio;
use tokio::sync::{oneshot, mpsc};
use log;
use mamenoki::{BeanstalkClient, BeanstalkError};

/// Sleep duration after a reserve failure
const RESERVE_FAILURE_WAIT_TIME: u64 = 10;
/// First backoff duration in seconds (increases exponentially)
const DEFAULT_BACKOFF: u64 = 2;
/// Maximum backoff duration in seconds
const MAX_BACKOFF: u64 = 300;
/// Duration (seconds) without a backoff request after which the backoff is reset to its default
const BACKOFF_RESET_AFTER: u64 = 30;
const DEFAULT_RESERVE_TIMEOUT: u32 = 5;

/// An event associated to a write key, for the forwarder
#[derive(Serialize, Deserialize, Debug)]
pub struct ForwarderEnvelope {
    pub write_key: String,
    pub event_or_batch: EventOrBatch,
}

/// An event storage message for the forwarder channel
#[derive(Debug)]
pub struct EventForwardMessage {
    pub envelope: ForwarderEnvelope,
    pub return_tx: oneshot::Sender<StorageResult>,
}

/// An event storage message for the forwarder channel
#[derive(Debug)]
pub struct StatusRequestMessage {
    pub return_tx: oneshot::Sender<HashMap<String, DestinationStatistics>>,
}

/// An flush message for the forwarder channel
#[derive(Debug)]
pub struct FlushMessage {
    pub return_tx: oneshot::Sender<()>,
}

/// Any message sent to the forwarder channel
#[derive(Debug)]
pub enum ForwardingChannelMessage {
    Event(EventForwardMessage),
    Stats(StatusRequestMessage),
    Flush(FlushMessage),
}

/// The forwarder channel, receiving events and passing them over to destinations
pub struct ForwardingChannel {
    destinations: Destinations,
    stats: mpsc::Sender<WebStatsEvent>,
    channel_tx: mpsc::Sender<ForwardingChannelMessage>,
    channel_rx: mpsc::Receiver<ForwardingChannelMessage>,
}

impl ForwardingChannel {
    /// Takes ownership of the destinations and returns the channel struct
    pub fn new(destinations: Destinations, stats: mpsc::Sender<WebStatsEvent>) -> Self {
        let (channel_tx, channel_rx) = mpsc::channel::<ForwardingChannelMessage>(32); // TODO 32?
        Self {
            destinations,
            stats,
            channel_tx,
            channel_rx,
        }
    }

    /// Clones the channel's sender for sharing with other components
    pub fn handle(&self) -> mpsc::Sender<ForwardingChannelMessage> {
        self.channel_tx.clone()
    }

    /// Runs the forwarding channel, processes messages
    pub async fn run_channel(&mut self) {
        while let Some(message) = self.channel_rx.recv().await {
            match message {
                ForwardingChannelMessage::Event(forward_request) => {
                    forward_request.return_tx.send(self.dispatch_event(forward_request.envelope).await)
                        .expect("failed to respond to event forward message on oneshot channel")
                },

                ForwardingChannelMessage::Stats(stats_request) => {
                    stats_request.return_tx.send(self.gather_stats().await)
                        .expect("failed to respond to stats request message on oneshot channel")
                },

                ForwardingChannelMessage::Flush(flush_request) => {
                    for destination in self.destinations.iter() {
                        destination.flush().await;
                    }
                    flush_request.return_tx.send(()).expect("failed to acknowledge flush request on oneshot channel");
                },
            }
        }
    }

    /// Forwards an event to matching destinations
    async fn dispatch_event(&mut self, envelope: ForwarderEnvelope) -> StorageResult{
        let mut store_result: StorageResult = Ok(());
        for destination in self.destinations.iter() {
            if !destination.matches_write_key(&envelope.write_key) {
                continue;
            }

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
                    log::error!("critical destination error: {}: {}", destination, e);
                    store_result = Err(e);
                } else {
                    log::warn!("non-critical destination error: {}: {}", destination, e);
                }
            } else {
                log::debug!("forwarded to destination: {}", destination);
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


/// Pulls events from beanstalkd and feeds them to the forwarding channel
pub async fn feed_forwarding_channel(beanstalk: BeanstalkClient, forwarding_channel: mpsc::Sender<ForwardingChannelMessage>) {
    log::debug!("pushing events from beanstalkd into the forwarding channel");
    let mut exponential_backoff: u64 = DEFAULT_BACKOFF;
    let mut last_backoff = Instant::now();

    loop {
        let job = match beanstalk.reserve_with_timeout(DEFAULT_RESERVE_TIMEOUT).await {
            Ok(j) => j,
            Err(BeanstalkError::ReservationTimeout) => continue,
            Err(BeanstalkError::CommunicationError(e)) => {
                log::error!("beanstalkd communication error: {}", e);
                std::process::exit(1);
            },
            Err(e) => {
                log::warn!("failed to reserve job, will try again soon: {}", e);
                tokio::time::sleep(Duration::from_secs(RESERVE_FAILURE_WAIT_TIME)).await;
                continue;
            }
        };

        /* Immediately delete the job, whatever happens next */
        log::debug!("new job from beanstalkd: {}", job.payload);
        if let Err(e) = beanstalk.delete(job.id).await {
            log::error!("failed to delete job, will process anyway: {}", e);
        }

        /* Make sure it's a proper event with its write key */
        let envelope: ForwarderEnvelope = match serde_json::from_str(&job.payload) {
            Ok(ev) => ev,
            Err(err) => {
                log::warn!("could not re-parse job: {}", err);
                continue;
            }
        };

        /* If it's a batch event, split it and reschedule each subevent individually (now) */
        if let EventOrBatch::Batch(mut batch_event) = envelope.event_or_batch {
            for subevent in batch_event.batch.iter_mut() {
                set_common_attribute!(subevent, sent_at, batch_event.sent_at);
                let new_envelope = ForwarderEnvelope {
                    write_key: envelope.write_key.clone(),
                    event_or_batch: EventOrBatch::Event(subevent.clone()),
                };

                match serde_json::to_string(&new_envelope) {
                    Ok(envelope_str) => if let Err(e) = beanstalk.put(envelope_str).await {
                        log::warn!("failed to submit subevent from batch: {}", e);
                    },
                    Err(e) => {
                        log::warn!("could not parse event in batch, skipping: {}", e);
                    }
                }
            }
            continue;
        }

        /* Send the event to the forwarding channel */
        let (forward_tx, forward_rx) = oneshot::channel::<StorageResult>();
        forwarding_channel.send(ForwardingChannelMessage::Event(EventForwardMessage {
            envelope,
            return_tx: forward_tx
        })).await.expect("failed to push event into the forwarding channel");
        let store_result = forward_rx.await.expect("failed to receive event forward response from channel");

        /* Back off in case of error, stop when normal service resumes */
        if let Err(_err) = store_result {
            log::warn!("at least 1 destination reported an error recently, backing off for {} seconds", exponential_backoff);
            tokio::time::sleep(tokio::time::Duration::from_secs(exponential_backoff)).await;
            last_backoff = Instant::now();
            exponential_backoff = std::cmp::min(MAX_BACKOFF, exponential_backoff * 2);
        } else {
            if exponential_backoff > DEFAULT_BACKOFF && last_backoff.elapsed().as_secs() > BACKOFF_RESET_AFTER {
                log::info!("operations back to normal, resetting forwarder backoff");
                exponential_backoff = DEFAULT_BACKOFF;
            }
        }
    }
}
