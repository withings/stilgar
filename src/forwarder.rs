use crate::events::any::{AnyEvent, EventOrBatch, set_common_attribute};

use crate::beanstalk::{BeanstalkProxy, BeanstalkError};
use crate::destinations::Destinations;

use std::time::{Instant, Duration};
use serde_json;
use tokio;
use log;

/// Sleep duration after a reserve failure
const RESERVE_FAILURE_WAIT_TIME: u64 = 10;
/// First backoff duration in seconds (increases exponentially)
const DEFAULT_BACKOFF: u64 = 2;
/// Duration (seconds) without a backoff request after which the backoff is reset to its default
const BACKOFF_RESET_AFTER: u64 = 30;

/// The events forwarder along with its beanstalk and suspend channel
pub struct Forwarder {
    beanstalk: BeanstalkProxy,
}

impl Forwarder {
    /// Creates a new forwarder
    pub fn new(beanstalk: BeanstalkProxy) -> Self {
        Self {
            beanstalk,
        }
    }

    /// Actually runs the channel for a given set of destinations
    pub async fn run_for(&mut self, destinations: &Destinations) {
        log::debug!("starting events forwarder");
        let mut exponential_backoff: u64 = DEFAULT_BACKOFF;
        let mut last_backoff = Instant::now();

        loop {
            let job = match self.beanstalk.reserve().await {
                Ok(j) => j,
                Err(BeanstalkError::ReservationTimeout) => continue,
                Err(e) => {
                    log::warn!("failed to reserve job, will try again soon: {}", e);
                    tokio::time::sleep(Duration::from_secs(RESERVE_FAILURE_WAIT_TIME)).await;
                    continue;
                }
            };

            /* Immediately delete the job, whatever happens next */
            log::debug!("new processor job: {}", job.payload);
            if let Err(e) = self.beanstalk.delete(job.id).await {
                log::error!("failed to delete job, will process anyway: {}", e);
            }

            /* Make sure it's a proper event */
            let event_or_batch: EventOrBatch = match serde_json::from_str(&job.payload) {
                Ok(ev) => ev,
                Err(err) => {
                    log::warn!("could not re-parse job: {}", err);
                    continue;
                }
            };

            /* If it's a batch event, split it and reschedule each subevent individually (now) */
            if let EventOrBatch::Batch(mut batch_event) = event_or_batch {
                for subevent in batch_event.batch.iter_mut() {
                    set_common_attribute!(subevent, sent_at, batch_event.sent_at);
                    match serde_json::to_string(&subevent) {
                        Ok(subevent_str) => if let Err(e) = self.beanstalk.put(subevent_str).await {
                            log::warn!("failed to submit subevent from batch: {}", e);
                        },
                        Err(e) => {
                            log::warn!("could not parse event in batch, skipping: {}", e);
                        }
                    }
                }
                continue;
            }

            /* Forward the event to all known destinations using Destination methods */
            let mut critical_destination_error = false;
            for destination in destinations.iter() {
                let storage_result = match &event_or_batch {
                    EventOrBatch::Event(event) => match event {
                        AnyEvent::Alias(alias) => destination.alias(alias).await,
                        AnyEvent::Group(group) => destination.group(group).await,
                        AnyEvent::Identify(identify) => destination.identify(identify).await,
                        AnyEvent::Page(page) => destination.store_page(page).await,
                        AnyEvent::Screen(screen) => destination.store_screen(screen).await,
                        AnyEvent::Track(track) => destination.store_track(track).await,
                    },
                    _ => panic!("a batch event has made it through unsplit, this should not happen"),
                };

                if let Err(e) = storage_result {
                    if destination.error_is_critical(&e) {
                        /* Delay further job processing on destination errors */
                        log::error!("critical destination error: {}: {}", destination, e);
                        critical_destination_error = true;
                    } else {
                        log::warn!("non-critical destination error: {}: {}", destination, e);
                    }
                } else {
                    log::debug!("forwarded to destination: {}", destination);
                }
            }

            /* Back off in case of error, stop when normal service resumes */
            if critical_destination_error {
                log::warn!("at least 1 destination reported an error recently, backing off for {} seconds", exponential_backoff);
                tokio::time::sleep(tokio::time::Duration::from_secs(exponential_backoff)).await;
                last_backoff = Instant::now();
                exponential_backoff *= 2;
            } else if exponential_backoff > DEFAULT_BACKOFF && last_backoff.elapsed().as_secs() > BACKOFF_RESET_AFTER {
                log::info!("operations back to normal, resetting forwarder backoff");
                exponential_backoff = DEFAULT_BACKOFF;
            }
        }
    }
}
