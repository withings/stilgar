use crate::events::any::{AnyEvent, EventOrBatch, set_common_attribute};

use crate::beanstalk::BeanstalkProxy;
use crate::destinations::Destinations;

use std::time::Duration;
use serde_json;
use chrono;
use tokio;
use cron;
use log;

/// Computes the time, in seconds, to the next processing slot
pub fn delay_from_schedule(schedule: &cron::Schedule) -> u64 {
    let upcoming = schedule.upcoming(chrono::Local).next()
        .expect("failed to get upcoming time from forward CRON expression");
    let duration = upcoming - chrono::Local::now();
    let seconds = duration.num_seconds();
    if seconds > 0 { seconds as u64 } else { 0 }
}

/// Processes the job when the time comes
pub async fn events_forwarder(beanstalk: BeanstalkProxy, destinations: &Destinations) {
    log::debug!("starting events forwarder");

    loop {
        /* Wait for beanstalkd to yield a job for which the delay is now zero */
        let job = match beanstalk.reserve().await {
            Ok(j) => j,
            Err(e) => {
                log::warn!("failed to reserve job, will try again soon: {}", e);
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        /* Immediately delete the job, whatever happens next */
        log::debug!("new processor job: {}", job.payload);
        if let Err(e) = beanstalk.delete(job.id).await {
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
                    Ok(subevent_str) => if let Err(e) = beanstalk.put(subevent_str, 0).await {
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

            log::debug!("forwarded to destination: {}", destination);
            if let Err(e) = storage_result {
                log::error!("destination failure on {}: {}", destination, e);
            }
        }
    }
}
