use crate::beanstalk::BeanstalkProxy;
use crate::events::any::{AnyEvent, EventOrBatch};
use crate::forwarder::delay_from_schedule;

use std::collections::HashMap;
use std::sync::Arc;
use chrono::Utc;
use serde_json;
use warp;
use http;
use cron;

/// Rewrites a single event's received_at
fn overwrite_event_received_at(event: &mut AnyEvent) {
    let now = Utc::now();
    match event {
        AnyEvent::Alias(alias) => alias.common.received_at = Some(now),
        AnyEvent::Group(group) => group.common.received_at = Some(now),
        AnyEvent::Identify(group) => group.common.received_at = Some(now),
        AnyEvent::Page(group) => group.common.received_at = Some(now),
        AnyEvent::Screen(group) => group.common.received_at = Some(now),
        AnyEvent::Track(group) => group.common.received_at = Some(now),
    }
}

/// Rewrites an event's received_at, going into batches if necessary
fn overwrite_any_received_at(event_or_batch: &mut EventOrBatch) {
    match event_or_batch {
        EventOrBatch::Batch(batch_event) => for event in batch_event.batch.iter_mut() {
            overwrite_event_received_at(event)
        },
        EventOrBatch::Event(event) => overwrite_event_received_at(event)
    }
}

/// Actual event route: adds the received_at field and tries to reserialise for beanstalkd
pub async fn event_or_batch(beanstalk: BeanstalkProxy, schedule: cron::Schedule, mut event_or_batch: EventOrBatch) -> Result<impl warp::Reply, warp::Rejection> {
    /* Check that we were able to re-serialise the job for beanstalkd */
    overwrite_any_received_at(&mut event_or_batch);
    let job_payload = match serde_json::to_string(&event_or_batch) {
        Ok(j) => j,
        Err(e) => {
            log::debug!("failed to re-serialise event: {}", e);
            return Ok(warp::reply::with_status("KO", warp::http::StatusCode::INTERNAL_SERVER_ERROR));
        }
    };

    log::debug!("enqueuing job: {}", &job_payload);

    /* Enqueue with a delay */
    match beanstalk.put(job_payload, delay_from_schedule(&schedule)).await {
        Ok(_) => Ok(warp::reply::with_status("OK", warp::http::StatusCode::OK)),
        Err(e) => {
            log::warn!("could not enqueue job: {}", e);
            Ok(warp::reply::with_status("KO", warp::http::StatusCode::INTERNAL_SERVER_ERROR))
        },
    }
}

/// Control plane mock route
pub async fn source_config(expected_write_key: Arc<Option<String>>, query_params: HashMap<String, String>) -> Result<impl warp::Reply, warp::Rejection> {
    let enabled = expected_write_key.as_ref().clone()
        .map(|expected| query_params.get("writeKey")
             .as_ref()
             .map(|submitted| **submitted == expected)
             .unwrap_or(false)) /* key expected but none submitted: no! */
        .unwrap_or(true); /* no key expected: ok! */

    let response = serde_json::json!({
        "source": {
            "enabled": enabled
        }
    });

    Ok(
        warp::reply::with_status(
            warp::reply::json(&response),
            if enabled { http::status::StatusCode::OK } else { http::status::StatusCode::FORBIDDEN }
        )
    )
}
