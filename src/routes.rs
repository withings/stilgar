use crate::beanstalk::BeanstalkProxy;
use crate::events::any::{AnyEvent, EventOrBatch, set_common_attribute};
use crate::forwarder::delay_from_schedule;
use crate::middleware;

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
    set_common_attribute!(event, received_at, Some(now));
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
pub async fn event_or_batch(beanstalk: BeanstalkProxy,
                            schedule: cron::Schedule,
                            request_info: middleware::BasicRequestInfo,
                            payload: String) -> Result<impl warp::Reply, warp::Rejection> {
    let event_or_batch = serde_json::from_str::<EventOrBatch>(&payload);
    let mut event_or_batch = match event_or_batch {
        Ok(eb) => eb,
        Err(_) => {
            log::warn!(
                "[rejected] [{}] malformed event from {} ({}) - {}",
                request_info.request_id.unwrap_or("?".into()),
                request_info.client_ip,
                request_info.user_agent.unwrap_or("unknown user agent".into()),
                payload
            );
            return Err(warp::reject::custom(middleware::InvalidJSONPayload));
        }
    };

    /* Re-serialise the job for beanstalkd */
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


/// Ping route
pub async fn ping() -> Result<impl warp::Reply, warp::Rejection> {
    Ok(warp::reply::with_status(":-)", http::status::StatusCode::OK))
}


/// Status route
pub async fn status(schedule: cron::Schedule, beanstalk: BeanstalkProxy) -> Result<impl warp::Reply, warp::Rejection> {
    let mut all_stats: HashMap<String, serde_json::Value> = HashMap::new();
    let mut all_good = true;

    /* stilgar info */
    all_stats.insert("next_forward_in".into(), delay_from_schedule(&schedule).into());

    /* beanstalkd stats */
    match beanstalk.stats().await {
        Ok(stats) => {
            all_stats.insert("beanstalkd_status".into(), "OK".into());
            all_stats.insert("beanstalkd_jobs_ready".into(), stats.jobs_ready.into());
            all_stats.insert("beanstalkd_jobs_reserved".into(), stats.jobs_reserved.into());
            all_stats.insert("beanstalkd_jobs_delayed".into(), stats.jobs_delayed.into());
            all_stats.insert("beanstalkd_total_jobs".into(), stats.total_jobs.into());
            all_stats.insert("beanstalkd_current_connections".into(), stats.current_connections.into());
        },
        Err(e) => {
            all_stats.insert("beanstalkd_status".to_string(), format!("KO: {}", e).into());
            all_good = false;
        }
    }

    all_stats.insert("status".into(), (if all_good { "OK" } else { "KO" }).into());
    let json_reply = serde_json::to_value(&all_stats).expect("failed to serialise status hashmap");
    Ok(warp::reply::with_status(
        warp::reply::json(&json_reply),
        if all_good { http::status::StatusCode::OK } else { http::status::StatusCode::INTERNAL_SERVER_ERROR }
    ))
}
