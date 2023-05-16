use crate::beanstalk::BeanstalkProxy;
use crate::destinations::DestinationStatistics;
use crate::forwarder::{ForwarderEnvelope, ForwardingChannelMessage, StatusRequestMessage};
use crate::events::any::{AnyEvent, EventOrBatch, set_common_attribute};
use crate::events::rejections::explain_rejection;
use crate::webstats::{WebStatsEvent, send_stats_event, fetch_stats};
use crate::middleware;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{oneshot, mpsc};
use chrono::Utc;
use serde_json;
use warp;
use http;

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
                            stats: mpsc::Sender<WebStatsEvent>,
                            request_info: middleware::BasicRequestInfo,
                            write_key: String,
                            payload: String) -> Result<impl warp::Reply, warp::Rejection> {
    let event_or_batch = serde_json::from_str::<EventOrBatch>(&payload);
    let mut event_or_batch = match event_or_batch {
        Ok(eb) => eb,
        Err(_) => {
            let request_id = request_info.request_id.unwrap_or("?".into());
            let explanations = explain_rejection(&payload);
            log::warn!(
                "[rejected] [{}] {} {} malformed event from {} ({}) - {}",
                request_id,
                request_info.method,
                request_info.path,
                request_info.client_ip,
                request_info.user_agent.unwrap_or("unknown user agent".into()),
                match explanations.is_empty() {
                    true => "no explanation".into(),
                    false => explanations.join(" ")
                }
            );
            log::warn!("[rejected] [{}] rejected payload: {}", request_id, payload);
            return Err(warp::reject::custom(middleware::InvalidJSONPayload));
        }
    };

    /* Re-serialise the job for beanstalkd */
    overwrite_any_received_at(&mut event_or_batch);
    let envelope = ForwarderEnvelope { write_key, event_or_batch };
    let job_payload = match serde_json::to_string(&envelope) {
        Ok(j) => j,
        Err(e) => {
            log::debug!("failed to re-serialise event: {}", e);
            return Ok(warp::reply::with_status("KO", warp::http::StatusCode::INTERNAL_SERVER_ERROR));
        }
    };

    log::debug!("enqueuing job: {}", &job_payload);

    /* Enqueue with a delay */
    let reply = match beanstalk.put(job_payload).await {
        Ok(_) => Ok(warp::reply::with_status("OK", warp::http::StatusCode::OK)),
        Err(e) => {
            log::warn!("could not enqueue job: {}", e);
            Ok(warp::reply::with_status("KO", warp::http::StatusCode::INTERNAL_SERVER_ERROR))
        },
    };

    send_stats_event(&stats, WebStatsEvent::EventReceived).await;
    reply
}


/// Control plane mock route
pub async fn source_config(expected_write_keys: Arc<HashSet<String>>, query_params: HashMap<String, String>) -> Result<impl warp::Reply, warp::Rejection> {
    let enabled = query_params.get("writeKey").as_ref()
        .map(|submitted| expected_write_keys.iter().any(|k| k == *submitted))
        .unwrap_or(false);

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
pub async fn status(forwarder_channel: mpsc::Sender<ForwardingChannelMessage>,
                    beanstalk: BeanstalkProxy,
                    stats: mpsc::Sender<WebStatsEvent>) -> Result<impl warp::Reply, warp::Rejection> {
    let mut all_stats: HashMap<String, serde_json::Value> = HashMap::new();
    let mut all_good = true;

    /* web stats */
    let stats = fetch_stats(stats).await;
    all_stats.insert("events".into(), serde_json::json!({
        "up_since": stats.up_since.to_rfc3339(),
        "received": stats.events_received,
        "forwarded": {
            "aliases": stats.aliases,
            "groups": stats.groups,
            "identifies": stats.identifies,
            "pages": stats.pages,
            "screens": stats.screens,
            "tracks": stats.tracks,
        }
    }));

    /* destination stats */
    let (dest_stats_tx, dest_stats_rx) = oneshot::channel::<HashMap<String, DestinationStatistics>>();
    forwarder_channel.send(ForwardingChannelMessage::Stats(StatusRequestMessage { return_tx: dest_stats_tx })).await
        .expect("failed to send stats request to the forwarding channel");
    let destination_stats = dest_stats_rx.await.expect("failed to receive destination stats from the forwarding channel");
    for (destination_name, destination_stats) in destination_stats.into_iter() {
        all_stats.insert(destination_name, serde_json::to_value(destination_stats).expect("failed to parse destination statistics"));
    }

    /* beanstalkd stats */
    match beanstalk.stats().await {
        Ok(stats) => {
            all_stats.insert("beanstalkd".into(), serde_json::json!({
                "status": "OK",
                "current_connections": stats.current_connections,
                "jobs": {
                    "ready": stats.jobs_ready,
                    "reserved": stats.jobs_reserved,
                    "delayed": stats.jobs_delayed,
                    "total": stats.total_jobs,
                }
            }));
        },
        Err(e) => {
            all_stats.insert("beanstalkd".into(), serde_json::json!({"status": e.to_string()}));
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
