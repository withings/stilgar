use crate::beanstalk::BeanstalkProxy;
use crate::events::any::AnyEvent;
use crate::forwarder::delay_from_schedule;

use chrono::Utc;
use serde_json;
use warp;
use cron;

/// Generic event route handler: parse the event and push it to beanstalkd
async fn receive_and_queue(beanstalk: BeanstalkProxy, schedule: cron::Schedule, job: serde_json::Result<String>) -> Result<impl warp::Reply, warp::Rejection> {
    /* Check that we were able to re-serialise the job for beanstalkd */
    let job_payload = match job {
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

/// Actual route: adds the received_at field and tries to reserialise for beanstalkd
pub async fn any_event(beanstalk: BeanstalkProxy, schedule: cron::Schedule, mut event_json: serde_json::Value) -> Result<impl warp::Reply, warp::Rejection> {
    event_json["receivedAt"] = serde_json::Value::String(Utc::now().to_rfc3339());
    let any_event: AnyEvent = serde_json::from_value(event_json).map_err(|_| warp::reject::not_found())?;
    receive_and_queue(beanstalk, schedule, serde_json::to_string(&any_event)).await
}
