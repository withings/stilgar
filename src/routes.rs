use crate::beanstalk::BeanstalkProxy;
use crate::events::any::AnyEvent;
use crate::forwarder::delay_from_schedule;

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

pub async fn any_event(beanstalk: BeanstalkProxy, schedule: cron::Schedule, any_event: AnyEvent) -> Result<impl warp::Reply, warp::Rejection> {
    receive_and_queue(beanstalk, schedule, serde_json::to_string(&any_event)).await
}
