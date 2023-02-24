mod config;
mod destinations;
mod events;
mod routes;
mod beanstalk;
mod forwarder;


use crate::beanstalk::{Beanstalk, BeanstalkProxy};
use crate::forwarder::events_forwarder;
use crate::destinations::init_destinations;

use env_logger;
use tokio;
use cron;
use log;
use warp;
use warp::Filter;
use warp::http::Method;
use std::convert::Infallible;
use std::net::SocketAddr;

/// Stilgar's entry point: welcome!
#[tokio::main]
async fn main() {
    env_logger::init();

    /* Set a panic hook: we want a task panic to crash the whole process */
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    /* Locate and parse the configuration file, see config.rs */
    let args: Vec<String> = std::env::args().collect();
    let configuration = match config::get_configuration(args.get(1)) {
        Ok(c) => c,
        Err(e) => {
            log::error!("failed to process configuration file: {}", e);
            std::process::exit(1);
        }
    };

    /* First connection to beanstalkd, to PUT jobs */
    let mut bstk_web = match Beanstalk::connect(&configuration.forwarder.beanstalk).await {
        Ok(b) => b,
        Err(e) => {
            log::error!("failed to connect to Beanstalk: {}", e);
            std::process::exit(1);
        }
    };

    /* Second connection to beanstalkd, to RESERVE jobs and wait */
    let mut bstk_forwarder = match Beanstalk::connect(&configuration.forwarder.beanstalk).await {
        Ok(b) => b,
        Err(e) => {
            log::error!("failed to connect to Beanstalk: {}", e);
            std::process::exit(1);
        }
    };

    /* Instantiate all Destination structs as per the configuration */
    let destinations = match init_destinations(&configuration.destinations).await {
        Ok(d) => d,
        Err(e) => {
            log::error!("destination error: {}", e);
            std::process::exit(1);
        }
    };

    /* Routes used to store events */
    let any_event_route = warp::post().and(
        warp::path!("v1" / "batch")
            .or(warp::path!("v1" / "alias")).unify()
            .or(warp::path!("v1" / "group")).unify()
            .or(warp::path!("v1" / "identify")).unify()
            .or(warp::path!("v1" / "page")).unify()
            .or(warp::path!("v1" / "screen")).unify()
            .or(warp::path!("v1" / "track")).unify())
        .and(with_beanstalk(bstk_web.proxy()))
        .and(with_schedule(configuration.forwarder.schedule))
        .and(warp::body::json())
        .and_then(routes::any_event);

    /* Source config route to mock the Rudderstack control plane */
    let source_config_route = warp::get()
        .and(warp::path!("sourceConfig"))
        .map(|| String::from("{\"source\": {\"enabled\": true}}"));

    /* Prepare the API and forwarder tasks */
    let use_proxy = bstk_web.proxy();
    let watch_proxy = bstk_forwarder.proxy();
    let forwarder = events_forwarder(bstk_forwarder.proxy(), &destinations);
    let webservice = warp::serve(
        any_event_route.or(source_config_route)
            .with(cors(&configuration.server.origins))
            .recover(handle_rejection)
    ).run(SocketAddr::new(configuration.server.ip, configuration.server.port));

    /* Start everything */
    tokio::join!(
        bstk_web.run_channel(), /* run the mpsc channel for beanstalkd (PUT) */
        bstk_forwarder.run_channel(), /* same for the RESERVE channel */
        async {
            /* once the PUT channel is ready (has processed the USE command), start taking requests */
            match use_proxy.use_tube("stilgar").await {
                Ok(_) => webservice.await,
                Err(e) => {
                    log::error!("failed to use beanstalkd tube on webservice connection: {}", e);
                    std::process::exit(1);
                }
            }
        },
        async {
            /* same for WATCH and the forwarder/worker */
            match watch_proxy.watch_tube("stilgar").await {
                Ok(_) => forwarder.await,
                Err(e) => {
                    log::error!("failed to watch beanstalkd tube on forwarder connection: {}", e);
                    std::process::exit(1);
                }
            }
        }
    );
}

fn with_schedule(schedule: cron::Schedule) -> impl Filter<Extract = (cron::Schedule,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || schedule.clone())
}

fn with_beanstalk(proxy: BeanstalkProxy) -> impl Filter<Extract = (BeanstalkProxy,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || proxy.clone())
}

#[derive(Debug)]
struct Unauthorized;
#[derive(Debug)]
struct Forbidden;
impl warp::reject::Reject for Unauthorized {}
impl warp::reject::Reject for Forbidden {}

fn cors(origins: &Vec<String>) -> warp::cors::Builder {
    warp::cors()
        .allow_methods(&[Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(["authorization", "anonymousid", "content-type"])
        .allow_credentials(true)
        .allow_origins(origins.iter().map(|s| s.as_str()))
}

async fn handle_rejection(_err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    Ok(warp::reply::with_status("KO", warp::http::StatusCode::NOT_FOUND))
}
