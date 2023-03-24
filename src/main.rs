mod config;
mod destinations;
mod events;
mod routes;
mod beanstalk;
mod forwarder;
mod middleware;

use crate::beanstalk::{Beanstalk, BeanstalkProxy};
use crate::forwarder::events_forwarder;
use crate::destinations::init_destinations;

use env_logger;
use tokio;
use cron;
use log;
use warp;
use warp::Filter;
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;

/// Stilgar's entry point: welcome!
#[tokio::main(flavor = "multi_thread")]
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
        .and(middleware::content_length_filter(configuration.server.payload_size_limit))
        .and(middleware::auth_filter(configuration.server.write_key.clone()))
        .and(with_beanstalk(bstk_web.proxy()))
        .and(with_schedule(configuration.forwarder.schedule))
        .and(middleware::compressible_event())
        .and_then(routes::event_or_batch);

    /* Source config route to mock the Rudderstack control plane */
    let source_config_route = warp::get()
        .and(warp::path!("sourceConfig"))
        .and(with_write_key(configuration.server.write_key.clone()))
        .and(warp::query::<HashMap<String, String>>())
        .and_then(routes::source_config);

    /* Prepare the API and forwarder tasks */
    let use_proxy = bstk_web.proxy();
    let watch_proxy = bstk_forwarder.proxy();
    let forwarder = events_forwarder(bstk_forwarder.proxy(), &destinations);
    let webservice = warp::serve(
        any_event_route.or(source_config_route)
            .with(middleware::cors(&configuration.server.origins))
            .recover(middleware::handle_rejection)
    ).run(SocketAddr::new(configuration.server.ip, configuration.server.port));

    /* Start everything */
    tokio::join!(
        bstk_web.run_channel(), /* run the mpsc channel for beanstalkd (PUT) */
        bstk_forwarder.run_channel(), /* same for the RESERVE channel */
        async {
            /* once the PUT channel is ready (has processed the USE command), start taking requests */
            match use_proxy.use_tube("stilgar").await {
                Ok(_) => webservice.await, // TODO rate limit this
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

fn with_write_key(write_key: Arc<Option<String>>) -> impl Filter<Extract = (Arc<Option<String>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || write_key.clone())
}
