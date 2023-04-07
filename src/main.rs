mod config;
mod destinations;
mod events;
mod routes;
mod beanstalk;
mod forwarder;
mod middleware;

use crate::beanstalk::{Beanstalk, BeanstalkProxy};
use crate::forwarder::Forwarder;
use crate::destinations::init_destinations;

use tokio;
use cron;
use log;
use warp;
use simple_logger::SimpleLogger;
use warp::Filter;
use std::net::SocketAddr;
use std::collections::HashMap;

/// Stilgar's entry point: welcome!
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    /* Set a panic hook: we want a task (or thread) panic to crash the whole process */
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
            eprintln!("failed to process configuration file: {}", e);
            std::process::exit(1);
        }
    };

    /* Start logging properly */
    SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("stilgar", configuration.logging.level)
        .init().expect("failed to initialise the logger");

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

    /* Instiantiate the forwarder */
    let mut forwarder = Forwarder::new(bstk_forwarder.proxy());

    /* Instantiate all Destination structs as per the configuration */
    let destinations = match init_destinations(&configuration.destinations, forwarder.suspend_channel()).await {
        Ok(d) => d,
        Err(e) => {
            log::error!("destination error: {}", e);
            std::process::exit(1);
        }
    };

    /* Routes used to catch events */
    let any_event_route = warp::post().and(
        warp::path!("v1" / "batch")
            .or(warp::path!("v1" / "alias")).unify()
            .or(warp::path!("v1" / "group")).unify()
            .or(warp::path!("v1" / "identify")).unify()
            .or(warp::path!("v1" / "page")).unify()
            .or(warp::path!("v1" / "screen")).unify()
            .or(warp::path!("v1" / "track")).unify())
        .and(middleware::content_length_filter(configuration.server.payload_size_limit))
        .and(middleware::write_key_auth_filter(configuration.server.write_key.clone()))
        .and(with_beanstalk(bstk_web.proxy()))
        .and(with_schedule(configuration.forwarder.schedule.clone()))
        .and(middleware::basic_request_info())
        .and(middleware::compressible_body())
        .and_then(routes::event_or_batch);

    /* Source config route to mock the Rudderstack control plane */
    let source_config_route = warp::get()
        .and(warp::path!("sourceConfig"))
        .map(move || configuration.server.write_key.clone())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(routes::source_config);

    /* Status route */
    let status_route = warp::get()
        .and(warp::path("status"))
        .and(middleware::admin_auth_filter(
            configuration.server.admin_username.clone(),
            configuration.server.admin_password.clone()
        ))
        .and(with_schedule(configuration.forwarder.schedule.clone()))
        .and(with_beanstalk(bstk_web.proxy()))
        .and_then(routes::status);

    /* Ping (root) route for monitoring */
    let ping_route = warp::get()
        .and(warp::path::end())
        .and_then(routes::ping);

    /* Prepare the API and forwarder tasks */
    let use_proxy = bstk_web.proxy();
    let watch_proxy = bstk_forwarder.proxy();
    let webservice = warp::serve(
        any_event_route.or(source_config_route).or(status_route).or(ping_route)
            .with(middleware::cors(&configuration.server.origins))
            .with(warp::log::custom(middleware::request_logger))
            .recover(middleware::handle_rejection)
    ).run(SocketAddr::new(configuration.server.ip, configuration.server.port));

    /* Start everything */
    tokio::join!(
        bstk_web.run_channel(), /* run the mpsc channel for beanstalkd (PUT) */
        bstk_forwarder.run_channel(), /* same for the RESERVE channel */
        async {
            /* once the PUT channel is ready (has processed the USE command), start taking requests */
            match use_proxy.use_tube("stilgar").await {
                Ok(_) => {
                    log::info!("webservice ready for events!");
                    webservice.await
                },
                Err(e) => {
                    log::error!("failed to use beanstalkd tube on webservice connection: {}", e);
                    std::process::exit(1);
                }
            }
        },
        async {
            /* same for WATCH and the forwarder/worker */
            match watch_proxy.watch_tube("stilgar").await {
                Ok(_) => {
                    log::info!("forwarder ready for events!");
                    forwarder.run_for(&destinations).await
                },
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
