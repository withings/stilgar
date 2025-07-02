/* stilgar - a lightweight, no-fuss, drop-in replacement for Rudderstack
 * Copyright (C) 2023 Withings
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>. */

mod config;
mod logging;
mod destinations;
mod events;
mod routes;
mod forwarder;
mod middleware;
mod webstats;

use crate::forwarder::{ForwardingChannel, ForwardingChannelAdminMessage, FlushMessage};
use crate::destinations::init_destinations;
use crate::webstats::{WebStatsChannel, WebStatsEvent};

use tokio;
use tokio::sync::{oneshot, mpsc};
use tokio::task::JoinSet;
use tokio::signal::unix::{signal, SignalKind};
use log;
use warp;
use warp::Filter;
use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::{HashMap, HashSet};

/// Duration (in seconds) between a destination flush and shutdown upon signal reception
const KILL_TIMEOUT: u64 = 5;

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
    logging::init_logger(&configuration.logging);

    /* Instantiate all Destination structs as per the configuration */
    let destinations = match init_destinations(&configuration.destinations).await {
        Ok(d) => d,
        Err(e) => {
            log::error!("destination error: {}", e);
            std::process::exit(1);
        }
    };

    /* Gather up the write keys for authentication filters */
    let all_write_keys: Arc<HashSet<String>> = Arc::new(
        configuration.destinations.iter()
            .map(|d| d.write_keys.iter()).flatten()
            .map(|s| s.clone()).collect()
    );

    /* Instiantiate the web stats channel */
    let mut web_stats = WebStatsChannel::new();
    let web_stats_handle = web_stats.handle();
    let forwarder_stats_handle = web_stats.handle();

    /* Instantiate the forwarding channel */
    let mut forwarder = ForwardingChannel::new(
        configuration.forwarder.max_queue_size,
        destinations,
        forwarder_stats_handle
    );
    let forwarder_event_handle = forwarder.events_handle();
    let forwarder_status_events_handle = forwarder.events_handle();
    let forwarder_status_admin_handle = forwarder.admin_handle();
    let forwarder_flush_handle = forwarder.admin_handle();

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
        .map(move || forwarder_event_handle.clone())
        .and(with_stats(web_stats_handle.clone()))
        .and(middleware::basic_request_info())
        .and(middleware::write_key(all_write_keys.clone()))
        .and(middleware::compressible_body())
        .and_then(routes::event_or_batch);

    /* Source config route to mock the Rudderstack control plane */
    let source_config_route = warp::get()
        .and(warp::path!("sourceConfig"))
        .map(move || all_write_keys.clone())
        .and(warp::query::<HashMap<String, String>>())
        .and_then(routes::source_config);

    /* Status route */
    let status_route = warp::get()
        .and(warp::path("status"))
        .and(middleware::admin_auth_filter(configuration.server.admin.clone()))
        .map(move || (forwarder_status_events_handle.clone(), forwarder_status_admin_handle.clone()))
        .untuple_one()
        .and(with_stats(web_stats_handle.clone()))
        .and_then(routes::status);

    /* Ping (root) route for monitoring */
    let ping_route = warp::get()
        .and(warp::path::end())
        .and_then(routes::ping);

    /* Prepare the API and forwarder tasks */
    let webservice = warp::serve(
        any_event_route.or(source_config_route).or(status_route).or(ping_route)
            .with(middleware::cors(&configuration.server.origins))
            .recover(middleware::handle_rejection)
    ).run(SocketAddr::new(configuration.server.ip, configuration.server.port));

    /* Start everything */
    tokio::join!(
        signal_handler(forwarder_flush_handle), /* signal handlers */
        forwarder.run_channel(), /* run the events channel */
        web_stats.run_channel(), /* run the web stats channel */
        async { webservice.await }, /* run the API */
    );
}

/// Handles UNIX signals and gives destinations a final chance to flush
async fn signal_handler(forwarder_channel: mpsc::Sender<ForwardingChannelAdminMessage>) {
    let mut signal_joinset: JoinSet<()> = JoinSet::new();
    for kind in [SignalKind::interrupt(), SignalKind::terminate()] {
        signal_joinset.spawn(async move {
            let mut stream = signal(kind).expect("failed to set signal handler");
            stream.recv().await;
        });
    }

    signal_joinset.join_next().await;
    log::info!("shutdown signal received, will request a flush to all destinations");
    signal_joinset.shutdown().await;

    /* Ask for forwarder to flush all destinations (and wait for their confirmation) */
    let (return_tx, return_rx) = oneshot::channel::<()>();
    forwarder_channel.send(ForwardingChannelAdminMessage::Flush(FlushMessage { return_tx })).await
        .expect("failed to send force flush request to the forwarding channel");

    /* Wait for either the confirmations or a timeout (in case destinations don't reply) */
    let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(KILL_TIMEOUT));
    tokio::select! {
        _ = timeout => log::warn!("destinations failed to confirm flush on time"),
        _ = return_rx => log::info!("clean shutdown, all destinations have flushed"),
    };

    log::info!("bye bye!");
    std::process::exit(0);
}

fn with_stats(stats: mpsc::Sender<WebStatsEvent>) -> impl Filter<Extract = (mpsc::Sender<WebStatsEvent>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || stats.clone())
}
