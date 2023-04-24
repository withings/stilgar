use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, mpsc};
use chrono::prelude::*;

/// An event as sent over the stats channel
#[derive(Debug)]
pub enum WebStatsEvent {
    EventReceived,

    AliasStored,
    GroupStored,
    IdentifyStored,
    PageStored,
    ScreenStored,
    TrackStored,

    Fetch(oneshot::Sender<WebStats>),
}

/// Basic statistics for web services
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebStats {
    pub events_received: u64,
    pub up_since: DateTime<Utc>,
    pub aliases: u64,
    pub groups: u64,
    pub identifies: u64,
    pub pages: u64,
    pub screens: u64,
    pub tracks: u64,
}

/// Web statistics collector channel
pub struct WebStatsChannel {
    channel_tx: mpsc::Sender<WebStatsEvent>,
    channel_rx: mpsc::Receiver<WebStatsEvent>,
    stats: WebStats,
}

impl WebStatsChannel {
    /// Initialises a new collector
    pub fn new() -> Self {
        let (channel_tx, channel_rx) = mpsc::channel::<WebStatsEvent>(32); // TODO 32?
        Self {
            channel_tx, channel_rx,
            stats: WebStats {
                events_received: 0,
                up_since: Utc::now(),
                aliases: 0,
                groups: 0,
                identifies: 0,
                pages: 0,
                screens: 0,
                tracks: 0,
            }
        }
    }

    /// Returns a clone of the collector's channel
    pub fn handle(&self) -> mpsc::Sender<WebStatsEvent> {
        self.channel_tx.clone()
    }

    /// Runs the collector's channel
    pub async fn run_channel(&mut self) {
        while let Some(event) = self.channel_rx.recv().await {
            match event {
                WebStatsEvent::EventReceived => self.stats.events_received += 1,
                WebStatsEvent::AliasStored => self.stats.aliases += 1,
                WebStatsEvent::GroupStored => self.stats.groups += 1,
                WebStatsEvent::IdentifyStored => self.stats.identifies += 1,
                WebStatsEvent::PageStored => self.stats.pages += 1,
                WebStatsEvent::ScreenStored => self.stats.screens += 1,
                WebStatsEvent::TrackStored => self.stats.tracks += 1,

                WebStatsEvent::Fetch(return_tx) => {
                    return_tx.send(self.stats.clone())
                        .expect("failed to send current web stats over the return channel")
                }
            }
        }
    }
}

/// Convenience function: sends an event to the statistics collector
pub async fn send_stats_event(stats: &mpsc::Sender<WebStatsEvent>, event: WebStatsEvent) {
    stats.send(event).await.expect("failed to send web stats event over the channel");
}

/// Convenience function: fetches current statistics from the collector
pub async fn fetch_stats(stats: mpsc::Sender<WebStatsEvent>) -> WebStats {
    let (tx, rx) = oneshot::channel::<WebStats>();
    stats.send(WebStatsEvent::Fetch(tx)).await.expect("failed to send stats fetch request over the channel");
    rx.await.expect("failed to receive stats over the channel")
}
