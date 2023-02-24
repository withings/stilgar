use lazy_static::lazy_static;
use indoc::{indoc, formatdoc};

lazy_static! {
    pub static ref EVENT_BASICS: String = indoc! {"
    `id` String,
    `user_id` Nullable(String),
    `anonymous_id` Nullable(String),
    `sent_at` Nullable(DateTime),
    `original_timestamp` Nullable(DateTime),
    `received_at` DateTime,
    `timestamp` Nullable(DateTime),
    `channel` Nullable(String),
    "}.to_string();


    pub static ref CONTEXT: String = indoc! {"
    `context_app_name` Nullable(String),
    `context_app_version` Nullable(String),
    `context_app_build` Nullable(String),

    `context_campaign_name` Nullable(String),
    `context_campaign_source` Nullable(String),
    `context_campaign_content` Nullable(String),
    `context_campaign_medium` Nullable(String),
    `context_campaign_term` Nullable(String),

    `context_device_type` Nullable(String),
    `context_device_id` Nullable(String),
    `context_device_advertising_id` Nullable(String),
    `context_device_ad_tracking_enabled` Nullable(String),
    `context_device_manufacturer` Nullable(String),
    `context_device_model` Nullable(String),
    `context_device_name` Nullable(String),

    `context_library_name` Nullable(String),
    `context_library_version` Nullable(String),

    `context_network_bluetooth` Nullable(String),
    `context_network_carrier` Nullable(String),
    `context_network_cellular` Nullable(String),
    `context_network_wifi` Nullable(String),

    `context_os_name` Nullable(String),
    `context_os_version` Nullable(String),

    `context_screen_density` Nullable(Int64),
    `context_screen_width` Nullable(Int64),
    `context_screen_height` Nullable(Int64),
    `context_screen_inner_width` Nullable(Int64),
    `context_screen_inner_height` Nullable(Int64),
    "}.to_string();


    pub static ref PAGES: String = formatdoc! {"
    CREATE TABLE pages
    (
        {event_basics}
        {context}

        `name` Nullable(String),
        `path` Nullable(String),
        `url` Nullable(String),
        `title` Nullable(String),
        `referrer` Nullable(String),
        `search` Nullable(String),
        `keywords` Nullable(String),
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};


    pub static ref SCREENS: String = formatdoc! {"
    CREATE TABLE screens
    (
        {event_basics}
        {context}

        `name` Nullable(String),
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};


    pub static ref IDENTIFIES: String = formatdoc! {"
    CREATE TABLE identifies
    (
        {event_basics}
        {context}
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};


    pub static ref TRACKS: String = formatdoc! {"
    CREATE TABLE tracks
    (
        {event_basics}
        {context}

        `event` LowCardinality(String),
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};


    pub static ref GROUPS: String = formatdoc! {"
    CREATE TABLE groups
    (
        {event_basics}
        {context}

        `group_id` LowCardinality(String),
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};
}
