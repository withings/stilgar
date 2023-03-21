use crate::destinations::StorageResult;
use crate::destinations::clickhouse::Clickhouse;

use std::collections::HashMap;
use itertools::Itertools;
use lazy_static::lazy_static;
use indoc::{indoc, formatdoc};
use chrono::DateTime;

lazy_static! {
    pub static ref EVENT_BASICS: String = indoc! {"
    `id` String,
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


    pub static ref ALIASES: String = formatdoc! {"
    CREATE TABLE aliases
    (
        {event_basics}
        {context}

        `user_id` String,
        `previous_id` String,
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};


    pub static ref PAGES: String = formatdoc! {"
    CREATE TABLE pages
    (
        {event_basics}
        {context}

        `user_id` Nullable(String),
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

        `user_id` Nullable(String),
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

        `user_id` String,
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};


    pub static ref USERS: String = formatdoc! {"
    CREATE TABLE users
    (
        `id` String,
        `received_at` DateTime,
    )
    ENGINE = AggregatingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY id;
    "};


    pub static ref TRACKS: String = formatdoc! {"
    CREATE TABLE tracks
    (
        {event_basics}
        {context}

        `user_id` Nullable(String),
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

        `user_id` Nullable(String),
        `group_id` LowCardinality(String),
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toDate(received_at)
    ORDER BY (received_at, id);
    ", event_basics = EVENT_BASICS.to_string(), context = CONTEXT.to_string()};
}

impl Clickhouse {
    /// Creates the basic tables used by Stilgar
    pub async fn create_tables(&self) -> StorageResult {
        let initial_ddl = [
            ("aliases", ALIASES.to_string()),
            ("pages", PAGES.to_string()),
            ("screens", SCREENS.to_string()),
            ("identifies", IDENTIFIES.to_string()),
            ("users", USERS.to_string()),
            ("tracks", TRACKS.to_string()),
            ("groups", GROUPS.to_string()),
        ];

        for (table_name, ddl_query) in initial_ddl.iter() {
            if !(self.table_exists(table_name).await?) {
                log::info!("table `{}` does not exist yet, creating it with basic schema", table_name);
                self.nio(ddl_query.clone()).await?;
            }
        }

        Ok(())
    }

    /// Adds any missing columns to an existing table
    pub async fn extend_existing_table(&self, table_name: &String, expected_columns: HashMap<String, String>) -> StorageResult {
        /* Figure out which columns are missing */
        let current_columns = self.describe_table(&table_name).await.expect("failed to describe table");
        let missing_columns = expected_columns.iter().filter(|(k, _v)| !current_columns.keys().contains(k)).collect_vec();
        let use_aggregate_function = self.table_is_aggregating(&table_name).await?;

        log::debug!("{} missing column(s) in table {}: will try to extend", missing_columns.len(), table_name);

        /* Create them */
        for (column_name, column_type) in missing_columns {
            let aggregating_column_type = format!("SimpleAggregateFunction(anyLast, {})", column_type);
            let final_column_type = match use_aggregate_function {
                true => &aggregating_column_type,
                false => column_type
            };
            let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", table_name, column_name, final_column_type);
            log::debug!("creating missing column: {}.{} {}", table_name, column_name, column_type);
            self.nio(sql).await?;
        }
        Ok(())
    }

    /// Designs a basic table with common fields and context info.
    pub fn get_basic_table_ddl(table_name: &String) -> String {
        format!(
            "CREATE TABLE {} ({} {}) ENGINE = ReplacingMergeTree PARTITION BY toDate(received_at) ORDER BY (received_at, id)",
            table_name,
            EVENT_BASICS.to_string(),
            CONTEXT.to_string()
        )
    }

    /// Infer a column's type using known values
    pub fn infer_vec_type<'a, I>(values: I) -> String
    where
        I: Iterator<Item=&'a Option<String>>
    {
        values.map(|v| Self::infer_value_type(&v))
            .max_by(|v1, v2| v1.0.cmp(&v2.0)) /* pick broadest type */
            .expect("trying to infer a column type without sample values").1
    }

    /// Infers a value's Clickhouse type
    /// Each type is returned with a "breadth" value: low breadth
    /// means the type is very specific, high breadth means you could
    /// store many other types in there
    pub fn infer_value_type(value: &Option<String>) -> (u8, String) {
        match value {
            Some(v) => {
                let parsed = if let Ok(_) = v.parse::<i64>() {
                    (1, "Nullable(Int64)")
                } else if let Ok(_) = v.parse::<f64>() {
                    (2, "Nullable(Float64)")
                } else if v == "true" || v == "false" {
                    (3, "Nullable(Boolean)")
                } else if let Ok(_) = DateTime::parse_from_rfc2822(&v) {
                    (4, "Nullable(DateTime64(1))")
                } else if let Ok(_) = DateTime::parse_from_rfc3339(&v) {
                    (5, "Nullable(DateTime64(1))")
                } else {
                    (6, "Nullable(String)")
                };
                (parsed.0, parsed.1.into())
            },
            None => (0, "Nullable(String)".into())
        }
    }
}
