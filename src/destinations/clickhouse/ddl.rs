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

use crate::destinations::{StorageResult, StorageError};
use crate::destinations::clickhouse::Clickhouse;

use std::collections::HashMap;
use itertools::Itertools;
use lazy_static::lazy_static;
use indoc::{indoc, formatdoc};
use chrono::DateTime;
use secular::lower_lay_string;

const MAX_IDENTIFIER_LENGTH: usize = 64;

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

        `user_id` Nullable(String),
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

const TYPE_NULLABLE_BOOL: &str = "Nullable(Bool)";
const TYPE_NULLABLE_BOOL_BREATH: u8 = 1;
const TYPE_NULLABLE_INT: &str = "Nullable(Int64)";
const TYPE_NULLABLE_INT_BREATH: u8 = 2;
const TYPE_NULLABLE_FLOAT: &str = "Nullable(Float64)";
const TYPE_NULLABLE_FLOAT_BREATH: u8 = 3;
const TYPE_NULLABLE_DATETIME: &str = "Nullable(DateTime64(1))";
const TYPE_NULLABLE_DATETIME_BREATH: u8 = 4;
const TYPE_NULLABLE_LARGEST: &str = "Nullable(String)";
const TYPE_NULLABLE_LARGEST_BREATH: u8 = 5;

lazy_static! {
    static ref TYPES_BREADTH_MAP: HashMap<String, u8> = HashMap::from([
        (TYPE_NULLABLE_BOOL.into(), TYPE_NULLABLE_BOOL_BREATH),
        (TYPE_NULLABLE_INT.into(), TYPE_NULLABLE_INT_BREATH),
        (TYPE_NULLABLE_FLOAT.into(), TYPE_NULLABLE_FLOAT_BREATH),
        (TYPE_NULLABLE_DATETIME.into(), TYPE_NULLABLE_DATETIME_BREATH),
    ]);
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

    /// Validates a table or column name (maximum size, REGEX)
    fn is_valid_identifier(&self, column_name: &String) -> bool {
        column_name.len() > 0 && column_name.len() <= MAX_IDENTIFIER_LENGTH && self.identifier_regex.is_match(column_name)
    }

    /// Transforms a string into a valid identifier (table name)
    pub fn make_valid_identifier(name: &str) -> String {
        let normalised_identifier = lower_lay_string(name)
            .to_lowercase()
            .replace(|c: char| !c.is_ascii_graphic() && !c.is_whitespace(), "")
            .replace(|c: char| !c.is_alphanumeric(), "_");

        match normalised_identifier.chars().nth(0).unwrap().is_digit(10) {
            true => format!("track_{}", normalised_identifier),
            false => normalised_identifier
        }
    }

    /// Adds any missing columns to an existing table
    pub async fn extend_existing_table(&self, table_name: &String, expected_columns: HashMap<String, String>) -> StorageResult {
        /* Validate the new column names */
        for column_name in expected_columns.keys() {
            if !self.is_valid_identifier(column_name) {
                return Err(StorageError::GrowthControl(format!(
                    "new column name is invalid ({}), refusing to extend table {}",
                    column_name, table_name
                )))
            }
        }

        /* Figure out which columns are missing */
        let current_columns = self.describe_table(&table_name).await.expect("failed to describe table");
        let missing_columns = expected_columns.iter().filter(|(k, _v)| !current_columns.keys().contains(k)).collect_vec();

        /* Lil' bit of growth control */
        if self.max_table_expansion != 0 && missing_columns.len() > self.max_table_expansion {
            return Err(StorageError::GrowthControl(format!(
                "table expansion requires adding {} column(s), but max_table_expansion is {}",
                missing_columns.len(), self.max_table_expansion
            )))
        }

        if self.max_table_width != 0 && (current_columns.len() + missing_columns.len()) > self.max_table_width {
            return Err(StorageError::GrowthControl(format!(
                "table expansion requires adding {} column(s), but the table's width would then exceed max_table_width ({})",
                missing_columns.len(), self.max_table_width
            )))
        }

        log::trace!("{} missing column(s) in table {}: will try to extend", missing_columns.len(), table_name);
        let use_aggregate_function = self.table_is_aggregating(&table_name).await?;

        /* Create them */
        for (column_name, column_type) in missing_columns {
            let aggregating_column_type = format!("SimpleAggregateFunction(anyLast, {})", column_type);
            let final_column_type = match use_aggregate_function {
                true => &aggregating_column_type,
                false => &column_type
            };
            log::info!("creating missing column: {}.{} {}", table_name, column_name, column_type);
            let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", table_name, column_name, final_column_type);
            self.nio(sql).await?;
        }
        Ok(())
    }

    /// Modify column types to broader options to try and match values which don't currently fit
    pub async fn reshape_existing_table(&self, table_name: &String, expected_columns: HashMap<String, String>) -> StorageResult {
        /* Identify columns which appear to have the wrong type now */
        let current_columns = self.describe_table(&table_name).await.expect("failed to describe table");
        let column_retypes = expected_columns.iter()
            .map(|(name, exp_type)| current_columns.get(name.as_str()).map(|cur_type| (name, cur_type, exp_type))).flatten()
            .filter(|(name, _cur_type, exp_type)| current_columns.get(name.as_str()).map(|cur_type| cur_type != *exp_type).unwrap_or(false))
            .collect_vec();

        for (column_name, current_type, new_type) in column_retypes {
            /* If the current type is unknown to Stilgar, it was probably created externally or as part of the initial DDL
             * In that case, we don't want to modify it automatically */
            let old_column_breadth = match TYPES_BREADTH_MAP.get(current_type) {
                Some(b) => *b,
                None => {
                    log::warn!("column {}.{} appears to have the wrong type ({}, should be {}) but the current type has unknown breadth",
                               table_name, column_name, current_type, new_type);
                    continue;
                }
            };
            let new_column_breadth = *TYPES_BREADTH_MAP.get(new_type).unwrap_or(&TYPE_NULLABLE_LARGEST_BREATH);

            if new_column_breadth <= old_column_breadth {
                /* The new type has been inferred as narrower, adjusting would truncate data */
                continue;
            }

            log::info!("adjusting type for column {}.{}: expanding from {} to {}", table_name, column_name, current_type, new_type);
            let sql = format!("ALTER TABLE {} MODIFY COLUMN {} {}", table_name, column_name, new_type);
            self.nio(sql).await?;
        }

        Ok(())
    }

    /// Designs a basic table with common fields and context info.
    pub fn get_basic_table_ddl(&self, table_name: &String) -> Result<String, StorageError> {
        match self.is_valid_identifier(table_name) {
            true => Ok(format!(
                "CREATE TABLE {} ({} {}) ENGINE = ReplacingMergeTree PARTITION BY toDate(received_at) ORDER BY (received_at, id)",
                table_name,
                EVENT_BASICS.to_string(),
                CONTEXT.to_string()
            )),
            false => Err(StorageError::GrowthControl(format!(
                "new table name is invalid ({}), cannot generate DDL",
                table_name
            )))
        }
    }

    /// Infer a column's type using known values
    pub fn infer_vec_type<'a, I>(values: I) -> String
    where
        I: Iterator<Item=&'a Option<String>>
    {
        values.map(|v| Self::infer_value_type(&v))
            .max_by(|v1, v2| v1.1.cmp(&v2.1)) /* pick broadest type */
            .expect("trying to infer a column type without sample values").0
    }

    /// Infers a value's Clickhouse type
    /// Each type is returned with a "breadth" value: low breadth
    /// means the type is very specific, high breadth means you could
    /// store many other types in there
    pub fn infer_value_type(value: &Option<String>) -> (String, u8) {
        let type_breadth = match value {
            Some(v) => {
                if v == "true" || v == "false" {
                    (TYPE_NULLABLE_BOOL, TYPE_NULLABLE_BOOL_BREATH)
                } else if let Ok(_) = v.parse::<i64>() {
                    (TYPE_NULLABLE_INT, TYPE_NULLABLE_INT_BREATH)
                } else if let Ok(_) = v.parse::<f64>() {
                    (TYPE_NULLABLE_FLOAT, TYPE_NULLABLE_FLOAT_BREATH)
                } else if let Ok(_) = DateTime::parse_from_rfc2822(&v) {
                    (TYPE_NULLABLE_DATETIME, TYPE_NULLABLE_DATETIME_BREATH)
                } else if let Ok(_) = DateTime::parse_from_rfc3339(&v) {
                    (TYPE_NULLABLE_DATETIME, TYPE_NULLABLE_DATETIME_BREATH)
                } else {
                    (TYPE_NULLABLE_LARGEST, TYPE_NULLABLE_LARGEST_BREATH)
                }
            },
            None => (TYPE_NULLABLE_LARGEST, TYPE_NULLABLE_LARGEST_BREATH)
        };

        (type_breadth.0.into(), type_breadth.1)
    }
}
