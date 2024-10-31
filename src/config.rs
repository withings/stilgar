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

use serde::Deserialize;
use std::net::IpAddr;
use std::fs::File;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use ipnetwork::IpNetwork;
use directories::ProjectDirs;
use byte_unit::Byte as ByteSize;
use flexi_logger::writers::SyslogFacility;
use serde_with::serde_as;
use serde_yaml;
use log;

/// Configuration defaults
pub mod defaults {
    use std::net::{IpAddr, Ipv4Addr};
    use byte_unit::{Byte as ByteSize, Unit as ByteUnit};
    use flexi_logger::writers::SyslogFacility;

    pub fn server_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)) }
    pub fn server_port() -> u16 { 8080 }
    pub fn server_payload_size_limit() -> ByteSize { ByteSize::from_f32_with_unit(4.0, ByteUnit::MiB).unwrap() }

    pub fn logging_level() -> log::LevelFilter { log::LevelFilter::Info }
    pub fn logging_syslog_port() -> u16 { 514 }
    pub fn logging_syslog_facility() -> SyslogFacility { SyslogFacility::UserLevel }

    pub fn forwarder_beanstalk() -> String { String::from("127.0.0.1:11300") }
}

/// Server admin block
#[derive(Deserialize)]
pub struct Admin {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub allowed_networks: Option<Vec<IpNetwork>>,
}

impl Default for Admin {
    /// Builds a default server admin block in case none is provided
    fn default() -> Self {
        Self {
            username: None,
            password: None,
            allowed_networks: None,
        }
    }
}

/// Server block
#[serde_as]
#[derive(Deserialize)]
pub struct Server {
    /// The IP we're going to bind to
    #[serde(default = "defaults::server_ip")]
    pub ip: IpAddr,
    /// The port we're going to listen on
    #[serde(default = "defaults::server_port")]
    pub port: u16,
    /// The write key (can be left empty)
    #[serde(default = "defaults::server_payload_size_limit")]
    pub payload_size_limit: ByteSize,
    /// A list of allowed origins (CORS)
    #[serde(default)]
    pub origins: Vec<String>,
    #[serde(default)]
    pub admin: Arc<Option<Admin>>,
}

impl Default for Server {
    /// Builds a default server block in case none is provided
    fn default() -> Self {
        return Self {
            ip: defaults::server_ip(),
            port: defaults::server_port(),
            payload_size_limit: defaults::server_payload_size_limit(),
            origins: vec!(),
            admin: Arc::new(None),
        }
    }
}

/// Syslog configuration
#[serde_as]
#[derive(Deserialize)]
pub struct Syslog {
    pub protocol: String,
    pub host: String,
    #[serde(default = "defaults::logging_syslog_port")]
    pub port: u16,
    #[serde(default = "defaults::logging_syslog_facility", deserialize_with = "crate::logging::parse_facility")]
    pub facility: SyslogFacility,
    #[serde(default)]
    pub process: Option<String>,
}

/// Logging block
#[serde_as]
#[derive(Deserialize)]
pub struct Logging {
    #[serde(default = "defaults::logging_level")]
    pub level: log::LevelFilter,
    #[serde(default)]
    pub syslog: Option<Syslog>,
}

impl Default for Logging {
    /// Builds a default logging block in case none is provided
    fn default() -> Self {
        return Self {
            level: defaults::logging_level(),
            syslog: None,
        }
    }
}

/// Forwarder block
#[serde_as]
#[derive(Deserialize)]
pub struct Forwarder {
    /// Hostname and port to the beanstalkd server
    #[serde(default = "defaults::forwarder_beanstalk")]
    pub beanstalk: String,
}

impl Default for Forwarder {
    /// Builds a default forwarder block in case none is provided
    fn default() -> Self {
        return Self {
            beanstalk: defaults::forwarder_beanstalk(),
        }
    }
}

/// Convenience type: arbitrary key-value settings (for destinations)
pub type Settings = HashMap<String, serde_yaml::Value>;

/// A single destination block
#[derive(Deserialize)]
pub struct Destination {
    /// The destination type
    #[serde(rename = "type")]
    pub destination_type: String,
    /// The write keys for this destination
    pub write_keys: HashSet<String>,
    /// Some key-value settings specific to this destination
    #[serde(flatten)]
    pub settings: Settings,
}

/// The overall configuration file
#[derive(Deserialize)]
pub struct Configuration {
    /// A server block
    #[serde(default)]
    pub server: Server,
    /// A logging block
    #[serde(default)]
    pub logging: Logging,
    /// A forwarder block
    #[serde(default)]
    pub forwarder: Forwarder,
    /// And some destinations
    pub destinations: Vec<Destination>,
}

/// Parse a configuration file given a path
fn parse_configuration_file(path: &Path) -> Result<Configuration, String> {
    let path_str = path.to_str().unwrap();
    let file = File::open(path).map_err(|e| format!("{}: {}", path_str, e))?;
    let configuration = serde_yaml::from_reader(file).map_err(|e| format!("{}: {}", path_str, e))?;
    Ok(configuration)
}

/// Locates and parses the configuration file
pub fn get_configuration(cmd_arg: Option<&String>) -> Result<Configuration, String> {
    let given_location = cmd_arg
        .map(|s| PathBuf::from(s))
        .or(std::env::var("STILGAR_CONFIG").map(|s| PathBuf::from(s)).ok());

    /* If a path was given on the command line, ignore all other options */
    if let Some(path) = given_location {
        return parse_configuration_file(path.as_path());
    }

    /* Otherwise, try and guess */
    let xdg_dirs = ProjectDirs::from("com", "withings", "stilgar");
    let inferred_locations = [
        Some(PathBuf::from("/etc/withings/stilgar.yml")),
        Some(PathBuf::from("/etc/withings/stilgar.yaml")),
        xdg_dirs.as_ref().map(|dirs| PathBuf::from(dirs.config_dir()).join("stilgar.yml")),
        xdg_dirs.as_ref().map(|dirs| PathBuf::from(dirs.config_dir()).join("stilgar.yaml")),
        std::env::current_dir().map(|p| p.join("stilgar.yml")).ok(),
        std::env::current_dir().map(|p| p.join("stilgar.yaml")).ok(),
    ];

    let configuration = inferred_locations.iter()
        .flatten()
        .filter(|p| p.as_path().is_file())
        .map(|p| parse_configuration_file(p))
        .flatten()
        .next();

    configuration.ok_or(String::from("no valid configuration file found"))
}
