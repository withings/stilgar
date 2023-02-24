use serde::{Serialize, Deserialize};
use std::net::IpAddr;
use std::fs::File;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use directories::ProjectDirs;
use serde_with::{DisplayFromStr, serde_as};
use serde_yaml;
use cron;
use log;

/// Configuration defaults
pub mod defaults {
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;
    use cron;

    pub fn server_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)) }
    pub fn server_port() -> u16 { 8080 }

    pub fn forwarder_beanstalk() -> String { String::from("127.0.0.1:11300") }
    pub fn forwarder_schedule() -> cron::Schedule { cron::Schedule::from_str("0 * * * * * *").unwrap() }
}

/// Server block
#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct Server {
    /// The IP we're going to bind to
    #[serde(default = "defaults::server_ip")]
    pub ip: IpAddr,
    /// The port we're going to listen on
    #[serde(default = "defaults::server_port")]
    pub port: u16,
    /// A list of allowed origins (CORS)
    #[serde(default)]
    pub origins: Vec<String>,
}

impl Default for Server {
    /// Builds a default server block in case none is provided
    fn default() -> Self {
        return Self {
            ip: defaults::server_ip(),
            port: defaults::server_port(),
            origins: vec!(),
        }
    }
}

/// Forwarder block
#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct Forwarder {
    /// Hostname and port to the beanstalkd server
    #[serde(default = "defaults::forwarder_beanstalk")]
    pub beanstalk: String,
    /// A CRON to define the processing time slots, supports seconds
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "defaults::forwarder_schedule")]
    pub schedule: cron::Schedule,
}

impl Default for Forwarder {
    /// Builds a default forwarder block in case none is provided
    fn default() -> Self {
        return Self {
            beanstalk: defaults::forwarder_beanstalk(),
            schedule: defaults::forwarder_schedule(),
        }
    }
}

/// Convenience type: arbitrary key-value settings (for destinations)
pub type Settings = HashMap<String, serde_yaml::Value>;

/// A single destination block
#[derive(Serialize, Deserialize)]
pub struct Destination {
    /// The destination type
    #[serde(rename = "type")]
    pub destination_type: String,
    /// Some key-value settings specific to this destination
    #[serde(flatten)]
    pub settings: Settings,
}

/// The overall configuration file
#[derive(Serialize, Deserialize)]
pub struct Configuration {
    /// A server block
    #[serde(default)]
    pub server: Server,
    /// A forwarder block
    #[serde(default)]
    pub forwarder: Forwarder,
    /// And some destinations
    pub destinations: Vec<Destination>,
}

/// Parse a configuration file given a path
fn parse_configuration_file(path: &Path) -> Result<Configuration, String> {
    let path_str = path.to_str().unwrap();
    log::debug!("attempting to parse {} as a configuration file", path_str);

    let file = match File::open(path).map_err(|e| e.to_string()) {
        Ok(f) => f,
        Err(e) => {
            log::debug!("cannot open {}: {}", path_str, e);
            return Err(e);
        }
    };

    let configuration: Configuration = match serde_yaml::from_reader(file) {
        Ok(c) => c,
        Err(e) => {
            log::debug!("cannot parse {}: {}", path_str, e);
            return Err(e.to_string());
        }
    };

    log::debug!("successfully parsed {} as a configuration file", path_str);
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
