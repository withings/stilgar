use crate::config;

use flexi_logger::{DeferredNow, LogSpecification, Logger};
use flexi_logger::writers::{SyslogWriter, SyslogConnection, SyslogLineHeader, SyslogFacility};
use serde::de;

pub fn init_logger(log_config: &config::Logging) {
    let mut spec = LogSpecification::builder();
    spec.module("stilgar", log_config.level);
    let logger = Logger::with(spec.build());

    let logger = match &log_config.syslog {
        Some(syslog_config) => {
            let addr = (syslog_config.host.clone(), syslog_config.port);
            let connection = match syslog_config.protocol.as_str() {
                "tcp" => SyslogConnection::try_tcp(addr),
                "udp" => SyslogConnection::try_udp(("0.0.0.0".into(), 0), addr),
                _ => panic!("unknown syslog protocol"),
            }.expect("failed to create a connection to syslog");

            let writer = SyslogWriter::builder(
                connection,
                SyslogLineHeader::Rfc3164,
                syslog_config.facility
            )
                .process(syslog_config.process.as_deref())
                .format(record_formatter)
                .build()
                .expect("failed to build syslog writer");

            logger.log_to_writer(writer)
        },
        None => logger.log_to_stdout().format(record_formatter)
    };

    logger.start().expect("failed to start logger");
}

fn record_formatter(
    writer: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &log::Record) -> Result<(), std::io::Error> {
    writeln!(
        writer,
        "{} {} [{}]{}{} {}",
        now.format_rfc3339(),
        record.level(),
        record.module_path().unwrap_or("stilgar::<unknown>"),
        record.key_values().get("rid".into()).map(|i| format!(" [{}]", i)).unwrap_or("".into()),
        record.key_values().get("mid".into()).map(|i| format!(" [{}]", i)).unwrap_or("".into()),
        record.args(),
    )
}

pub fn parse_facility<'de, D>(deserializer: D) -> Result<SyslogFacility, D::Error>
where
    D: de::Deserializer<'de>,
{
    let v: serde_json::Value = de::Deserialize::deserialize(deserializer)?;
    let facility_str = v.as_str().ok_or(de::Error::custom("the syslog facility type should be a string"))?;

    Ok(match facility_str {
        "kern" => SyslogFacility::Kernel,
        "user" => SyslogFacility::UserLevel,
        "mail" => SyslogFacility::MailSystem,
        "daemon" => SyslogFacility::SystemDaemons,
        "auth" => SyslogFacility::Authorization,
        "syslog" => SyslogFacility::SyslogD,
        "lpr" => SyslogFacility::LinePrinter,
        "news" => SyslogFacility::News,
        "uucp" => SyslogFacility::Uucp,
        "cron" => SyslogFacility::Clock,
        "authpriv" => SyslogFacility::Authorization2,
        "ftp" => SyslogFacility::Ftp,
        "ntp" => SyslogFacility::Ntp,
        "security" => SyslogFacility::LogAudit,
        "console" => SyslogFacility::LogAlert,
        "solaris-cron" => SyslogFacility::Clock2,
        "local0" => SyslogFacility::LocalUse0,
        "local1" => SyslogFacility::LocalUse1,
        "local2" => SyslogFacility::LocalUse2,
        "local3" => SyslogFacility::LocalUse3,
        "local4" => SyslogFacility::LocalUse4,
        "local5" => SyslogFacility::LocalUse5,
        "local6" => SyslogFacility::LocalUse6,
        "local7" => SyslogFacility::LocalUse7,
        _ => return Err(de::Error::custom("unknown syslog facility")),
    })
}
