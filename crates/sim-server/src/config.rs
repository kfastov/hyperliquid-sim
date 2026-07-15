use std::{env, error::Error, fmt, net::SocketAddr, num::NonZeroUsize, time::Duration};
use url::{Host, Url};

const DEFAULT_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const DEFAULT_WSS_URL: &str = "wss://api.hyperliquid.xyz/ws";

const MAX_QUEUE_CAPACITY: usize = 65_536;
const MAX_BODY_BYTES: usize = 1_048_576;
const MAX_BATCH_SIZE: usize = 128;
const MAX_WS_CONNECTIONS: usize = 4_096;
const MAX_WS_SUBSCRIPTIONS: usize = 128;
const MAX_WS_OUTBOUND_CAPACITY: usize = 4_096;
const MAX_CONCURRENT_REQUESTS: usize = 4_096;
const MAX_REQUESTS_PER_SECOND: usize = 10_000;
const MAX_ACTOR_INTERVAL_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 120_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OracleMode {
    Offline,
    Live,
}

impl fmt::Display for OracleMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Offline => "offline",
            Self::Live => "live",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub public_bind_acknowledged: bool,
    pub seed: u64,
    pub oracle_mode: OracleMode,
    pub oracle_info_url: Url,
    pub oracle_wss_url: Url,
    pub oracle_info_url_overridden: bool,
    pub oracle_wss_url_overridden: bool,
    pub command_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
    pub max_body_bytes: NonZeroUsize,
    pub max_batch_size: NonZeroUsize,
    pub max_ws_connections: NonZeroUsize,
    pub max_ws_subscriptions: NonZeroUsize,
    pub ws_outbound_capacity: NonZeroUsize,
    pub max_concurrent_requests: NonZeroUsize,
    pub requests_per_second: NonZeroUsize,
    pub actors_enabled: bool,
    pub actor_interval: Duration,
    pub reply_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub oracle_completeness_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConfigError {}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(env::VarError::NotUnicode(_)) => {
                Err(ConfigError(format!("{name} must contain valid UTF-8")))
            }
        })
    }

    #[cfg(test)]
    fn from_pairs(pairs: &[(&str, &str)]) -> Result<Self, ConfigError> {
        use std::collections::BTreeMap;

        let values = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>();
        Self::from_lookup(|name| Ok(values.get(name).cloned()))
    }

    fn from_lookup<F>(mut lookup: F) -> Result<Self, ConfigError>
    where
        F: FnMut(&str) -> Result<Option<String>, ConfigError>,
    {
        let bind_addr: SocketAddr = parse_or(&mut lookup, "SIM_BIND_ADDR", "127.0.0.1:8080")?;
        let public_bind_acknowledged = parse_bool(
            "SIM_ACKNOWLEDGE_PUBLIC_BIND",
            lookup("SIM_ACKNOWLEDGE_PUBLIC_BIND")?.as_deref().unwrap_or("false"),
        )?;
        if !bind_addr.ip().is_loopback() && !public_bind_acknowledged {
            return Err(ConfigError(
                "SIM_BIND_ADDR must be loopback unless SIM_ACKNOWLEDGE_PUBLIC_BIND=true".to_owned(),
            ));
        }

        let oracle_mode = match lookup("SIM_ORACLE_MODE")?.as_deref().unwrap_or("offline") {
            "offline" => OracleMode::Offline,
            "live" => OracleMode::Live,
            _ => {
                return Err(ConfigError(
                    "SIM_ORACLE_MODE must be either offline or live".to_owned(),
                ));
            }
        };

        let info_override = lookup("SIM_ORACLE_INFO_URL")?;
        let wss_override = lookup("SIM_ORACLE_WSS_URL")?;
        let oracle_info_url = validate_upstream_url(
            "SIM_ORACLE_INFO_URL",
            info_override.as_deref().unwrap_or(DEFAULT_INFO_URL),
            UpstreamKind::Info,
        )?;
        let oracle_wss_url = validate_upstream_url(
            "SIM_ORACLE_WSS_URL",
            wss_override.as_deref().unwrap_or(DEFAULT_WSS_URL),
            UpstreamKind::WebSocket,
        )?;

        Ok(Self {
            bind_addr,
            public_bind_acknowledged,
            seed: parse_or(&mut lookup, "SIM_SEED", "6")?,
            oracle_mode,
            oracle_info_url,
            oracle_wss_url,
            oracle_info_url_overridden: info_override.is_some(),
            oracle_wss_url_overridden: wss_override.is_some(),
            command_capacity: bounded_usize(
                &mut lookup,
                "SIM_COMMAND_CAPACITY",
                256,
                MAX_QUEUE_CAPACITY,
            )?,
            event_capacity: bounded_usize(
                &mut lookup,
                "SIM_EVENT_CAPACITY",
                1_024,
                MAX_QUEUE_CAPACITY,
            )?,
            max_body_bytes: bounded_usize(
                &mut lookup,
                "SIM_MAX_BODY_BYTES",
                16_384,
                MAX_BODY_BYTES,
            )?,
            max_batch_size: bounded_usize(&mut lookup, "SIM_MAX_BATCH_SIZE", 16, MAX_BATCH_SIZE)?,
            max_ws_connections: bounded_usize(
                &mut lookup,
                "SIM_MAX_WS_CONNECTIONS",
                256,
                MAX_WS_CONNECTIONS,
            )?,
            max_ws_subscriptions: bounded_usize(
                &mut lookup,
                "SIM_MAX_WS_SUBSCRIPTIONS",
                16,
                MAX_WS_SUBSCRIPTIONS,
            )?,
            ws_outbound_capacity: bounded_usize(
                &mut lookup,
                "SIM_WS_OUTBOUND_CAPACITY",
                64,
                MAX_WS_OUTBOUND_CAPACITY,
            )?,
            max_concurrent_requests: bounded_usize(
                &mut lookup,
                "SIM_MAX_CONCURRENT_REQUESTS",
                128,
                MAX_CONCURRENT_REQUESTS,
            )?,
            requests_per_second: bounded_usize(
                &mut lookup,
                "SIM_REQUESTS_PER_SECOND",
                100,
                MAX_REQUESTS_PER_SECOND,
            )?,
            actors_enabled: parse_bool(
                "SIM_ACTORS_ENABLED",
                lookup("SIM_ACTORS_ENABLED")?.as_deref().unwrap_or("false"),
            )?,
            actor_interval: bounded_duration(
                &mut lookup,
                "SIM_ACTOR_INTERVAL_MS",
                1_000,
                MAX_ACTOR_INTERVAL_MS,
            )?,
            reply_timeout: bounded_duration(
                &mut lookup,
                "SIM_REPLY_TIMEOUT_MS",
                2_000,
                MAX_TIMEOUT_MS,
            )?,
            shutdown_timeout: bounded_duration(
                &mut lookup,
                "SIM_SHUTDOWN_TIMEOUT_MS",
                5_000,
                MAX_TIMEOUT_MS,
            )?,
            oracle_completeness_timeout: bounded_duration(
                &mut lookup,
                "SIM_ORACLE_COMPLETENESS_TIMEOUT_MS",
                10_000,
                MAX_TIMEOUT_MS,
            )?,
        })
    }

    pub fn validation_summary(&self) -> String {
        format!(
            "sim-server configuration valid: bind={} public_bind_acknowledged={} seed={} oracle_mode={} oracle_info_url={} oracle_wss_url={} command_capacity={} event_capacity={} max_body_bytes={} max_batch_size={} max_ws_connections={} max_ws_subscriptions={} ws_outbound_capacity={} max_concurrent_requests={} requests_per_second={} actors_enabled={} actor_interval_ms={} reply_timeout_ms={} shutdown_timeout_ms={} oracle_completeness_timeout_ms={}",
            self.bind_addr,
            self.public_bind_acknowledged,
            self.seed,
            self.oracle_mode,
            endpoint_source(self.oracle_info_url_overridden),
            endpoint_source(self.oracle_wss_url_overridden),
            self.command_capacity,
            self.event_capacity,
            self.max_body_bytes,
            self.max_batch_size,
            self.max_ws_connections,
            self.max_ws_subscriptions,
            self.ws_outbound_capacity,
            self.max_concurrent_requests,
            self.requests_per_second,
            self.actors_enabled,
            self.actor_interval.as_millis(),
            self.reply_timeout.as_millis(),
            self.shutdown_timeout.as_millis(),
            self.oracle_completeness_timeout.as_millis(),
        )
    }
}

fn endpoint_source(overridden: bool) -> &'static str {
    if overridden { "explicit-override" } else { "fixed-default" }
}

fn parse_or<T, F>(lookup: &mut F, name: &str, default: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
    F: FnMut(&str) -> Result<Option<String>, ConfigError>,
{
    lookup(name)?
        .unwrap_or_else(|| default.to_owned())
        .parse()
        .map_err(|error| ConfigError(format!("invalid {name}: {error}")))
}

fn bounded_usize<F>(
    lookup: &mut F,
    name: &str,
    default: usize,
    maximum: usize,
) -> Result<NonZeroUsize, ConfigError>
where
    F: FnMut(&str) -> Result<Option<String>, ConfigError>,
{
    let value = parse_or(lookup, name, &default.to_string())?;
    if value == 0 || value > maximum {
        return Err(ConfigError(format!("{name} must be between 1 and {maximum}")));
    }
    NonZeroUsize::new(value).ok_or_else(|| ConfigError(format!("{name} must not be zero")))
}

fn bounded_duration<F>(
    lookup: &mut F,
    name: &str,
    default_ms: u64,
    maximum_ms: u64,
) -> Result<Duration, ConfigError>
where
    F: FnMut(&str) -> Result<Option<String>, ConfigError>,
{
    let value = parse_or(lookup, name, &default_ms.to_string())?;
    if value == 0 || value > maximum_ms {
        return Err(ConfigError(format!("{name} must be between 1 and {maximum_ms}")));
    }
    Ok(Duration::from_millis(value))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, ConfigError> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(ConfigError(format!("{name} must be true, false, 1, or 0"))),
    }
}

#[derive(Clone, Copy)]
enum UpstreamKind {
    Info,
    WebSocket,
}

fn validate_upstream_url(name: &str, value: &str, kind: UpstreamKind) -> Result<Url, ConfigError> {
    let url = Url::parse(value).map_err(|error| ConfigError(format!("invalid {name}: {error}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError(format!("{name} must not contain credentials")));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ConfigError(format!("{name} must not contain a query or fragment")));
    }
    let host = url.host().ok_or_else(|| ConfigError(format!("{name} must contain a host")))?;
    let loopback = match host {
        Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        Host::Ipv4(address) => address.is_loopback(),
        Host::Ipv6(address) => address.is_loopback(),
    };
    let (secure_scheme, local_scheme, required_path) = match kind {
        UpstreamKind::Info => ("https", "http", "/info"),
        UpstreamKind::WebSocket => ("wss", "ws", "/ws"),
    };
    if url.scheme() != secure_scheme && !(loopback && url.scheme() == local_scheme) {
        return Err(ConfigError(format!(
            "{name} must use {secure_scheme}, or {local_scheme} for a loopback host"
        )));
    }
    if url.path() != required_path {
        return Err(ConfigError(format!(
            "{name} must target the fixed read-only {required_path} path"
        )));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe_and_deterministic() {
        let config = Config::from_pairs(&[]).expect("safe defaults");
        assert_eq!(config.bind_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.seed, 6);
        assert_eq!(config.oracle_mode, OracleMode::Offline);
        assert_eq!(config.oracle_info_url.as_str(), DEFAULT_INFO_URL);
        assert_eq!(config.oracle_wss_url.as_str(), DEFAULT_WSS_URL);
        assert!(!config.public_bind_acknowledged);
        assert!(!config.actors_enabled);
        assert_eq!(config.command_capacity.get(), 256);
        assert_eq!(config.event_capacity.get(), 1_024);
        assert_eq!(config.max_body_bytes.get(), 16_384);
        assert_eq!(config.max_batch_size.get(), 16);
        assert_eq!(config.max_ws_connections.get(), 256);
        assert_eq!(config.max_ws_subscriptions.get(), 16);
        assert_eq!(config.ws_outbound_capacity.get(), 64);
        assert_eq!(config.max_concurrent_requests.get(), 128);
        assert_eq!(config.requests_per_second.get(), 100);
        assert_eq!(config.actor_interval, Duration::from_millis(1_000));
        assert_eq!(config.reply_timeout, Duration::from_millis(2_000));
        assert_eq!(config.shutdown_timeout, Duration::from_millis(5_000));
        assert_eq!(config.oracle_completeness_timeout, Duration::from_millis(10_000));
    }

    #[test]
    fn explicit_bounded_configuration_is_accepted() {
        let config = Config::from_pairs(&[
            ("SIM_BIND_ADDR", "0.0.0.0:9000"),
            ("SIM_ACKNOWLEDGE_PUBLIC_BIND", "true"),
            ("SIM_SEED", "42"),
            ("SIM_ORACLE_MODE", "live"),
            ("SIM_ORACLE_INFO_URL", "http://127.0.0.1:9100/info"),
            ("SIM_ORACLE_WSS_URL", "ws://localhost:9101/ws"),
            ("SIM_COMMAND_CAPACITY", "1"),
            ("SIM_EVENT_CAPACITY", "65536"),
            ("SIM_ACTORS_ENABLED", "1"),
            ("SIM_ACTOR_INTERVAL_MS", "60000"),
        ])
        .expect("explicit valid config");
        assert_eq!(config.seed, 42);
        assert_eq!(config.oracle_mode, OracleMode::Live);
        assert!(config.public_bind_acknowledged);
        assert!(config.oracle_info_url_overridden);
        assert!(config.oracle_wss_url_overridden);
        assert!(config.actors_enabled);
    }

    #[test]
    fn non_loopback_bind_requires_explicit_acknowledgement() {
        let error = Config::from_pairs(&[("SIM_BIND_ADDR", "0.0.0.0:8080")]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "SIM_BIND_ADDR must be loopback unless SIM_ACKNOWLEDGE_PUBLIC_BIND=true"
        );
    }

    #[test]
    fn every_bounded_value_rejects_zero() {
        for name in [
            "SIM_COMMAND_CAPACITY",
            "SIM_EVENT_CAPACITY",
            "SIM_MAX_BODY_BYTES",
            "SIM_MAX_BATCH_SIZE",
            "SIM_MAX_WS_CONNECTIONS",
            "SIM_MAX_WS_SUBSCRIPTIONS",
            "SIM_WS_OUTBOUND_CAPACITY",
            "SIM_MAX_CONCURRENT_REQUESTS",
            "SIM_REQUESTS_PER_SECOND",
            "SIM_ACTOR_INTERVAL_MS",
            "SIM_REPLY_TIMEOUT_MS",
            "SIM_SHUTDOWN_TIMEOUT_MS",
            "SIM_ORACLE_COMPLETENESS_TIMEOUT_MS",
        ] {
            assert!(Config::from_pairs(&[(name, "0")]).is_err(), "{name}");
        }
    }

    #[test]
    fn every_bounded_value_rejects_values_above_its_maximum() {
        for (name, value) in [
            ("SIM_COMMAND_CAPACITY", "65537"),
            ("SIM_EVENT_CAPACITY", "65537"),
            ("SIM_MAX_BODY_BYTES", "1048577"),
            ("SIM_MAX_BATCH_SIZE", "129"),
            ("SIM_MAX_WS_CONNECTIONS", "4097"),
            ("SIM_MAX_WS_SUBSCRIPTIONS", "129"),
            ("SIM_WS_OUTBOUND_CAPACITY", "4097"),
            ("SIM_MAX_CONCURRENT_REQUESTS", "4097"),
            ("SIM_REQUESTS_PER_SECOND", "10001"),
            ("SIM_ACTOR_INTERVAL_MS", "60001"),
            ("SIM_REPLY_TIMEOUT_MS", "120001"),
            ("SIM_SHUTDOWN_TIMEOUT_MS", "120001"),
            ("SIM_ORACLE_COMPLETENESS_TIMEOUT_MS", "120001"),
        ] {
            assert!(Config::from_pairs(&[(name, value)]).is_err(), "{name}");
        }
    }

    #[test]
    fn upstream_urls_reject_credentials_and_state_changing_paths() {
        for (name, value) in [
            ("SIM_ORACLE_INFO_URL", "https://user:secret@example.com/info"),
            ("SIM_ORACLE_INFO_URL", "https://example.com/exchange"),
            ("SIM_ORACLE_INFO_URL", "https://example.com/info?token=secret"),
            ("SIM_ORACLE_WSS_URL", "wss://example.com/exchange"),
            ("SIM_ORACLE_WSS_URL", "ftp://example.com/ws"),
        ] {
            assert!(Config::from_pairs(&[(name, value)]).is_err(), "{name}={value}");
        }
    }

    #[test]
    fn insecure_upstream_urls_are_loopback_only() {
        assert!(Config::from_pairs(&[("SIM_ORACLE_INFO_URL", "http://example.com/info")]).is_err());
        assert!(Config::from_pairs(&[("SIM_ORACLE_WSS_URL", "ws://example.com/ws")]).is_err());
    }

    #[test]
    fn summary_is_stable_and_does_not_disclose_endpoint_values() {
        let config = Config::from_pairs(&[
            ("SIM_ORACLE_INFO_URL", "https://private.example/info"),
            ("SIM_ORACLE_WSS_URL", "wss://private.example/ws"),
        ])
        .expect("valid overrides");
        let summary = config.validation_summary();
        assert!(summary.contains("oracle_info_url=explicit-override"));
        assert!(summary.contains("oracle_wss_url=explicit-override"));
        assert!(!summary.contains("private.example"));
    }
}
