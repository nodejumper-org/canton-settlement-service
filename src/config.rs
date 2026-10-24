//! Configuration, read once from the environment at startup.
//!
//! Everything is an environment variable so the same image runs anywhere a
//! `.env` file can be mounted. Secrets (a bearer token, an OIDC client secret,
//! the API key) are only ever held here and handed to the client that needs
//! them; nothing logs a [`Config`] (its `Debug` redacts them).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use canton::daml::{Numeric, Party};

/// Why the environment does not describe a runnable service.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

/// How the service authenticates to the participant.
#[derive(Clone)]
pub enum LedgerAuth {
    /// No token: an unauthenticated `LocalNet`.
    None,
    /// A bearer token as issued, for short runs.
    Token(String),
    /// OIDC client credentials, fetched and refreshed by the SDK.
    Oidc {
        token_url: String,
        client_id: String,
        client_secret: String,
        audience: Option<String>,
    },
}

/// The demo traders: two parties that open a small two-leg settlement with
/// each other on a timer, so a `DevNet` deployment always has traffic.
#[derive(Clone, Debug)]
pub struct DemoConfig {
    pub trader_a: Party,
    pub trader_b: Party,
    pub instrument_id: String,
    /// What A pays B.
    pub amount_a: Numeric,
    /// What B pays A.
    pub amount_b: Numeric,
    pub every: Duration,
}

#[derive(Clone)]
pub struct Config {
    pub ledger_url: String,
    pub auth: LedgerAuth,
    pub registry_url: String,
    pub executor: Party,
    pub listen: SocketAddr,
    pub database: PathBuf,
    /// How often the active contract set is re-read even when the update
    /// stream has been quiet. The stream is a doorbell; this is the safety net.
    pub reconcile_every: Duration,
    /// How long a settlement may wait for its allocations by default.
    pub default_ttl: Duration,
    /// Required as a bearer token on every mutating API call. Without it the
    /// API is read-only.
    pub api_key: Option<String>,
    pub demo: Option<DemoConfig>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("ledger_url", &self.ledger_url)
            .field("registry_url", &self.registry_url)
            .field("executor", &self.executor)
            .field("listen", &self.listen)
            .field("database", &self.database)
            .field("reconcile_every", &self.reconcile_every)
            .field("default_ttl", &self.default_ttl)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("demo", &self.demo)
            .finish_non_exhaustive()
    }
}

impl Config {
    /// Read the configuration from the process environment.
    ///
    /// # Errors
    /// A required variable is missing, or a value does not parse.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Read the configuration through `lookup`, which the tests use to supply
    /// a fixed environment.
    ///
    /// # Errors
    /// A required variable is missing, or a value does not parse.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let get = |name: &str| lookup(name).filter(|value| !value.trim().is_empty());
        let required = |name: &str| get(name).ok_or_else(|| ConfigError(format!("set {name}")));
        let party = |name: &str| -> Result<Party, ConfigError> {
            let text = required(name)?;
            crate::model::party_id(&text).map_err(|e| ConfigError(format!("{name}: {e}")))
        };
        let seconds = |name: &str, default: u64| -> Result<Duration, ConfigError> {
            get(name).map_or(Ok(Duration::from_secs(default)), |text| {
                text.parse::<u64>()
                    .ok()
                    .filter(|secs| *secs > 0)
                    .map(Duration::from_secs)
                    .ok_or_else(|| ConfigError(format!("{name} is a positive number of seconds")))
            })
        };

        let auth = match (
            get("SETTLEMENT_LEDGER_TOKEN"),
            get("SETTLEMENT_OIDC_TOKEN_URL"),
        ) {
            (Some(token), _) => LedgerAuth::Token(token),
            (None, Some(token_url)) => LedgerAuth::Oidc {
                token_url,
                client_id: required("SETTLEMENT_OIDC_CLIENT_ID")?,
                client_secret: required("SETTLEMENT_OIDC_CLIENT_SECRET")?,
                audience: get("SETTLEMENT_OIDC_AUDIENCE"),
            },
            (None, None) => LedgerAuth::None,
        };

        let listen = get("SETTLEMENT_LISTEN")
            .unwrap_or_else(|| "0.0.0.0:8080".to_string())
            .parse()
            .map_err(|e| ConfigError(format!("SETTLEMENT_LISTEN: {e}")))?;

        let demo = if get("SETTLEMENT_DEMO_TRADER_A").is_some() {
            let amount = |name: &str, default: &str| -> Result<Numeric, ConfigError> {
                let text = get(name).unwrap_or_else(|| default.to_string());
                Numeric::parse(&text).map_err(|e| ConfigError(format!("{name}: {e}")))
            };
            Some(DemoConfig {
                trader_a: party("SETTLEMENT_DEMO_TRADER_A")?,
                trader_b: party("SETTLEMENT_DEMO_TRADER_B")?,
                instrument_id: get("SETTLEMENT_DEMO_INSTRUMENT")
                    .unwrap_or_else(|| "Amulet".to_string()),
                amount_a: amount("SETTLEMENT_DEMO_AMOUNT_A", "0.1")?,
                amount_b: amount("SETTLEMENT_DEMO_AMOUNT_B", "0.05")?,
                every: seconds("SETTLEMENT_DEMO_EVERY_SECS", 900)?,
            })
        } else {
            None
        };

        Ok(Self {
            ledger_url: required("SETTLEMENT_LEDGER_URL")?,
            auth,
            registry_url: required("SETTLEMENT_REGISTRY_URL")?,
            executor: party("SETTLEMENT_EXECUTOR_PARTY")?,
            listen,
            database: get("SETTLEMENT_DB")
                .map_or_else(|| PathBuf::from("settlement.db"), PathBuf::from),
            reconcile_every: seconds("SETTLEMENT_RECONCILE_SECS", 30)?,
            default_ttl: seconds("SETTLEMENT_DEFAULT_TTL_SECS", 3600)?,
            api_key: get("SETTLEMENT_API_KEY"),
            demo,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const PARTY: &str =
        "executor::1220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn minimal() -> Vec<(&'static str, &'static str)> {
        vec![
            ("SETTLEMENT_LEDGER_URL", "http://localhost:3901"),
            ("SETTLEMENT_REGISTRY_URL", "http://localhost:5012"),
            ("SETTLEMENT_EXECUTOR_PARTY", PARTY),
        ]
    }

    #[test]
    fn the_minimal_environment_runs_read_only_and_unauthenticated() {
        let config = Config::from_lookup(env(&minimal())).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(config.auth, LedgerAuth::None));
        assert!(config.api_key.is_none());
        assert!(config.demo.is_none());
        assert_eq!(config.reconcile_every, Duration::from_secs(30));
    }

    #[test]
    fn oidc_needs_its_client_credentials() {
        let mut pairs = minimal();
        pairs.push(("SETTLEMENT_OIDC_TOKEN_URL", "http://kc/token"));
        let error = Config::from_lookup(env(&pairs))
            .err()
            .map(|e| e.to_string());
        assert_eq!(error.as_deref(), Some("set SETTLEMENT_OIDC_CLIENT_ID"));
    }

    #[test]
    fn a_missing_executor_is_named() {
        let pairs: Vec<_> = minimal()
            .into_iter()
            .filter(|(k, _)| *k != "SETTLEMENT_EXECUTOR_PARTY")
            .collect();
        let error = Config::from_lookup(env(&pairs))
            .err()
            .map(|e| e.to_string());
        assert_eq!(error.as_deref(), Some("set SETTLEMENT_EXECUTOR_PARTY"));
    }

    #[test]
    fn the_debug_output_does_not_carry_the_api_key() {
        let mut pairs = minimal();
        pairs.push(("SETTLEMENT_API_KEY", "very-secret"));
        let config = Config::from_lookup(env(&pairs)).unwrap_or_else(|e| panic!("{e}"));
        assert!(!format!("{config:?}").contains("very-secret"));
    }
}
