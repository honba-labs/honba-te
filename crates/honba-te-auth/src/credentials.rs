//! Broker credentials and secure storage backends.
//!
//! Secrets are sourced from (in order of security):
//! 1. The native OS keyring via `keyring-rs` (feature `keyring`)
//! 2. Environment variables (`HONBA_{BROKER}_{FIELD}`) for headless servers
//! 3. An in-process memory store for deterministic tests

use crate::error::AuthError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio::sync::Mutex as AsyncMutex;

/// Supported brokers with dedicated headless login flows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BrokerName {
    /// Zerodha Kite Connect v3
    Zerodha,
    /// Upstox v2 OAuth2
    Upstox,
    /// Angel One SmartAPI 2.0
    AngelOne,
}

impl BrokerName {
    /// Canonical adapter identifier ("zerodha", "upstox", "angelone").
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zerodha => "zerodha",
            Self::Upstox => "upstox",
            Self::AngelOne => "angelone",
        }
    }

    /// Environment variable prefix, e.g. `HONBA_ZERODHA_API_KEY`.
    fn env_prefix(self) -> String {
        format!("HONBA_{}", self.as_str().to_uppercase())
    }
}

impl FromStr for BrokerName {
    type Err = AuthError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "zerodha" | "kite" => Ok(Self::Zerodha),
            "upstox" => Ok(Self::Upstox),
            "angelone" | "angel" | "smartapi" => Ok(Self::AngelOne),
            other => Err(AuthError::Config(format!(
                "unknown broker name '{other}' (expected zerodha | upstox | angelone)"
            ))),
        }
    }
}

/// Broker login credentials for headless session bootstrap.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerCredentials {
    /// Broker adapter name
    pub broker: String,
    /// API key (public app identifier)
    pub api_key: String,
    /// API secret (used for login checksums / token exchange)
    pub api_secret: String,
    /// User/client id (Zerodha: "AB1234", Angel One: client code)
    pub user_id: String,
    /// Login password (broker web password)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    /// Base32 TOTP seed for RFC 6238 codes
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub totp_seed: String,
    /// CDSL eDIS TPIN (delivery authorization)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tpin: String,
    /// OAuth2 redirect URI (Upstox)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub redirect_uri: String,
}

impl BrokerCredentials {
    /// A 6-digit TOTP for the standard broker authenticator configuration.
    pub fn totp_code_at(&self, unix_seconds: u64) -> Result<String, AuthError> {
        let totp = crate::totp::Totp::from_base32(&self.totp_seed)?;
        Ok(totp.code_at(unix_seconds))
    }
}

/// Pluggable credential storage.
///
/// Implementations must never log or serialize secrets outside their dedicated
/// storage boundaries. `save` on read-only stores returns [`AuthError::Config`].
#[async_trait::async_trait]
pub trait CredentialStore: Send + Sync {
    /// Load stored credentials; `Ok(None)` when nothing is stored.
    async fn load(&self, broker: BrokerName) -> Result<Option<BrokerCredentials>, AuthError>;

    /// Persist credentials (overwrites existing entry).
    async fn save(&self, creds: &BrokerCredentials) -> Result<(), AuthError>;

    /// Remove stored credentials.
    async fn delete(&self, broker: BrokerName) -> Result<(), AuthError>;
}

/// Environment-variable backed credential store.
///
/// Reads `HONBA_{BROKER}_API_KEY`, `HONBA_{BROKER}_API_SECRET`,
/// `HONBA_{BROKER}_USER_ID`, `HONBA_{BROKER}_PASSWORD`,
/// `HONBA_{BROKER}_TOTP_SEED`, `HONBA_{BROKER}_TPIN`, and
/// `HONBA_{BROKER}_REDIRECT_URI`.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnvCredentialStore;

impl EnvCredentialStore {
    fn read_env(prefix: &str, field: &str) -> Option<String> {
        let key = format!("{prefix}_{field}");
        std::env::var(&key).ok().filter(|v| !v.trim().is_empty())
    }
}

#[async_trait::async_trait]
impl CredentialStore for EnvCredentialStore {
    async fn load(&self, broker: BrokerName) -> Result<Option<BrokerCredentials>, AuthError> {
        let prefix = broker.env_prefix();
        let api_key = Self::read_env(&prefix, "API_KEY");
        if api_key.is_none() {
            // Nothing configured for this broker at all.
            return Ok(None);
        }
        Ok(Some(BrokerCredentials {
            broker: broker.as_str().to_string(),
            api_key: api_key.unwrap_or_default(),
            api_secret: Self::read_env(&prefix, "API_SECRET").unwrap_or_default(),
            user_id: Self::read_env(&prefix, "USER_ID").unwrap_or_default(),
            password: Self::read_env(&prefix, "PASSWORD").unwrap_or_default(),
            totp_seed: Self::read_env(&prefix, "TOTP_SEED").unwrap_or_default(),
            tpin: Self::read_env(&prefix, "TPIN").unwrap_or_default(),
            redirect_uri: Self::read_env(&prefix, "REDIRECT_URI").unwrap_or_default(),
        }))
    }

    /// Env stores are read-only: set environment variables instead.
    async fn save(&self, _creds: &BrokerCredentials) -> Result<(), AuthError> {
        Err(AuthError::Config(
            "env credential store is read-only; export HONBA_* variables instead".into(),
        ))
    }

    /// Env stores are read-only.
    async fn delete(&self, _broker: BrokerName) -> Result<(), AuthError> {
        Err(AuthError::Config(
            "env credential store is read-only; unset HONBA_* variables instead".into(),
        ))
    }
}

/// In-process memory store for deterministic tests and simulated sessions.
#[derive(Debug, Default)]
pub struct MemoryCredentialStore {
    entries: AsyncMutex<std::collections::HashMap<String, BrokerCredentials>>,
}

impl MemoryCredentialStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl CredentialStore for MemoryCredentialStore {
    async fn load(&self, broker: BrokerName) -> Result<Option<BrokerCredentials>, AuthError> {
        let entries = self.entries.lock().await;
        Ok(entries.get(broker.as_str()).cloned())
    }

    async fn save(&self, creds: &BrokerCredentials) -> Result<(), AuthError> {
        let broker = BrokerName::from_str(&creds.broker)?;
        let mut entries = self.entries.lock().await;
        entries.insert(broker.as_str().to_string(), creds.clone());
        Ok(())
    }

    async fn delete(&self, broker: BrokerName) -> Result<(), AuthError> {
        let mut entries = self.entries.lock().await;
        entries.remove(broker.as_str());
        Ok(())
    }
}

/// Native OS keyring credentials (macOS Keychain / Windows Credential Manager /
/// Linux secret-service), enabled behind the `keyring` cargo feature.
#[cfg(feature = "keyring")]
#[derive(Debug, Clone)]
pub struct KeyringCredentialStore {
    service: String,
}

#[cfg(feature = "keyring")]
impl KeyringCredentialStore {
    /// Store under service `honba-te-auth` / account `{broker}`.
    pub fn new() -> Self {
        Self {
            service: "honba-te-auth".to_string(),
        }
    }

    fn entry(&self, broker: BrokerName) -> Result<keyring::Entry, AuthError> {
        keyring::Entry::new(&self.service, broker.as_str())
            .map_err(|e| AuthError::Keyring(e.to_string()))
    }
}

#[cfg(feature = "keyring")]
impl Default for KeyringCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "keyring")]
#[async_trait::async_trait]
impl CredentialStore for KeyringCredentialStore {
    async fn load(&self, broker: BrokerName) -> Result<Option<BrokerCredentials>, AuthError> {
        let entry = self.entry(broker)?;
        match entry.get_password() {
            Ok(json) => serde_json::from_str(&json)
                .map(Some)
                .map_err(|e| AuthError::Keyring(format!("stored credential parse error: {e}"))),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(AuthError::Keyring(e.to_string())),
        }
    }

    async fn save(&self, creds: &BrokerCredentials) -> Result<(), AuthError> {
        let broker = BrokerName::from_str(&creds.broker)?;
        let json = serde_json::to_string(creds)
            .map_err(|e| AuthError::Keyring(format!("credential serialize error: {e}")))?;
        self.entry(broker)?
            .set_password(&json)
            .map_err(|e| AuthError::Keyring(e.to_string()))
    }

    async fn delete(&self, broker: BrokerName) -> Result<(), AuthError> {
        let entry = self.entry(broker)?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(AuthError::Keyring(e.to_string())),
        }
    }
}
