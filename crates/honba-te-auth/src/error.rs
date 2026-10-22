//! Authentication error taxonomy.

use thiserror::Error;

/// Errors surfaced by the headless authentication subsystem.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Invalid or incomplete configuration (missing endpoint, bad TTL...).
    #[error("configuration error: {0}")]
    Config(String),

    /// No stored credentials for the requested broker.
    #[error("credentials not found for broker '{0}'")]
    CredentialNotFound(String),

    /// Native OS keyring (keyring-rs) failure.
    #[error("keyring failure: {0}")]
    Keyring(String),

    /// TOTP computation / Base32 seed failure.
    #[error("totp error: {0}")]
    Totp(String),

    /// Hashing or encoding failure.
    #[error("crypto error: {0}")]
    Crypto(String),

    /// Network / transport failure against the broker auth endpoints.
    #[error("network error: {0}")]
    Network(String),

    /// Broker answered but the login protocol did not proceed.
    #[error("broker '{broker}' login protocol error: {detail}")]
    Protocol {
        /// Broker adapter name
        broker: String,
        /// Error detail
        detail: String,
    },

    /// Stored session has expired and must be refreshed.
    #[error("session expired for broker '{0}'")]
    SessionExpired(String),

    /// CDSL eDIS requires a TPIN that is not configured.
    #[error("missing CDSL TPIN for broker '{0}'")]
    MissingTpin(String),

    /// eDIS pipeline failure.
    #[error("edis error: {0}")]
    Edis(String),
}
