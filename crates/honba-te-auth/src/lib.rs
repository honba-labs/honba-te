//! # 本場・真 honba-te-auth
//!
//! Headless authentication for the 本場・手 (honba-te) execution subsystem:
//! RFC 6238 TOTP (zero-dependency), secure credential management, automated
//! broker session login, token caching with 08:30 IST pre-market refresh, and
//! CDSL eDIS demat-delivery pre-authorization.
//!
//! # Module map
//! - [`totp`] — RFC 6238 TOTP over Base32 seeds (SHA-1 / SHA-256)
//! - [`crypto`] — hand-rolled SHA-1/SHA-256/HMAC/Base32/Base64/hex
//! - [`credentials`] — [`BrokerCredentials`] + env/memory/keyring stores
//! - [`login`] — headless login flows (Zerodha, Upstox, Angel One)
//! - [`tokens`] — [`SessionTokens`], cache, IST scheduler
//! - [`edis`] — CDSL TPIN batch pre-authorization

pub mod credentials;
pub mod crypto;
pub mod edis;
pub mod error;
pub mod login;
pub mod tokens;
pub mod totp;

pub use credentials::{
    BrokerCredentials, BrokerName, CredentialStore, EnvCredentialStore, MemoryCredentialStore,
};
pub use edis::{
    CdslGateway, EdisEntry, EdisOutcome, EdisPreAuthorizer, EdisRequest, EdisStatus,
    HttpCdslGateway,
};
pub use error::AuthError;
pub use login::{
    AngelOneLoginConfig, AngelOneSmartLogin, AuthHttpClient, AuthHttpRequest, HttpReply, LoginFlow,
    ReqwestAuthHttpClient, UpstoxLoginConfig, UpstoxOauthLogin, ZerodhaHeadlessLogin,
    ZerodhaLoginConfig, extract_code_from_redirect,
};
pub use tokens::{
    DailyRefreshScheduler, IST_OFFSET_SECS, SessionManager, SessionTokens, TokenCache, next_ist_run,
};
pub use totp::{Totp, TotpAlgorithm, TotpConfig, base32_decode, base32_encode};

#[cfg(feature = "keyring")]
pub use credentials::KeyringCredentialStore;
