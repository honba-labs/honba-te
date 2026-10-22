//! Credential store behavior tests (env / memory round-trips, broker names).

use honba_te_auth::credentials::{
    BrokerCredentials, BrokerName, CredentialStore, EnvCredentialStore, MemoryCredentialStore,
};
use honba_te_auth::error::AuthError;
use std::str::FromStr;
use std::sync::OnceLock;

/// Environment tests mutate process globals; serialize them via an async lock
/// (holding the guard across the store's await points stays sound).
fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Set an env var while holding the env mutex.
fn set_var(key: &str, value: &str) {
    // SAFETY: serialized behind `env_lock`, so no other thread reads concurrently.
    unsafe { std::env::set_var(key, value) };
}

/// Remove an env var while holding the env mutex.
fn remove_var(key: &str) {
    // SAFETY: serialized behind `env_lock`, so no other thread reads concurrently.
    unsafe { std::env::remove_var(key) };
}

fn sample_creds(broker: &str) -> BrokerCredentials {
    BrokerCredentials {
        broker: broker.to_string(),
        api_key: "api-key-1".into(),
        api_secret: "api-secret-2".into(),
        user_id: "AB1234".into(),
        password: "hunter2".into(),
        tpin: "070164".into(),
        totp_seed: "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ".into(),
        redirect_uri: "https://example.local/cb".into(),
    }
}

// ---------------------------------------------------------------------------
// BrokerName
// ---------------------------------------------------------------------------

#[test]
fn broker_names_parse_canonically() {
    assert_eq!(
        BrokerName::from_str("zerodha").unwrap(),
        BrokerName::Zerodha
    );
    assert_eq!(BrokerName::from_str("Kite").unwrap(), BrokerName::Zerodha);
    assert_eq!(BrokerName::from_str("upstox").unwrap(), BrokerName::Upstox);
    assert_eq!(
        BrokerName::from_str("SmartAPI").unwrap(),
        BrokerName::AngelOne
    );
    assert!(BrokerName::from_str("fyers").is_err());
    assert_eq!(BrokerName::Zerodha.as_str(), "zerodha");
}

// ---------------------------------------------------------------------------
// EnvCredentialStore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn env_store_loads_full_credential_set() {
    let _g = env_lock().lock().await;
    set_var("HONBA_ZERODHA_API_KEY", "kite-key-771");
    set_var("HONBA_ZERODHA_API_SECRET", "kite-secret-771");
    set_var("HONBA_ZERODHA_USER_ID", "ZQ5551");
    set_var("HONBA_ZERODHA_PASSWORD", "pw");
    set_var("HONBA_ZERODHA_TOTP_SEED", "GEZDGNBVGY3TQOJQ");
    set_var("HONBA_ZERODHA_TPIN", "070164");

    let creds = EnvCredentialStore
        .load(BrokerName::Zerodha)
        .await
        .unwrap()
        .expect("credentials configured");
    assert_eq!(creds.api_key, "kite-key-771");
    assert_eq!(creds.user_id, "ZQ5551");
    assert_eq!(creds.totp_seed, "GEZDGNBVGY3TQOJQ");
    assert_eq!(creds.tpin, "070164");

    remove_var("HONBA_ZERODHA_API_KEY");
    remove_var("HONBA_ZERODHA_API_SECRET");
    remove_var("HONBA_ZERODHA_USER_ID");
    remove_var("HONBA_ZERODHA_PASSWORD");
    remove_var("HONBA_ZERODHA_TOTP_SEED");
    remove_var("HONBA_ZERODHA_TPIN");
}

#[tokio::test]
async fn env_store_returns_none_when_unset() {
    let _g = env_lock().lock().await;
    remove_var("HONBA_UPSTOX_API_KEY");
    assert!(
        EnvCredentialStore
            .load(BrokerName::Upstox)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn env_store_ignore_blank_values() {
    let _g = env_lock().lock().await;
    set_var("HONBA_ANGELONE_API_KEY", "   ");
    assert!(
        EnvCredentialStore
            .load(BrokerName::AngelOne)
            .await
            .unwrap()
            .is_none()
    );
    remove_var("HONBA_ANGELONE_API_KEY");
}

#[tokio::test]
async fn env_store_is_read_only() {
    let err = EnvCredentialStore
        .save(&sample_creds("zerodha"))
        .await
        .unwrap_err();
    assert!(matches!(err, AuthError::Config(_)));
    let err = EnvCredentialStore
        .delete(BrokerName::Zerodha)
        .await
        .unwrap_err();
    assert!(matches!(err, AuthError::Config(_)));
}

// ---------------------------------------------------------------------------
// MemoryCredentialStore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_store_round_trips_and_deletes() {
    let store = MemoryCredentialStore::new();
    assert!(store.load(BrokerName::Zerodha).await.unwrap().is_none());

    store.save(&sample_creds("zerodha")).await.unwrap();
    let loaded = store.load(BrokerName::Zerodha).await.unwrap().unwrap();
    assert_eq!(loaded, sample_creds("zerodha"));

    store.delete(BrokerName::Zerodha).await.unwrap();
    assert!(store.load(BrokerName::Zerodha).await.unwrap().is_none());
}

#[tokio::test]
async fn memory_store_rejects_unknown_broker_labels() {
    let store = MemoryCredentialStore::new();
    let err = store.save(&sample_creds("fyers")).await.unwrap_err();
    assert!(matches!(err, AuthError::Config(_)));
}

// ---------------------------------------------------------------------------
// Credential TOTP delegate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn broker_credentials_delegate_totp_to_engine() {
    let creds = sample_creds("zerodha");
    let code = creds.totp_code_at(59).unwrap();
    // "12345678901234567890" SHA-1 TOTP at T=59 is 94287082 (8 digits) →
    // the 6-digit standard truncation is 287082.
    assert_eq!(code, "287082");
}

#[tokio::test]
async fn broker_totp_without_seed_fails_cleanly() {
    let creds = BrokerCredentials {
        broker: "zerodha".into(),
        ..Default::default()
    };
    let err = creds.totp_code_at(59).unwrap_err();
    assert!(matches!(err, AuthError::Totp(_)));
}
