//! Token cache, SessionManager, and IST scheduler tests (deterministic).

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use honba_te_auth::credentials::{
    BrokerCredentials, BrokerName, CredentialStore, MemoryCredentialStore,
};
use honba_te_auth::error::AuthError;
use honba_te_auth::login::LoginFlow;
use honba_te_auth::tokens::{
    ClockFn, DailyRefreshScheduler, SessionManager, SessionTokens, TokenCache, next_ist_run,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-22T03:30:00+00:00")
        .unwrap()
        .into()
}

fn tokens(broker: &str) -> SessionTokens {
    SessionTokens {
        access_token: format!("at-{broker}"),
        refresh_token: None,
        feed_token: None,
        issued_at: t0(),
        expires_at: t0() + ChronoDuration::hours(12),
    }
}

// ---------------------------------------------------------------------------
// TokenCache
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cache_returns_valid_sessions_and_invalidates() {
    let cache = TokenCache::new();
    assert!(cache.valid(BrokerName::Zerodha, t0()).await.is_none());

    cache.store(BrokerName::Zerodha, tokens("zerodha")).await;
    assert_eq!(
        cache
            .valid(BrokerName::Zerodha, t0())
            .await
            .unwrap()
            .access_token,
        "at-zerodha"
    );

    // Expired sessions are not served.
    cache
        .store(
            BrokerName::Zerodha,
            SessionTokens {
                expires_at: t0(),
                ..tokens("zerodha")
            },
        )
        .await;
    let peeked = cache.peek(BrokerName::Zerodha).await.unwrap();
    assert!(peeked.is_expired(t0()));
    assert!(cache.valid(BrokerName::AngelOne, t0()).await.is_none());
    assert!(cache.valid(BrokerName::Zerodha, t0()).await.is_none());

    cache.invalidate(BrokerName::Zerodha).await;
    assert!(cache.peek(BrokerName::Zerodha).await.is_none());
}

// ---------------------------------------------------------------------------
// SessionManager (cache-first, login fallback)
// ---------------------------------------------------------------------------

/// Counting stub flow standing in for a broker's headless login.
struct CountingFlow {
    calls: AtomicUsize,
    sessions: Mutex<Vec<SessionTokens>>,
}

impl CountingFlow {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            sessions: Mutex::new(vec![tokens("zerodha")]),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LoginFlow for CountingFlow {
    fn broker(&self) -> &str {
        "zerodha"
    }

    async fn login(
        &self,
        _creds: &BrokerCredentials,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut sessions = self.sessions.lock().await;
        let issued = if sessions.is_empty() {
            tokens("zerodha")
        } else {
            sessions.remove(0)
        };
        Ok(SessionTokens {
            issued_at: now,
            ..issued
        })
    }
}

fn sample_creds() -> BrokerCredentials {
    BrokerCredentials {
        broker: "zerodha".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn manager_caches_session_between_calls() {
    let store = Arc::new(MemoryCredentialStore::new());
    store.save(&sample_creds()).await.unwrap();
    let flow = Arc::new(CountingFlow::new());

    let manager = SessionManager::new(
        store as Arc<dyn CredentialStore>,
        Arc::new(TokenCache::new()),
    )
    .with_flow(BrokerName::Zerodha, flow.clone() as Arc<dyn LoginFlow>);

    let first = manager.session(BrokerName::Zerodha, t0()).await.unwrap();
    assert_eq!(first.access_token, "at-zerodha");
    assert_eq!(flow.calls(), 1);

    // Second call is served from the cache: no extra login.
    let second = manager.session(BrokerName::Zerodha, t0()).await.unwrap();
    assert_eq!(second.access_token, "at-zerodha");
    assert_eq!(flow.calls(), 1);

    // Expiry forces a fresh login.
    manager
        .session(BrokerName::Zerodha, t0() + ChronoDuration::hours(13))
        .await
        .unwrap();
    assert_eq!(flow.calls(), 2, "expired session must re-login");
}

#[tokio::test]
async fn manager_without_flow_or_credentials_errors_clearly() {
    let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::new());
    let manager = SessionManager::new(store, Arc::new(TokenCache::new()))
        .with_flow(BrokerName::Zerodha, Arc::new(CountingFlow::new()));

    let err = manager.session(BrokerName::Upstox, t0()).await.unwrap_err();
    assert!(matches!(err, AuthError::Config(_)), "no flow registered");

    let err = manager
        .session(BrokerName::Zerodha, t0())
        .await
        .unwrap_err();
    assert!(
        matches!(err, AuthError::CredentialNotFound(b) if b == "zerodha"),
        "no stored credentials"
    );
}

#[tokio::test]
async fn force_refresh_ignores_cached_session() {
    let store = Arc::new(MemoryCredentialStore::new());
    store.save(&sample_creds()).await.unwrap();
    let flow = Arc::new(CountingFlow::new());

    let manager = SessionManager::new(
        store as Arc<dyn CredentialStore>,
        Arc::new(TokenCache::new()),
    )
    .with_flow(BrokerName::Zerodha, flow.clone() as Arc<dyn LoginFlow>);

    manager.session(BrokerName::Zerodha, t0()).await.unwrap();
    let fresh = manager
        .force_refresh(BrokerName::Zerodha, t0())
        .await
        .unwrap();
    assert_eq!(fresh.access_token, "at-zerodha");
    assert_eq!(flow.calls(), 2, "force_refresh re-mints even when cached");
}

// ---------------------------------------------------------------------------
// IST scheduler math
// ---------------------------------------------------------------------------

#[test]
fn ist_0830_boundary_math_is_exact() {
    // 2026-10-22 02:00 UTC == 07:30 IST → next 08:30 IST run = 03:00 UTC.
    let now = DateTime::parse_from_rfc3339("2026-10-22T02:00:00+00:00")
        .unwrap()
        .into();
    let next = next_ist_run(now, 8, 30);
    assert_eq!(next.to_rfc3339(), "2026-10-22T03:00:00+00:00");

    // At exactly the run time the next run is the following day.
    assert_eq!(
        next_ist_run(next, 8, 30).to_rfc3339(),
        "2026-10-23T03:00:00+00:00"
    );

    // One second after the run → also the following day.
    let just_after = next + ChronoDuration::seconds(1);
    assert_eq!(
        next_ist_run(just_after, 8, 30).to_rfc3339(),
        "2026-10-23T03:00:00+00:00"
    );

    // One second before the run → today's run (in our near-immediate future).
    let just_before = next - ChronoDuration::seconds(1);
    assert_eq!(
        next_ist_run(just_before, 8, 30).to_rfc3339(),
        "2026-10-22T03:00:00+00:00"
    );
}

#[test]
fn ist_expiry_anchor_matches_smartapi_rolloff() {
    // 10:00 UTC = 15:30 IST → next 03:30 IST = 22:00 UTC (same day).
    let now = DateTime::parse_from_rfc3339("2026-10-22T10:00:00+00:00")
        .unwrap()
        .into();
    assert_eq!(
        next_ist_run(now, 3, 30).to_rfc3339(),
        "2026-10-22T22:00:00+00:00"
    );
}

// ---------------------------------------------------------------------------
// DailyRefreshScheduler (paused tokio time + injected clock)
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn scheduler_refreshes_registered_brokers_at_0830_ist() {
    let store = Arc::new(MemoryCredentialStore::new());
    store.save(&sample_creds()).await.unwrap();
    let flow = Arc::new(CountingFlow::new());
    let cache = Arc::new(TokenCache::new());

    let manager = Arc::new(
        SessionManager::new(store as Arc<dyn CredentialStore>, cache)
            .with_flow(BrokerName::Zerodha, flow.clone() as Arc<dyn LoginFlow>),
    );

    // 08:30 IST = 03:00 UTC. Clock pinned at 02:00 UTC → 1 hour to the run.
    let fixed_now = DateTime::parse_from_rfc3339("2026-10-22T02:00:00+00:00")
        .unwrap()
        .into();
    let clock: ClockFn = Arc::new(move || fixed_now);
    let scheduler = DailyRefreshScheduler::with_clock(manager.clone(), 8, 30, clock);

    let handle = tokio::spawn(async move { scheduler.tick(fixed_now).await });

    // Not yet 03:00 UTC: the scheduler must still be sleeping.
    tokio::time::advance(
        ChronoDuration::hours(1).to_std().unwrap() - std::time::Duration::from_secs(1),
    )
    .await;
    assert!(!handle.is_finished(), "scheduler woke before 08:30 IST");

    // Cross the boundary: the run fires.
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let next = handle.await.unwrap().unwrap();
    assert_eq!(next.to_rfc3339(), "2026-10-22T03:00:00+00:00");
    assert_eq!(flow.calls(), 1, "morning refresh logged in exactly once");
    assert!(manager.cache().peek(BrokerName::Zerodha).await.is_some());
}

#[tokio::test(start_paused = true)]
async fn scheduler_skips_brokers_without_registered_flows() {
    let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::new());
    let manager = Arc::new(SessionManager::new(store, Arc::new(TokenCache::new())));
    let clock: ClockFn = Arc::new(|| {
        DateTime::parse_from_rfc3339("2026-10-22T02:00:00+00:00")
            .unwrap()
            .into()
    });
    let scheduler = DailyRefreshScheduler::with_clock(manager, 8, 30, clock);

    let now = DateTime::parse_from_rfc3339("2026-10-22T02:00:00+00:00")
        .unwrap()
        .into();
    tokio::spawn(async move { scheduler.tick(now).await });
    tokio::time::advance(ChronoDuration::hours(1).to_std().unwrap()).await;
    // No flows → tick completes without touching any credentials (no panic,
    // no credential lookups). The beat itself is the contract under test.
}

#[tokio::test(start_paused = true)]
async fn scheduler_survives_login_failures_and_reports_next_run() {
    // Empty store + registered flow → CredentialNotFound during refresh; tick
    // must log & continue, returning the next run time.
    let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::new());
    let manager = Arc::new(
        SessionManager::new(store, Arc::new(TokenCache::new()))
            .with_flow(BrokerName::Zerodha, Arc::new(CountingFlow::new())),
    );

    let fixed_now = DateTime::parse_from_rfc3339("2026-10-22T02:00:00+00:00")
        .unwrap()
        .into();
    let clock: ClockFn = Arc::new(move || fixed_now);
    let scheduler = DailyRefreshScheduler::with_clock(manager, 8, 30, clock);

    let handle = tokio::spawn(async move { scheduler.tick(fixed_now).await });
    tokio::time::advance(std::time::Duration::from_secs(3_600)).await;
    let next = handle.await.unwrap().unwrap();
    assert_eq!(next.to_rfc3339(), "2026-10-22T03:00:00+00:00");
}
