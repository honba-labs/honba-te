//! Session token caching, expiry accounting, and the zero-touch daily
//! pre-market refresh scheduler (08:30 IST default).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, NaiveTime, TimeZone, Utc};
use tokio::sync::Mutex;

use crate::credentials::{BrokerName, CredentialStore};
use crate::error::AuthError;
use crate::login::LoginFlow;

/// Broker session tokens with expiry accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTokens {
    /// Bearer/API access token for REST + stream calls.
    pub access_token: String,
    /// Refresh token where the broker issues one.
    pub refresh_token: Option<String>,
    /// Market-data feed token (Angel One).
    pub feed_token: Option<String>,
    /// When the tokens were minted.
    pub issued_at: DateTime<Utc>,
    /// When the access token expires.
    pub expires_at: DateTime<Utc>,
}

impl SessionTokens {
    /// TTL until expiry (negative when already expired).
    pub fn ttl(&self, now: DateTime<Utc>) -> ChronoDuration {
        self.expires_at - now
    }

    /// True when the token is expired at `now`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// True when usable but will expire within `buffer` — triggers proactive
    /// refresh for the pre-market window.
    pub fn expires_within(&self, now: DateTime<Utc>, buffer: ChronoDuration) -> bool {
        self.expires_at - now <= buffer
    }
}

/// In-memory session cache organized per broker.
#[derive(Debug, Default)]
pub struct TokenCache {
    entries: Mutex<HashMap<String, SessionTokens>>,
}

impl TokenCache {
    /// Empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store tokens for a broker.
    pub async fn store(&self, broker: BrokerName, tokens: SessionTokens) {
        self.entries
            .lock()
            .await
            .insert(broker.as_str().to_string(), tokens);
    }

    /// Latest stored tokens (regardless of validity) for observability.
    pub async fn peek(&self, broker: BrokerName) -> Option<SessionTokens> {
        self.entries.lock().await.get(broker.as_str()).cloned()
    }

    /// Latest stored tokens that are still valid at `now`.
    pub async fn valid(&self, broker: BrokerName, now: DateTime<Utc>) -> Option<SessionTokens> {
        let tokens = self.peek(broker).await?;
        if tokens.is_expired(now) {
            None
        } else {
            Some(tokens)
        }
    }

    /// Invalidate a broker's cached session.
    pub async fn invalidate(&self, broker: BrokerName) {
        self.entries.lock().await.remove(broker.as_str());
    }
}

/// IST is a fixed +05:30 offset (no DST) — pre-market automation runs at a
/// constant wall-clock time.
pub const IST_OFFSET_SECS: i64 = 5 * 3600 + 30 * 60;

/// The next `HH:MM` IST wall-clock instant strictly after `now`, expressed in UTC.
///
/// Used for the 08:30 IST pre-market bootstrap and for broker token expiry
/// anchors (e.g. SmartAPI 03:30 IST roll-off).
pub fn next_ist_run(now: DateTime<Utc>, hour_ist: u32, minute_ist: u32) -> DateTime<Utc> {
    let target_ist = NaiveTime::from_hms_opt(hour_ist.clamp(0, 23), minute_ist.clamp(0, 59), 0)
        .unwrap_or_default();
    // Shift into IST frame, locate the candidate wall-clock slot on today's
    // IST date, then convert back to UTC.
    let now_ist = now + ChronoDuration::seconds(IST_OFFSET_SECS);
    let candidate_ist = now_ist
        .date_naive()
        .and_time(target_ist)
        .and_local_timezone(Utc)
        .single()
        .unwrap_or_else(|| {
            // Naive datetime arithmetic modulo tzdb edge cases (IST has none).
            Utc.from_utc_datetime(&now_ist.date_naive().and_time(target_ist))
        });
    let candidate_utc = candidate_ist - ChronoDuration::seconds(IST_OFFSET_SECS);
    if candidate_utc > now {
        candidate_utc
    } else {
        candidate_utc + ChronoDuration::days(1)
    }
}

/// Hook executed when a stored session must be (re-)minted: performs the
/// broker's headless login and returns fresh tokens.
#[async_trait]
pub trait SessionRefresher: Send + Sync {
    /// Refresh tokens for `broker` using stored credentials.
    async fn refresh_session(
        &self,
        creds: &crate::credentials::BrokerCredentials,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError>;
}

/// Session facade wiring credentials, login flows, and the token cache.
///
/// `session(broker)` returns a cached, valid session or performs the full
/// headless login (persisting refreshed tokens back through the cache).
pub struct SessionManager {
    store: Arc<dyn CredentialStore>,
    flows: HashMap<String, Arc<dyn LoginFlow>>,
    cache: Arc<TokenCache>,
    /// Proactive refresh buffer (default 30 minutes).
    refresh_buffer: ChronoDuration,
}

impl SessionManager {
    /// Create an empty manager around a credential store and shared cache.
    pub fn new(store: Arc<dyn CredentialStore>, cache: Arc<TokenCache>) -> Self {
        Self {
            store,
            flows: HashMap::new(),
            cache,
            refresh_buffer: ChronoDuration::minutes(30),
        }
    }

    /// Register a broker login flow.
    pub fn with_flow(mut self, broker: BrokerName, flow: Arc<dyn LoginFlow>) -> Self {
        self.flows.insert(broker.as_str().to_string(), flow);
        self
    }

    /// Override the proactive refresh buffer.
    pub fn with_refresh_buffer(mut self, buffer: ChronoDuration) -> Self {
        self.refresh_buffer = buffer;
        self
    }

    /// Shared cache handle.
    pub fn cache(&self) -> &Arc<TokenCache> {
        &self.cache
    }

    /// Registered flow for a broker, if any.
    pub fn flow(&self, broker: BrokerName) -> Option<&Arc<dyn LoginFlow>> {
        self.flows.get(broker.as_str())
    }

    /// Obtain a valid session: cache first, headless login second.
    pub async fn session(
        &self,
        broker: BrokerName,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError> {
        if let Some(tokens) = self.cache.valid(broker, now).await {
            return Ok(tokens);
        }

        let flow = self
            .flows
            .get(broker.as_str())
            .ok_or_else(|| {
                AuthError::Config(format!(
                    "no login flow registered for broker '{}'",
                    broker.as_str()
                ))
            })?
            .clone();

        let creds = self
            .store
            .load(broker)
            .await?
            .ok_or_else(|| AuthError::CredentialNotFound(broker.as_str().into()))?;

        let tokens = flow.login(&creds, now).await?;
        self.cache.store(broker, tokens.clone()).await;
        Ok(tokens)
    }

    /// Force a fresh login even if a cached session exists.
    pub async fn force_refresh(
        &self,
        broker: BrokerName,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError> {
        self.cache.invalidate(broker).await;
        self.session(broker, now).await
    }
}

/// Clock accessor for deterministic scheduling in tests.
pub type ClockFn = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Daily pre-market scheduler: sleeps until the next `HH:MM` IST run, then
/// refreshes every registered broker; repeats forever.
pub struct DailyRefreshScheduler {
    manager: Arc<SessionManager>,
    hour_ist: u32,
    minute_ist: u32,
    clock: ClockFn,
}

impl DailyRefreshScheduler {
    /// Scheduler at (hour, minute) IST using the system clock.
    pub fn new(manager: Arc<SessionManager>, hour_ist: u32, minute_ist: u32) -> Self {
        Self {
            manager,
            hour_ist,
            minute_ist,
            clock: Arc::new(Utc::now),
        }
    }

    /// Scheduler with an injected clock (tests).
    pub fn with_clock(
        manager: Arc<SessionManager>,
        hour_ist: u32,
        minute_ist: u32,
        clock: ClockFn,
    ) -> Self {
        Self {
            manager,
            hour_ist,
            minute_ist,
            clock,
        }
    }

    /// One scheduler beat: sleep until the next IST run (relative to `now`),
    /// refresh all registered flows, and return the next wake instant.
    pub async fn tick(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, AuthError> {
        let next = next_ist_run(now, self.hour_ist, self.minute_ist);
        let delay = (next - now).to_std().unwrap_or(Duration::ZERO);
        tokio::time::sleep(delay).await;

        for broker in [
            BrokerName::Zerodha,
            BrokerName::Upstox,
            BrokerName::AngelOne,
        ] {
            // Only refresh brokers that have registered flows & credentials.
            if self.manager.flow(broker).is_none() {
                continue;
            }
            match self.manager.force_refresh(broker, Utc::now()).await {
                Ok(tokens) => tracing::info!(
                    "morning session refreshed for {}: expires_at={}",
                    broker.as_str(),
                    tokens.expires_at
                ),
                Err(e) => tracing::error!(
                    "morning session refresh FAILED for {}: {e}",
                    broker.as_str()
                ),
            }
        }
        Ok(next)
    }

    /// Run beats forever (spawns as a background JoinHandle via [`Self::spawn`]).
    pub async fn run_forever(self) {
        loop {
            let now = (self.clock)();
            let _ = self.tick(now).await;
        }
    }

    /// Spawn the forever loop on the current tokio runtime.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run_forever())
    }
}
