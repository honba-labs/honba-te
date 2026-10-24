//! Token-bucket rate limiter with strict priority queue jumping.
//!
//! Shields every broker adapter from HTTP 429 bans by matching the exact
//! per-broker API limits (Zerodha 3 orders/s & 10 general/s, Upstox 25/s,
//! Angel One 10/s) while guaranteeing queue priority:
//!
//! | Priority | Class |
//! |----------|-------|
//! | [`RequestPriority::Immediate`] | Emergency kill-switch cancellations and manual panic halts |
//! | [`RequestPriority::High`] | Stop-loss trigger adjustments and limit order cancellations |
//! | [`RequestPriority::Normal`] | Passive order placements and modifications |
//! | [`RequestPriority::Low`] | Background order-book, position and margin polling |
//!
//! # Correctness invariants
//! 1. A waiter is granted only by [`TokenBucketRateLimiter::dispatch`], which
//!    runs under the bucket lock, atomically deducting tokens before marking
//!    the waiter granted and **popping it from its queue**.
//! 2. Therefore a queue contains only waiters that have NOT been granted.
//!    A waiter checks its own shared grant flag to learn its fate — no scans.
//! 3. Token accounting is exact [`rust_decimal::Decimal`] math. No floats, so
//!    grant cadence is deterministic across architectures.

use async_trait::async_trait;
use honba_core::{OrderAck, OrderRequest, OrderStatus, Price, Quantity};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use tracing::warn;

use crate::error::BrokerError;
use crate::executor::BrokerExecutor;
use crate::types::{BrokerHealth, BrokerMargins, BrokerPosition, OrderSnapshot};

/// Request urgency class controlling queue ordering in [`TokenBucketRateLimiter`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RequestPriority {
    /// Emergency kill-switch cancellations and manual panic halts (jumps every queue).
    Immediate,
    /// Stop-loss trigger adjustments and limit order cancellations.
    High,
    /// Passive order placements and modifications.
    Normal,
    /// Background order-book, position and margin polling.
    Low,
}

impl RequestPriority {
    /// Numeric level; lower value = higher urgency.
    pub fn level(self) -> u8 {
        match self {
            Self::Immediate => 0,
            Self::High => 1,
            Self::Normal => 2,
            Self::Low => 3,
        }
    }

    /// Label string for Prometheus metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::High => "high",
            Self::Normal => "normal",
            Self::Low => "low",
        }
    }
}

fn priority_level_str(level: u8) -> &'static str {
    match level {
        0 => "immediate",
        1 => "high",
        2 => "normal",
        _ => "low",
    }
}

/// Token bucket configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RateLimiterConfig {
    /// Maximum burst size (concurrently stored tokens).
    pub capacity_tokens: Decimal,
    /// Steady-state refill in tokens per second.
    pub refill_rate_per_sec: Decimal,
    /// Token balance after construction.
    pub initial_tokens: Decimal,
}

impl RateLimiterConfig {
    /// Validate configuration invariants (positive capacity, sane bounds).
    pub fn validated(self) -> Result<Self, BrokerError> {
        if self.capacity_tokens <= Decimal::ZERO {
            return Err(BrokerError::InvalidRequest(
                "rate limiter capacity must be positive".into(),
            ));
        }
        if self.refill_rate_per_sec < Decimal::ZERO {
            return Err(BrokerError::InvalidRequest(
                "rate limiter refill rate must be non-negative".into(),
            ));
        }
        if self.initial_tokens < Decimal::ZERO || self.initial_tokens > self.capacity_tokens {
            return Err(BrokerError::InvalidRequest(
                "rate limiter initial tokens must be within [0, capacity]".into(),
            ));
        }
        Ok(self)
    }
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        let ten = Decimal::from(10);
        Self {
            capacity_tokens: ten,
            refill_rate_per_sec: ten,
            initial_tokens: ten,
        }
    }
}

/// Preset per-broker limits reflecting documented Indian broker API constraints.
#[derive(Debug, Clone, Copy)]
pub struct BrokerRateCatalog;

impl BrokerRateCatalog {
    /// Zerodha Kite order mutations: 3 requests per second.
    pub fn zerodha_orders() -> RateLimiterConfig {
        let three = Decimal::from(3);
        RateLimiterConfig {
            capacity_tokens: three,
            refill_rate_per_sec: three,
            initial_tokens: three,
        }
    }

    /// Zerodha Kite general (non-order) API: 10 requests per second.
    pub fn zerodha_general() -> RateLimiterConfig {
        let ten = Decimal::from(10);
        RateLimiterConfig {
            capacity_tokens: ten,
            refill_rate_per_sec: ten,
            initial_tokens: ten,
        }
    }

    /// Upstox v2 API: 25 requests per second.
    pub fn upstox() -> RateLimiterConfig {
        let twenty_five = Decimal::from(25);
        RateLimiterConfig {
            capacity_tokens: twenty_five,
            refill_rate_per_sec: twenty_five,
            initial_tokens: twenty_five,
        }
    }

    /// Angel One SmartAPI: 10 requests per second.
    pub fn angelone() -> RateLimiterConfig {
        let ten = Decimal::from(10);
        RateLimiterConfig {
            capacity_tokens: ten,
            refill_rate_per_sec: ten,
            initial_tokens: ten,
        }
    }

    /// Preset for a broker adapter name (default limits for unknown brokers).
    pub fn for_broker(name: &str) -> RateLimiterConfig {
        match name {
            "zerodha" => Self::zerodha_orders(),
            "upstox" => Self::upstox(),
            "angelone" => Self::angelone(),
            _ => RateLimiterConfig::default(),
        }
    }
}

/// A queued acquisition attempt. Shared: the parking task checks `granted`
/// to learn its fate without holding or scanning the bucket lock.
struct WaiterCore {
    cost: Decimal,
    granted: AtomicBool,
    ticket: Arc<Notify>,
}

struct BucketState {
    config: RateLimiterConfig,
    tokens: Decimal,
    last_update: Instant,
    /// level (lower = higher urgency) -> FIFO of non-granted waiters
    waiters: BTreeMap<u8, VecDeque<Arc<WaiterCore>>>,
}

impl BucketState {
    /// Time until the bucket can afford `cost` tokens.
    fn time_to_afford(&self, cost: Decimal) -> Duration {
        if cost <= self.tokens {
            return Duration::ZERO;
        }
        let rate = self.config.refill_rate_per_sec;
        if rate <= Decimal::ZERO {
            // Refills disabled: park for the capped interval; deadline handles it.
            return Duration::from_secs(1);
        }
        seconds_to_duration((cost - self.tokens) / rate)
    }
}

fn seconds_to_duration(secs: Decimal) -> Duration {
    let rounded = secs.round_dp_with_strategy(9, RoundingStrategy::MidpointAwayFromZero);
    let whole = rounded.trunc().to_u64().unwrap_or(u64::MAX).min(86_400);
    let nanos = rounded
        .fract()
        .saturating_mul(Decimal::from(1_000_000_000u64))
        .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_u64()
        .unwrap_or(0)
        .min(999_999_999);
    Duration::new(whole, nanos as u32)
}

/// Adaptive token-bucket rate limiter with strict-priority wait queues.
///
/// A newly arriving [`RequestPriority::Immediate`] requester consumes tokens
/// ahead of ALL queued lower-priority waiters whenever the fast path can fire
/// (tokens available, no strictly higher waiter) — kill-switch commands do not
/// sit behind passive order-flow queues.
pub struct TokenBucketRateLimiter {
    broker: String,
    state: Mutex<BucketState>,
}

impl std::fmt::Debug for TokenBucketRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenBucketRateLimiter")
            .field("broker", &self.broker)
            .finish()
    }
}

impl TokenBucketRateLimiter {
    /// Create a validated rate limiter.
    pub fn new(config: RateLimiterConfig) -> Result<Self, BrokerError> {
        let config = config.validated()?;
        let tokens = config.initial_tokens;
        Ok(Self {
            broker: "generic".into(),
            state: Mutex::new(BucketState {
                config,
                tokens,
                last_update: Instant::now(),
                waiters: BTreeMap::new(),
            }),
        })
    }

    /// Set broker identifier for telemetry metrics.
    pub fn with_broker(mut self, broker: impl Into<String>) -> Self {
        self.broker = broker.into();
        self
    }

    /// Broker identifier for this limiter.
    pub fn broker(&self) -> &str {
        &self.broker
    }

    /// Create a limiter from the documented preset for the broker name.
    pub fn for_broker(name: &str) -> Result<Self, BrokerError> {
        Ok(Self::new(BrokerRateCatalog::for_broker(name))?.with_broker(name))
    }

    /// Adaptively reconfigure capacity & refill rate, clamping the balance.
    pub async fn set_limits(
        &self,
        capacity_tokens: Decimal,
        refill_rate_per_sec: Decimal,
    ) -> Result<(), BrokerError> {
        let config = RateLimiterConfig {
            capacity_tokens,
            refill_rate_per_sec,
            initial_tokens: capacity_tokens,
        }
        .validated()?;
        let mut state = self.state.lock().await;
        state.config = RateLimiterConfig {
            capacity_tokens: config.capacity_tokens,
            refill_rate_per_sec: config.refill_rate_per_sec,
            initial_tokens: config.initial_tokens,
        };
        state.last_update = Instant::now();
        state.tokens = state.tokens.min(config.capacity_tokens);
        Ok(())
    }

    /// Current token balance (observability/tests).
    pub async fn available_tokens(&self) -> Decimal {
        let mut state = self.state.lock().await;
        Self::refill(&mut state, Instant::now());
        state.tokens
    }

    /// Number of waiters queued per priority level (observability/tests).
    pub async fn queued_counts(&self) -> BTreeMap<u8, usize> {
        let state = self.state.lock().await;
        state
            .waiters
            .iter()
            .map(|(level, q)| (*level, q.len()))
            .collect()
    }

    /// Try consuming `cost` tokens right now without queueing.
    ///
    /// Succeeds only if tokens are available, no strictly higher-priority
    /// waiter is queued, and no same-priority waiter precedes us — the exact
    /// conditions under which an immediate grant would occur in [`Self::acquire`].
    pub async fn try_acquire(
        &self,
        priority: RequestPriority,
        cost: Decimal,
    ) -> Result<bool, BrokerError> {
        if cost <= Decimal::ZERO {
            return Err(BrokerError::InvalidRequest(
                "token cost must be positive".into(),
            ));
        }
        let level = priority.level();
        let mut state = self.state.lock().await;
        Self::refill(&mut state, Instant::now());

        let strictly_higher_waiting = state.waiters.range(..level).any(|(_, q)| !q.is_empty());
        let same_level_waiting = state
            .waiters
            .get(&level)
            .map(|q| !q.is_empty())
            .unwrap_or(false);

        if state.tokens >= cost && !strictly_higher_waiting && !same_level_waiting {
            state.tokens -= cost;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Consume tokens, queueing behind strictly higher-priority waiters until
    /// granted. Waits up to `max_wait`; on expiry the waiter is deregistered
    /// and [`BrokerError::Timeout`] is returned (tokens already spent on our
    /// behalf by a racing grant still complete as success).
    pub async fn acquire_for(
        &self,
        priority: RequestPriority,
        cost: Decimal,
        max_wait: Duration,
    ) -> Result<(), BrokerError> {
        if cost <= Decimal::ZERO {
            return Err(BrokerError::InvalidRequest(
                "token cost must be positive".into(),
            ));
        }
        let deadline = Instant::now() + max_wait;
        let level = priority.level();
        let ticket = Arc::new(Notify::new());
        let core = Arc::new(WaiterCore {
            cost,
            granted: AtomicBool::new(false),
            ticket,
        });
        let mut registered = false;

        loop {
            let now = Instant::now();
            let mut state = self.state.lock().await;
            Self::refill(&mut state, now);

            // 1. Racing grant? Tokens were deducted on our behalf — success.
            if core.granted.load(Ordering::SeqCst) {
                return Ok(());
            }

            // 2. Deadline expired with no grant: deregister and fail.
            if now >= deadline {
                if registered {
                    Self::deregister(&self.broker, &mut state, level, &core);
                }
                crate::telemetry::record_rate_limit_drop(&self.broker);
                let waited = max_wait;
                return Err(BrokerError::RateLimited(format!(
                    "broker rate limiter exhausted: priority {priority:?} acquire of {cost} token(s) \
                     timed out after waiting {waited:?}"
                )));
            }

            // 3. Fast path: if we can be served right now without joining a
            //    queue (no strictly higher waiter, we would be first at our
            //    level), skip the queue entirely — this is what makes
            //    Immediate/kill-switch grants sub-millisecond.
            if !registered {
                let strictly_higher_waiting =
                    state.waiters.range(..level).any(|(_, q)| !q.is_empty());
                let same_level_waiting = state
                    .waiters
                    .get(&level)
                    .map(|q| !q.is_empty())
                    .unwrap_or(false);
                if state.tokens >= cost && !strictly_higher_waiting && !same_level_waiting {
                    state.tokens -= cost;
                    return Ok(());
                }
                // Subscribe: join our priority queue.
                state
                    .waiters
                    .entry(level)
                    .or_insert_with(VecDeque::new)
                    .push_back(core.clone());
                registered = true;
                let q_len = state.waiters.get(&level).map(|q| q.len()).unwrap_or(0);
                crate::telemetry::set_queue_depth(
                    &self.broker,
                    priority_level_str(level),
                    q_len as i64,
                );
            }

            // 4. Grant pass: service eligible heads (ours may be granted here).
            Self::dispatch(&self.broker, &mut state);
            if core.granted.load(Ordering::SeqCst) {
                return Ok(());
            }

            // 5. Park until either a grant notification or a budget-accrual
            //    timer wakes us (capped, so nothing can strand us).
            let park_delay = state
                .time_to_afford(cost)
                .clamp(Duration::from_millis(1), Duration::from_secs(1));
            drop(state);

            tokio::select! {
                _ = core.ticket.notified() => {}
                _ = tokio::time::sleep(park_delay) => {}
            }
        }
    }

    /// Consume tokens with a default one-day budget; production call sites
    /// should prefer [`Self::acquire_for`] with bounded waits.\npub async fn acquire(
    pub async fn acquire(
        &self,
        priority: RequestPriority,
        cost: Decimal,
    ) -> Result<(), BrokerError> {
        self.acquire_for(priority, cost, Duration::from_secs(24 * 60 * 60))
            .await
    }

    /// Internal: elapsed-time token accrual, clamped to capacity.
    fn refill(state: &mut BucketState, now: Instant) {
        let elapsed = now.saturating_duration_since(state.last_update);
        state.last_update = now;
        if elapsed.is_zero() {
            return;
        }
        let secs = Decimal::from(elapsed.as_secs())
            + Decimal::from(elapsed.subsec_nanos()) / Decimal::from(1_000_000_000u64);
        let accrual = state.config.refill_rate_per_sec * secs;
        state.tokens = (state.tokens + accrual).min(state.config.capacity_tokens);
    }

    /// Internal: grant pass. Iterates priority levels in strict order; grants
    /// the head of an affordable level, pops it, and continues; stops at the
    /// first unaffordable head (lower priorities must not steal its budget).
    fn dispatch(broker: &str, state: &mut BucketState) {
        loop {
            let head = state
                .waiters
                .iter()
                .filter(|(_, q)| !q.is_empty())
                .map(|(level, q)| (*level, q.front().unwrap().clone()))
                .next();

            let Some((level, head_core)) = head else {
                return;
            };
            if state.tokens < head_core.cost {
                return; // Strict priority: never skip ahead of a richer waiter.
            }
            state.tokens -= head_core.cost;
            head_core.granted.store(true, Ordering::SeqCst);
            Self::pop_waiter(state, &head_core);
            let q_len = state.waiters.get(&level).map(|q| q.len()).unwrap_or(0);
            crate::telemetry::set_queue_depth(broker, priority_level_str(level), q_len as i64);
            head_core.ticket.notify_one();
            // Loop again: the freed budget may satisfy same/lower-priority heads.
        }
    }

    /// Internal: remove one identity from its queue (used on grant-pop and
    /// timeout-deregister).
    fn pop_waiter(state: &mut BucketState, core: &Arc<WaiterCore>) {
        for queue in state.waiters.values_mut() {
            if queue
                .iter()
                .position(|w| Arc::ptr_eq(w, core))
                .map(|idx| queue.remove(idx))
                .is_some()
            {
                return;
            }
        }
        warn!("rate limiter: waiter identity not found in queue (invariant violated?)");
    }

    /// Internal: timeout-deregister a specific waiter identity.
    fn deregister(broker: &str, state: &mut BucketState, level: u8, core: &Arc<WaiterCore>) {
        if let Some(queue) = state.waiters.get_mut(&level)
            && let Some(idx) = queue.iter().position(|w| Arc::ptr_eq(w, core))
        {
            queue.remove(idx);
            let q_len = queue.len();
            crate::telemetry::set_queue_depth(broker, priority_level_str(level), q_len as i64);
            return;
        }
        // Defensive sweep across all levels (should never be needed).
        Self::pop_waiter(state, core);
        let q_len = state.waiters.get(&level).map(|q| q.len()).unwrap_or(0);
        crate::telemetry::set_queue_depth(broker, priority_level_str(level), q_len as i64);
    }
}

/// Generic [`BrokerExecutor`] wrapper that meters every call through a
/// per-broker token bucket with priority-ordered waiting.
///
/// | Operation | Priority |
/// |-----------|----------|
/// | place / modify | Normal |
/// | cancel | High |
/// | kill-switch cancel ([`Self::cancel_order_now`]) | Immediate |
/// | order-book / positions / margins / status / health probes | Low |
///
/// Fetch-class traffic is explicitly background-prioritized so portfolio
/// polls can never delay order placements or cancellations.
pub struct RateLimitedBroker<E: BrokerExecutor> {
    inner: E,
    limiter: Arc<TokenBucketRateLimiter>,
    max_wait: Duration,
}

impl<E: BrokerExecutor> RateLimitedBroker<E> {
    /// Wrap a broker executor with the documented preset for its `name()`.
    pub fn preset(inner: E) -> Result<Self, BrokerError> {
        let limiter = TokenBucketRateLimiter::for_broker(inner.name())?;
        Ok(Self {
            inner,
            limiter: Arc::new(limiter),
            max_wait: Duration::from_secs(2),
        })
    }

    /// Wrap a broker executor with an explicit limiter configuration.
    pub fn with_config(inner: E, config: RateLimiterConfig) -> Result<Self, BrokerError> {
        let limiter = TokenBucketRateLimiter::new(config)?.with_broker(inner.name());
        Ok(Self {
            inner,
            limiter: Arc::new(limiter),
            max_wait: Duration::from_secs(2),
        })
    }

    /// Override the maximum time an acquire will spend queueing.
    pub fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Shared limiter handle (for metrics/observability).
    pub fn limiter(&self) -> &Arc<TokenBucketRateLimiter> {
        &self.limiter
    }

    /// Underlying executor.
    pub fn inner(&self) -> &E {
        &self.inner
    }

    async fn budget_for(&self, priority: RequestPriority) -> Result<(), BrokerError> {
        self.limiter
            .acquire_for(priority, Decimal::ONE, self.max_wait)
            .await
            .map_err(|e| BrokerError::RateLimited(format!("token budget exhausted: {e}")))
    }

    /// Kill-switch cancellation: jumps every queue at [`RequestPriority::Immediate`].
    pub async fn cancel_order_now(&self, order_id: &str) -> Result<(), BrokerError> {
        self.budget_for(RequestPriority::Immediate).await?;
        self.inner.cancel_order(order_id).await
    }
}

#[async_trait]
impl<E: BrokerExecutor> BrokerExecutor for RateLimitedBroker<E> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        self.budget_for(RequestPriority::Normal).await?;
        self.inner.health_check().await
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        self.budget_for(RequestPriority::Normal).await?;
        self.inner.place_order(req).await
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        self.budget_for(RequestPriority::High).await?;
        self.inner.cancel_order(order_id).await
    }

    async fn modify_order(
        &self,
        order_id: &str,
        new_price: Option<Price>,
        new_quantity: Option<Quantity>,
    ) -> Result<OrderAck, BrokerError> {
        self.budget_for(RequestPriority::Normal).await?;
        self.inner
            .modify_order(order_id, new_price, new_quantity)
            .await
    }

    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        self.budget_for(RequestPriority::Low).await?;
        self.inner.fetch_order_book().await
    }

    async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError> {
        self.budget_for(RequestPriority::Low).await?;
        self.inner.fetch_positions().await
    }

    async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError> {
        self.budget_for(RequestPriority::Low).await?;
        self.inner.fetch_margins().await
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        self.budget_for(RequestPriority::Low).await?;
        self.inner.get_order_status(order_id).await
    }
}
