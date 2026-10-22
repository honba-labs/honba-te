//! Dynamic broker health tracking & latency circuit breakers (plan §3.5).
//!
//! Each registered broker carries a [`HealthTracker`] fed by the router:
//! rolling round-trip latency samples, consecutive gateway failures (HTTP 5xx
//! and transport faults), and a Closed → Open → HalfOpen → Closed breaker
//! state machine. Trip conditions per the plan: rolling RTT average above
//! [`HealthConfig::max_rtt`] (500 ms default) or 3 consecutive gateway
//! failures. An Open breaker re-arms to HalfOpen after
//! [`HealthConfig::cooldown`] so a single probe request can prove recovery.
//!
//! All time is injected as explicit `DateTime<Utc>` — no clock queries inside
//! the domain logic (AGENTS §2.5).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};

/// Circuit breaker lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Healthy: accepts traffic.
    Closed,
    /// Tripped: rejects traffic until the cooldown elapses.
    Open,
    /// Cooldown elapsed: one probe request may pass to prove recovery.
    HalfOpen,
}

/// Breaker thresholds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthConfig {
    /// Rolling-average RTT above which the breaker trips (plan: 500 ms).
    pub max_rtt: Duration,
    /// Rolling latency window size (most recent samples).
    pub rtt_window_samples: usize,
    /// Samples required before the RTT average is authoritative.
    pub rtt_min_samples: usize,
    /// Consecutive gateway failures (5xx / transport) that trip the breaker
    /// (plan: 3 consecutive HTTP 5xx).
    pub max_consecutive_failures: u32,
    /// How long an Open breaker stays Open before re-arming to HalfOpen.
    pub cooldown: chrono::Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            max_rtt: Duration::from_millis(500),
            rtt_window_samples: 10,
            rtt_min_samples: 3,
            max_consecutive_failures: 3,
            cooldown: chrono::Duration::seconds(30),
        }
    }
}

#[derive(Debug)]
struct Inner {
    window: VecDeque<Duration>,
    streak: u32,
    /// Trip deadline: `Some(reopen)` while tripped. `now < reopen` → Open,
    /// `now >= reopen` → HalfOpen (pointer to the same breaker via probe
    /// success/failure transitions).
    reopens_at: Option<DateTime<Utc>>,
}

impl Inner {
    fn trip(&mut self, now: DateTime<Utc>, config: &HealthConfig) {
        if self.reopens_at.is_none() {
            self.reopens_at = Some(now + config.cooldown);
            self.streak = 0;
        }
    }
}

/// Rolling-health circuit breaker for one registered broker.
#[derive(Debug)]
pub struct HealthTracker {
    config: HealthConfig,
    inner: Mutex<Inner>,
}

impl HealthTracker {
    /// Tracker for one broker under `config` thresholds.
    pub fn new(config: HealthConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner {
                window: VecDeque::new(),
                streak: 0,
                reopens_at: None,
            }),
        }
    }

    /// Threshold configuration back-reference.
    pub fn config(&self) -> &HealthConfig {
        &self.config
    }

    /// Derived breaker state at `now`.
    pub fn state(&self, now: DateTime<Utc>) -> CircuitState {
        let inner = self.inner.lock().unwrap();
        match inner.reopens_at {
            Some(reopen) if now < reopen => CircuitState::Open,
            Some(_) => CircuitState::HalfOpen,
            None => CircuitState::Closed,
        }
    }

    /// True when the router may send traffic (Closed, or HalfOpen probe).
    pub fn is_routable(&self, now: DateTime<Utc>) -> bool {
        !matches!(self.state(now), CircuitState::Open)
    }

    /// Rolling-average RTT of the recorded window (`None` before samples).
    pub fn avg_rtt(&self) -> Option<Duration> {
        let inner = self.inner.lock().unwrap();
        if inner.window.is_empty() {
            return None;
        }
        let total: Duration = inner
            .window
            .iter()
            .copied()
            .fold(Duration::ZERO, std::ops::Add::add);
        Some(total / (inner.window.len() as u32))
    }

    /// Consecutive gateway-failure streak (0 when healthy).
    pub fn consecutive_failures(&self) -> u32 {
        self.inner.lock().unwrap().streak
    }

    /// Record one completed round trip; trips on averages above `max_rtt`
    /// once enough samples exist.
    pub fn record_latency(&self, rtt: Duration, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().unwrap();
        if inner.window.len() >= self.config.rtt_window_samples {
            inner.window.pop_front();
        }
        inner.window.push_back(rtt);
        if inner.reopens_at.is_none() && inner.window.len() >= self.config.rtt_min_samples {
            let total: Duration = inner
                .window
                .iter()
                .copied()
                .fold(Duration::ZERO, std::ops::Add::add);
            let average = total / (inner.window.len() as u32);
            if average > self.config.max_rtt {
                inner.trip(now, &self.config);
            }
        }
    }

    /// A request reached the gateway and completed — clears the failure
    /// streak and closes a HalfOpen probe.
    pub fn record_success(&self, _now: DateTime<Utc>) {
        let mut inner = self.inner.lock().unwrap();
        inner.streak = 0;
        inner.reopens_at = None;
    }

    /// Record a gateway failure (HTTP 5xx or transport fault). Trips when the
    /// streak reaches `max_consecutive_failures`; re-arms the cooldown when
    /// the failure occurred inside a HalfOpen probe.
    pub fn record_failure(&self, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().unwrap();
        inner.streak += 1;
        match inner.reopens_at {
            // Already Open → nothing else to arm.
            Some(reopen) if now < reopen => {}
            // HalfOpen probe failed → re-arm a fresh cooldown.
            Some(_) => {
                inner.reopens_at = Some(now + self.config.cooldown);
                inner.streak = 0;
            }
            // Closed → trip on the streak threshold.
            None => {
                if inner.streak >= self.config.max_consecutive_failures {
                    inner.trip(now, &self.config);
                }
            }
        }
    }
}
