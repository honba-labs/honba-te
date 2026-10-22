//! M4 health circuit-breaker tests (plan §3.5 item 2): rolling-RTT trips,
//! consecutive-failure trips, trip→cooldown→HalfOpen→probe state machine.

use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use honba_te_router::{CircuitState, HealthConfig, HealthTracker};

fn tracker() -> HealthTracker {
    HealthTracker::new(HealthConfig::default())
}

fn after(now: DateTime<Utc>, seconds: i64) -> DateTime<Utc> {
    now + ChronoDuration::seconds(seconds)
}

#[test]
fn closed_breaker_accepts_traffic() {
    let breaker = tracker();
    let now = Utc::now();
    assert_eq!(breaker.state(now), CircuitState::Closed);
    assert!(breaker.is_routable(now));
    assert_eq!(breaker.avg_rtt(), None);
    assert_eq!(breaker.consecutive_failures(), 0);
}

#[test]
fn three_consecutive_failures_trip_the_breaker() {
    let breaker = tracker();
    let now = Utc::now();
    breaker.record_failure(now);
    breaker.record_failure(now);
    assert_eq!(
        breaker.state(now),
        CircuitState::Closed,
        "two failures stay below the plan's 3-consecutive threshold"
    );
    breaker.record_failure(now);
    assert_eq!(breaker.state(now), CircuitState::Open);
    assert!(!breaker.is_routable(now));
}

#[test]
fn open_breaker_rearms_to_halfopen_after_cooldown() {
    let breaker = tracker();
    let now = Utc::now();
    for _ in 0..3 {
        breaker.record_failure(now);
    }
    assert_eq!(breaker.state(after(now, 29)), CircuitState::Open);
    assert_eq!(
        breaker.state(after(now, 30)),
        CircuitState::HalfOpen,
        "cooldown elapsed → single probe may pass"
    );
    assert!(breaker.is_routable(after(now, 30)));
}

#[test]
fn halfopen_probe_success_reopens_the_circuit() {
    let breaker = tracker();
    let now = Utc::now();
    for _ in 0..3 {
        breaker.record_failure(now);
    }
    let probe = after(now, 30);
    breaker.record_success(probe);
    assert_eq!(breaker.state(probe), CircuitState::Closed);
    assert_eq!(
        breaker.consecutive_failures(),
        0,
        "streak reset with recovery"
    );
}

#[test]
fn halfopen_probe_failure_rearms_fresh_cooldown() {
    let breaker = tracker();
    let now = Utc::now();
    for _ in 0..3 {
        breaker.record_failure(now);
    }
    let probe = after(now, 30);
    breaker.record_failure(probe);
    assert_eq!(
        breaker.state(probe),
        CircuitState::Open,
        "probe failed → re-trip"
    );
    assert!(!breaker.is_routable(probe));
    assert_eq!(
        breaker.state(after(probe, 30)),
        CircuitState::HalfOpen,
        "fresh cooldown of another full window"
    );
}

#[test]
fn rolling_rtt_above_500ms_trips_after_min_samples() {
    let breaker = tracker();
    let now = Utc::now();
    breaker.record_latency(Duration::from_millis(600), now);
    breaker.record_latency(Duration::from_millis(600), now);
    assert_eq!(
        breaker.state(now),
        CircuitState::Closed,
        "two samples: below rtt_min_samples authority"
    );
    breaker.record_latency(Duration::from_millis(600), now);
    assert_eq!(
        breaker.state(now),
        CircuitState::Open,
        "rolling average 600 ms > 500 ms plan ceiling"
    );
}

#[test]
fn healthy_latency_keeps_breaker_closed() {
    let breaker = tracker();
    let now = Utc::now();
    for _ in 0..10 {
        breaker.record_latency(Duration::from_millis(40), now);
    }
    assert_eq!(breaker.state(now), CircuitState::Closed);
    // Window is a rolling ring of 10: the old spike must roll out.
    breaker.record_latency(Duration::from_millis(800), now);
    assert_eq!(
        breaker.state(now),
        CircuitState::Closed,
        "1 spike in 10 ≤ threshold"
    );
}

#[test]
fn success_clears_consecutive_failure_streak() {
    let breaker = tracker();
    let now = Utc::now();
    breaker.record_failure(now);
    breaker.record_failure(now);
    breaker.record_success(now);
    breaker.record_failure(now);
    breaker.record_failure(now);
    assert_eq!(
        breaker.state(now),
        CircuitState::Closed,
        "streak reset by success"
    );
}
