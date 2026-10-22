//! Time-Weighted Average Price execution (plan §3.4).
//!
//! [`TwapPlan::build`] distributes a parent across `N` legs evenly spaced
//! over the horizon, lifting `N` when the catalog freeze limit demands more
//! legs so the freeze invariant (`child ≤ Q.freeze`) holds in every leg.
//! Sub-second Gaussian jitter masks the algorithm's footprint on the tape;
//! the jitter stream is deterministic from an explicit seed, keeping the
//! whole schedule reproducible from the audit journal.
//!
//! [`run_twap`] executes the plan: it sleeps to each leg's schedule slot,
//! submits the leg, waits for terminal fills, and — when the parent is a
//! limit order and a [`TwapChase`] is configured — performs the limit-chase:
//! cancel the quiet child, re-price `step` ticks toward the aggressing side,
//! and resubmit the unfilled remainder, up to `max_steps` re-prices per leg.

use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use honba_core::{FillReport, OrderRequest, OrderSide, OrderType, Price, Quantity};
use honba_te_core::{
    BrokerError, BrokerExecutionEvent, BrokerExecutor, ExecutionStream, quantity_to_u32_exact,
};

use crate::freeze::FreezeLimitCatalog;
use crate::jitter::JitterSource;

/// Limit-chase behaviour knobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwapChase {
    /// Maximum re-price attempts per leg (0 disables chasing entirely).
    pub max_steps: u32,
    /// Exact tick size for stepping the limit price (e.g. `0.05`).
    pub tick: Decimal,
    /// Quiet-window after which an unfilled child is cancelled and chased.
    pub chase_after: Duration,
}

impl TwapChase {
    /// Chase config with a Decimal tick parsed exactly from string form.
    pub fn with_tick(
        max_steps: u32,
        tick: &str,
        chase_after: Duration,
    ) -> Result<Self, BrokerError> {
        let tick: Decimal = tick
            .parse()
            .map_err(|e| BrokerError::InvalidRequest(format!("chase tick: {e}")))?;
        if tick <= Decimal::ZERO {
            return Err(BrokerError::InvalidRequest(
                "chase tick must be positive".into(),
            ));
        }
        Ok(Self {
            max_steps,
            tick,
            chase_after,
        })
    }
}

/// TWAP configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwapConfig {
    /// Desired leg count (lifted automatically when the freeze limit
    /// requires more legs than requested).
    pub slices: u32,
    /// Submission horizon across which legs are evenly spaced.
    pub horizon: Duration,
    /// Gaussian jitter standard deviation in milliseconds (0 = exact grid).
    pub jitter: Duration,
    /// Optional limit-chase policy (limit parents only).
    pub chase: Option<TwapChase>,
}

/// One scheduled leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwapLeg {
    /// Zero-based leg index.
    pub index: u32,
    /// Wall-clock submission slot (nominal grid + Gaussian jitter).
    pub submitted_at: DateTime<Utc>,
    /// Leg quantity (never above the freeze limit).
    pub quantity: Quantity,
}

/// A deterministic TWAP schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwapPlan {
    /// Parent the plan was built from.
    pub parent_client_order_id: Uuid,
    /// Scheduled legs.
    pub legs: Vec<TwapLeg>,
}

impl TwapPlan {
    /// Build the schedule for a parent order.
    ///
    /// # Arguments
    /// - `cfg` — leg count, horizon, jitter, chase policy.
    /// - `catalog` — freeze catalog; its limit lifts the leg count when
    ///   `⌈Q / Q.freeze⌉` exceeds `cfg.slices` so every child respects the
    ///   freeze invariant.
    /// - `start` — schedule origin (explicit clock, domain purity).
    /// - `seed` — jitter PRNG seed (auditable derivation from the parent, e.g.
    ///   `parent.client_order_id.as_u128() as u64`).
    ///
    /// # Determinism
    /// Same inputs (including the same seed) produce a byte-identical
    /// schedule; the jitter stream is replayed exactly.
    pub fn build(
        parent: &OrderRequest,
        cfg: &TwapConfig,
        catalog: &FreezeLimitCatalog,
        start: DateTime<Utc>,
        seed: u64,
    ) -> Result<TwapPlan, BrokerError> {
        let quantity = quantity_to_u32_exact(parent.quantity.0)? as u64;
        if cfg.slices == 0 {
            return Err(BrokerError::InvalidRequest(
                "TWAP requires at least one slice".into(),
            ));
        }
        let min_legs = match catalog.slicing_limit(&parent.instrument) {
            Some(limit) => quantity.div_ceil(limit.freeze_quantity.max(1)),
            None => 1,
        };
        let leg_count = (cfg.slices as u64).max(min_legs);
        if leg_count > u32::MAX as u64 {
            return Err(BrokerError::InvalidRequest(
                "TWAP slice count exceeds leg capacity".into(),
            ));
        }

        // Even distribution with the remainder spread over the first legs:
        // every leg ≤ freeze by construction when n ≥ ⌈Q/freeze⌉.
        let base = quantity / leg_count;
        let remainder = quantity % leg_count;

        let mut jitter = JitterSource::from_seed(seed);
        let jitter_sd_ms = cfg.jitter.as_millis() as i64;
        let horizon_ms = cfg.horizon.as_millis() as i128;

        let mut legs = Vec::with_capacity(leg_count as usize);
        for index in 0..leg_count {
            let mut leg_quantity = base;
            if index < remainder {
                leg_quantity += 1;
            }
            let slot_ms = horizon_ms * index as i128 / leg_count as i128;
            let jitter_ms = jitter.gaussian_millis(jitter_sd_ms) as i128;
            let submitted_at = start
                + chrono::Duration::milliseconds(
                    (slot_ms + jitter_ms).clamp(i64::MIN as i128, i64::MAX as i128) as i64,
                );
            legs.push(TwapLeg {
                index: index as u32,
                submitted_at,
                quantity: Quantity::from_u64(leg_quantity),
            });
        }
        Ok(TwapPlan {
            parent_client_order_id: parent.client_order_id,
            legs,
        })
    }

    /// Materialize one submission-ready child for `leg` from the parent.
    pub fn child_request(&self, parent: &OrderRequest, leg: &TwapLeg) -> OrderRequest {
        OrderRequest {
            client_order_id: Uuid::new_v4(),
            instrument: parent.instrument.clone(),
            side: parent.side,
            order_type: parent.order_type,
            quantity: leg.quantity,
            tif: parent.tif,
            algo_id: parent.algo_id.clone(),
            tag: parent.tag.clone(),
            client_id: parent.client_id.clone(),
            correlation_id: Some(self.parent_client_order_id),
        }
    }
}

/// Aggregate output of a TWAP run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TwapOutcome {
    /// Broker order IDs of submitted legs, in submission order.
    pub placed_legs: Vec<String>,
    /// Total filled quantity observed on the stream.
    pub filled: Quantity,
    /// Number of chase re-prices executed.
    pub chase_steps: u32,
}

/// Execute a [`TwapPlan`].
///
/// Legs sleep to their schedule slot, submit, and wait for a terminal state.
/// Limit parents with a configured [`TwapChase`] cancel a quiet child after
/// `chase_after`, re-price `step × tick` toward the aggressing side, and
/// resubmit only the unfilled remainder (partial fills are credited first).
/// Market parents never chase. All children correlate to the parent.
pub async fn run_twap(
    executor: &dyn BrokerExecutor,
    stream: &mut dyn ExecutionStream,
    parent: &OrderRequest,
    plan: &TwapPlan,
    chase: Option<&TwapChase>,
) -> Result<TwapOutcome, BrokerError> {
    let mut outcome = TwapOutcome::default();
    for leg in &plan.legs {
        // Sleep until the leg's schedule slot (no-op when already due).
        let now = Utc::now();
        let delta = leg.submitted_at - now;
        let delta = if delta > chrono::Duration::zero() {
            delta
                .to_std()
                .map_err(|e| BrokerError::Transport(format!("schedule sleep conversion: {e}")))?
        } else {
            Duration::ZERO
        };
        if !delta.is_zero() {
            tokio::time::sleep(delta).await;
        }

        let leg_filled =
            await_leg(executor, stream, parent, plan, leg, chase, &mut outcome).await?;
        outcome.filled = Quantity::new(outcome.filled.0 + leg_filled);
    }
    Ok(outcome)
}

async fn await_leg(
    executor: &dyn BrokerExecutor,
    stream: &mut dyn ExecutionStream,
    parent: &OrderRequest,
    plan: &TwapPlan,
    leg: &TwapLeg,
    chase: Option<&TwapChase>,
    outcome: &mut TwapOutcome,
) -> Result<Decimal, BrokerError> {
    let mut remaining = leg.quantity.0;
    let mut leg_filled = Decimal::ZERO;
    let mut step = 0u32;
    while remaining > Decimal::ZERO {
        let mut child = plan.child_request(parent, leg);
        child.quantity = Quantity::new(remaining);
        if let Some(clause) = chase
            && let OrderType::Limit(base_price) = parent.order_type
            && step > 0
        {
            child.order_type =
                OrderType::Limit(chased_price(base_price, parent.side, clause.tick, step)?);
        }
        let ack = executor.place_order(child).await?;
        outcome.placed_legs.push(ack.order_id.clone());
        // Each submitted child begins its own quiet/cancel cycle.
        let mut cancel_requested = false;

        loop {
            let wait = chase.map(|c| c.chase_after);
            let event = match wait {
                Some(window) => match tokio::time::timeout(window, stream.next_event()).await {
                    Ok(event) => event?,
                    Err(_) => {
                        // Quiet window expired — cancel and chase once per
                        // submitted child; a second silent window means the
                        // cancel ack never surfaced (fail fast, nothing hung).
                        if !cancel_requested {
                            executor
                                .cancel_order(&ack.order_id)
                                .await
                                .map_err(|e| BrokerError::Rejected(format!("chase cancel: {e}")))?;
                            cancel_requested = true;
                            continue;
                        }
                        return Err(BrokerError::Transport(
                            "twap chase: cancel ack not observed before the quiet window".into(),
                        ));
                    }
                },
                None => stream.next_event().await?,
            };
            if event.order_id() != Some(ack.order_id.as_str()) {
                continue;
            }
            match event {
                BrokerExecutionEvent::OrderAck(_) => {}
                BrokerExecutionEvent::Fill(FillReport { quantity, .. }) => {
                    leg_filled += quantity.0;
                    remaining -= quantity.0;
                    if remaining <= Decimal::ZERO {
                        return Ok(leg_filled);
                    }
                }
                BrokerExecutionEvent::OrderCancelled { .. } => {
                    // Chase re-price (or externally forced cancel): step
                    // upward and resubmit the remainder.
                    match chase {
                        Some(clause) if step < clause.max_steps => {
                            step += 1;
                            outcome.chase_steps += 1;
                        }
                        // Budget exhausted (or no chase policy) — leave the
                        // remainder unexecuted on this leg and move on.
                        _ => return Ok(leg_filled),
                    }
                    break; // resubmit at the chased price
                }
                BrokerExecutionEvent::OrderRejected { reason, .. } => {
                    return Err(BrokerError::Rejected(reason));
                }
            }
        }
    }
    Ok(leg_filled)
}

/// Limit price after `steps` chase ticks toward the aggressing side.
///
/// Buy legs step up (toward the ask), sell legs step down. Exact Decimal
/// arithmetic; the result is revalidated by the executor's price formatting.
fn chased_price(
    base: Price,
    side: OrderSide,
    tick: Decimal,
    steps: u32,
) -> Result<Price, BrokerError> {
    let magnitude = tick * Decimal::from(steps);
    let moved = match side {
        OrderSide::Buy => base.0 + magnitude,
        OrderSide::Sell => base.0 - magnitude,
    };
    if moved <= Decimal::ZERO {
        return Err(BrokerError::InvalidRequest(format!(
            "chase crossed below the cent floor: {moved}"
        )));
    }
    Ok(Price::new(moved))
}
