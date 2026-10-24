//! Native and synthetic iceberg execution engines (plan §3.4).
//!
//! Icebergs show only `Q.visible` per leg to the exchange depth book while
//! the hidden parent reserve rolls forward. Two routes:
//!
//! - **Native**: broker meters the legs itself — currently Zerodha, via
//!   [`honba_te_core::IcebergOrderExecutor`] posting the Kite Connect v3
//!   `iceberg` variety with `iceberg_quantity` and the derived `iceberg_legs`
//!   (2..=50, exact division, documented caps).
//! - **Synthetic**: universal fallback (Upstox, Angel One, paper brokers).
//!   [`run_synthetic_iceberg`] submits display-quantity children one at a
//!   time, advances to the next leg only after the active child reaches a
//!   terminal state (`Filled` matched over cumulative `FillReport` deltas),
//!   and aborts or records unfinished legs on rejection, cancellation, or
//!   placement failure.
//!
//! Synchronization ties into the parent audit chain: every child keeps
//! `correlation_id = parent.client_order_id`.

use std::time::Duration;

use rust_decimal::Decimal;
use uuid::Uuid;

use honba_core::{FillReport, OrderAck, OrderRequest, Quantity};
use honba_te_core::{
    BrokerError, BrokerExecutionEvent, BrokerExecutor, ExecutionStream, IcebergOrderExecutor,
    quantity_to_u32_exact,
};

/// Kite Connect v3 documented iceberg range caps.
pub const MIN_NATIVE_ICEBERG_LEGS: u64 = 2;
pub const MAX_NATIVE_ICEBERG_LEGS: u64 = 50;

/// Which iceberg route to take for a parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergLegging {
    /// Broker-native metered legging (requires `IcebergOrderExecutor`).
    Native,
    /// Client-managed display-quantity legs (universal).
    Synthetic,
}

/// Outcome of a synthetic iceberg run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticIcebergOutcome {
    /// Broker order IDs of legs that fully filled, in submission order.
    pub filled_legs: Vec<String>,
    /// Broker order IDs of legs terminated before filling fully.
    pub unfinished_legs: Vec<String>,
    /// Aggregate filled quantity across all legs.
    pub filled: Quantity,
}

/// Derived native iceberg leg count via the gateway contract (exact division,
/// `2..=50` legs).
pub fn native_leg_count(parent: &Quantity, display: &Quantity) -> Result<u64, BrokerError> {
    let parent = quantity_to_u32_exact(parent.0)? as u64;
    let display = quantity_to_u32_exact(display.0)? as u64;
    if display == 0 || display >= parent {
        return Err(BrokerError::InvalidRequest(format!(
            "iceberg display quantity {display} must be non-zero and smaller than parent {parent}"
        )));
    }
    if !parent.is_multiple_of(display) {
        return Err(BrokerError::InvalidRequest(format!(
            "iceberg display quantity {display} must divide parent {parent} exactly"
        )));
    }
    let legs = parent / display;
    if !(MIN_NATIVE_ICEBERG_LEGS..=MAX_NATIVE_ICEBERG_LEGS).contains(&legs) {
        return Err(BrokerError::InvalidRequest(format!(
            "iceberg leg count {legs} outside the gateway range \
             {MIN_NATIVE_ICEBERG_LEGS}..={MAX_NATIVE_ICEBERG_LEGS}"
        )));
    }
    Ok(legs)
}

/// Greedy synthetic leg split: legs of `display` quantity, last leg carries
/// the remainder (which may be smaller than `display`).
pub fn display_legs(parent: &Quantity, display: &Quantity) -> Result<Vec<Quantity>, BrokerError> {
    let parent = quantity_to_u32_exact(parent.0)? as u64;
    let display = quantity_to_u32_exact(display.0)? as u64;
    if display == 0 {
        return Err(BrokerError::InvalidRequest(
            "iceberg display quantity must be positive".into(),
        ));
    }
    let mut legs = Vec::new();
    let mut remaining = parent;
    while remaining > 0 {
        let chunk = remaining.min(display);
        legs.push(Quantity::from_u64(chunk));
        remaining -= chunk;
    }
    Ok(legs)
}

/// Place the parent through a broker-native iceberg route.
pub async fn place_native_iceberg(
    executor: &dyn IcebergOrderExecutor,
    parent: OrderRequest,
    display: Quantity,
) -> Result<OrderAck, BrokerError> {
    executor.place_iceberg_order(parent, display).await
}

/// Run a synthetic iceberg to completion against any [`BrokerExecutor`].
///
/// # Leg pacing
/// Children submit strictly sequentially: leg *i+1* is placed only after leg
/// *i* reaches a terminal state in the [`ExecutionStream`], matching the
/// "submit subsequent slices upon fill confirmation" requirement of §3.4.
///
/// # Termination policy
/// - Placement error on any leg → [`Err`] (abort before the next leg).
/// - Active-leg `Rejected` → [`Err::Rejected`] with the broker reason.
/// - Active-leg cancellation → recorded as an unfinished leg and the run
///   continues with the next leg (cancels may be pacing or square-off moves;
///   callers reconcile via the outcome).
/// - `wait` timeout on the active leg → cancel the child (best effort),
///   record unfinished, continue.
/// - Events for other order IDs are skipped (this runner tolerates an
///   unfiltered execution stream).
pub async fn run_synthetic_iceberg(
    executor: &dyn BrokerExecutor,
    stream: &mut dyn ExecutionStream,
    parent: OrderRequest,
    display: Quantity,
    wait: Duration,
) -> Result<SyntheticIcebergOutcome, BrokerError> {
    let mut outcome = SyntheticIcebergOutcome {
        filled_legs: Vec::new(),
        unfinished_legs: Vec::new(),
        filled: Quantity::ZERO,
    };
    let legs = display_legs(&parent.quantity, &display)?;
    for leg_quantity in legs {
        let child = OrderRequest {
            client_order_id: Uuid::new_v4(),
            instrument: parent.instrument.clone(),
            side: parent.side,
            order_type: parent.order_type,
            quantity: leg_quantity,
            tif: parent.tif,
            algo_id: parent.algo_id.clone(),
            tag: parent.tag.clone(),
            client_id: parent.client_id.clone(),
            correlation_id: Some(parent.client_order_id),
            trace_context: parent.trace_context.clone(),
        };
        let ack = executor.place_order(child).await?;
        let order_id = ack.order_id.clone();
        let leg_state = await_leg_terminal(executor, stream, &order_id, leg_quantity, wait).await?;
        match leg_state {
            LegTerminal::Filled => {
                outcome.filled_legs.push(order_id);
                outcome.filled = Quantity::new(outcome.filled.0 + leg_quantity.0);
            }
            LegTerminal::Abandoned(unfilled) => {
                outcome.unfinished_legs.push(order_id);
                outcome.filled = Quantity::new(outcome.filled.0 + (leg_quantity.0 - unfilled));
            }
        }
    }
    Ok(outcome)
}

enum LegTerminal {
    /// Leg filled its full quantity.
    Filled,
    /// Leg abandoned without full fill; carries the remaining quantity.
    Abandoned(Decimal),
}

async fn await_leg_terminal(
    executor: &dyn BrokerExecutor,
    stream: &mut dyn ExecutionStream,
    order_id: &str,
    leg_quantity: Quantity,
    wait: Duration,
) -> Result<LegTerminal, BrokerError> {
    let target = leg_quantity.0;
    let mut filled = Decimal::ZERO;
    let mut cancel_requested = false;
    loop {
        let event = match tokio::time::timeout(wait, stream.next_event()).await {
            Ok(event) => event?,
            Err(_) => {
                // Pacing cancel: best effort once, then keep draining for the
                // cancelled acknowledgement or any straggler fills.
                if !cancel_requested {
                    let _ = executor.cancel_order(order_id).await;
                    cancel_requested = true;
                    continue;
                }
                return Ok(LegTerminal::Abandoned(target - filled));
            }
        };
        if event.order_id() != Some(order_id) {
            continue;
        }
        match event {
            BrokerExecutionEvent::OrderAck(_) => {}
            BrokerExecutionEvent::Fill(FillReport { quantity, .. }) => {
                filled += quantity.0;
                if filled >= target {
                    return Ok(LegTerminal::Filled);
                }
            }
            BrokerExecutionEvent::OrderCancelled { .. } => {
                return Ok(LegTerminal::Abandoned(target - filled));
            }
            BrokerExecutionEvent::OrderRejected { reason, .. } => {
                return Err(BrokerError::Rejected(reason));
            }
        }
    }
}
