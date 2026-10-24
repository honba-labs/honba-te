//! Automatic parent-order slicing against scrip freeze limits (plan §3.4).
//!
//! [`FreezeLimitSlicer::plan`] decomposes a parent [`OrderRequest`] into
//! `N = ⌈Q / Q.freeze⌉` greedy children of size `min(remaining, freeze)`, the
//! documented split. Children inherit every parent leg (instrument, side,
//! type, prices, TIF, SEBI algo id) and stay synchronized through the SEBI
//! audit trail: each child carries a fresh client UUID in
//! `client_order_id` and the parent's UUID in `correlation_id`, so all legs
//! of one parent group into a single audit chain. Child tags are the parent's
//! tag untouched (Kite tag caps at 20 chars; correlation is the grouping key).
//!
//! The split is deterministic and exact: integer arithmetic over `u64`, no
//! floats, no rounding. When the parent fits under the limit it passes
//! through as a single child.

use std::sync::Arc;

use uuid::Uuid;

use honba_core::{OrderRequest, Quantity};
use honba_te_core::{BrokerError, BrokerExecutor, quantity_to_u32_exact};

use crate::freeze::FreezeLimitCatalog;

/// One planned child order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceLeg {
    /// Zero-based child index inside the plan.
    pub index: u32,
    /// Child quantity (never above the applicable freeze limit).
    pub quantity: Quantity,
    /// Child client order ID (fresh UUID, distinct per leg).
    pub client_order_id: Uuid,
}

/// A deterministic decomposition of one parent order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlicePlan {
    /// Parent client order ID the children correlate to.
    pub parent_client_order_id: Uuid,
    /// Applicable freeze limit driving the split (informative for logs).
    pub freeze_quantity: Option<u64>,
    /// Children in submission order.
    pub legs: Vec<SliceLeg>,
}

impl SlicePlan {
    /// Total child quantity (must reconstruct the parent's quantity).
    pub fn total_quantity(&self) -> Quantity {
        let mut sum = rust_decimal::Decimal::ZERO;
        for leg in &self.legs {
            sum += leg.quantity.0;
        }
        Quantity::new(sum)
    }

    /// Materialize submission-ready child [`OrderRequest`]s.
    ///
    /// Child legs inherit the parent fully; only quantity and identities are
    /// rewritten. Correlation targets the parent so downstream OMS/audit can
    /// group the synchronized leg chain.
    pub fn into_order_requests(&self, parent: &OrderRequest) -> Vec<OrderRequest> {
        self.legs
            .iter()
            .map(|leg| OrderRequest {
                client_order_id: leg.client_order_id,
                instrument: parent.instrument.clone(),
                side: parent.side,
                order_type: parent.order_type,
                quantity: leg.quantity,
                tif: parent.tif,
                algo_id: parent.algo_id.clone(),
                tag: parent.tag.clone(),
                client_id: parent.client_id.clone(),
                correlation_id: Some(self.parent_client_order_id),
                trace_context: parent.trace_context.clone(),
            })
            .collect()
    }
}

/// Freeze-limit aware slicer over a [`FreezeLimitCatalog`].
#[derive(Debug, Clone)]
pub struct FreezeLimitSlicer {
    catalog: Arc<FreezeLimitCatalog>,
}

impl FreezeLimitSlicer {
    /// Slicer over a catalog (builtin index constants may be preloaded).
    pub fn new(catalog: Arc<FreezeLimitCatalog>) -> Self {
        Self { catalog }
    }

    /// Loaded catalog reference.
    pub fn catalog(&self) -> &Arc<FreezeLimitCatalog> {
        &self.catalog
    }

    /// Plan the freeze-limit split of one parent order.
    ///
    /// # Errors
    /// - [`BrokerError::InvalidRequest`] for non-positive or fractional
    ///   parent quantities (exactness mandate).
    pub fn plan(&self, parent: &OrderRequest) -> Result<SlicePlan, BrokerError> {
        let freeze = self.catalog.slicing_limit(&parent.instrument);
        plan_against_limit(parent, freeze.map(|f| f.freeze_quantity))
    }
}

/// Free-function plan against an explicit cap (`None` = single leg).
///
/// Greedy full-freeze chunks: `N = ⌈Q / freeze⌉` legs, each
/// `min(remaining, freeze)`, the last leg carrying the remainder. Keeps the
/// leg count minimal and children maximally actionable.
pub fn plan_against_limit(
    parent: &OrderRequest,
    freeze: Option<u64>,
) -> Result<SlicePlan, BrokerError> {
    let quantity = quantity_to_u32_exact(parent.quantity.0)? as u64;
    let limit = freeze.unwrap_or(u64::MAX).max(1);
    let leg_count = quantity.div_ceil(limit);
    if leg_count > u32::MAX as u64 {
        return Err(BrokerError::InvalidRequest(
            "slice plan exceeds leg capacity".into(),
        ));
    }
    let mut legs = Vec::with_capacity(leg_count as usize);
    let mut remaining = quantity;
    let mut index = 0u32;
    while remaining > 0 {
        let chunk = remaining.min(limit);
        legs.push(SliceLeg {
            index,
            quantity: Quantity::from_u64(chunk),
            client_order_id: Uuid::new_v4(),
        });
        remaining -= chunk;
        index += 1;
    }
    Ok(SlicePlan {
        parent_client_order_id: parent.client_order_id,
        freeze_quantity: freeze,
        legs,
    })
}

/// Place every leg of a [`SlicePlan`] sequentially on the broker.
///
/// Leg submission is synchronous in order; on the first broker error the
/// orchestrator aborts immediately and returns the error (the completed
/// prefix stays on the exchange — callers reconcile via the execution
/// stream). Works both directly and through
/// [`honba_te_core::RateLimitedBroker`]/[`honba_te_core::RiskGatedExecutor`]
/// wrappers since only the [`BrokerExecutor`] trait is required.
pub async fn place_slice_plan(
    executor: &dyn BrokerExecutor,
    parent: &OrderRequest,
    plan: &SlicePlan,
) -> Result<Vec<honba_core::OrderAck>, BrokerError> {
    let children = plan.into_order_requests(parent);
    let mut acks = Vec::with_capacity(children.len());
    for child in children {
        acks.push(executor.place_order(child).await?);
    }
    Ok(acks)
}
