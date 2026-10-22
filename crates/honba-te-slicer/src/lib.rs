//! # 本場・手 honba-te-slicer
//!
//! SEBI scrip-freeze-limit slicing and execution algorithms (plan §3.4, M3).
//!
//! Indian exchanges reject parent orders whose quantity exceeds the scrip freeze
//! limit (e.g. NIFTY 1800, BANKNIFTY 900). This crate decomposes parents into
//! permissible child orders and provides the iceberg and TWAP execution
//! algorithms. Every quantity path is exact `rust_decimal::Decimal` backend
//! arithmetic over integers — no floating point, ever.
//!
//! # Module map
//! - [`freeze`] — [`FreezeLimitCatalog`] of per-scrip freeze quantities
//!   (CSV/JSON master-data loaders + builtin index constants)
//! - [`slice`] — [`FreezeLimitSlicer`]: parent → `N = ⌈Q/Qfreeze⌉` children
//!   with synchronized client IDs, plus the placement orchestrator
//! - [`iceberg`] — native (Kite variety `iceberg`) and synthetic iceberg
//!   engines with visible-quantity legging
//! - [`twap`] — time-weighted schedule with deterministic zero-float Gaussian
//!   jitter and limit-chase re-pricing
//! - [`jitter`] — deterministic integer jitter source (mulberry32 / Irwin–Hall)
//!
//! # Domain purity
//! Planning functions ([`FreezeLimitSlicer::plan`], [`iceberg::native_leg_count`],
//! [`twap::TwapPlan::build`]) are pure: they take an explicit `start` timestamp,
//! never query the clock, and mutate nothing. Runtime orchestrators
//! ([`slice::place_slice_plan`], [`iceberg::run_synthetic_iceberg`],
//! [`twap::run_twap`]) drive a [`honba_te_core::BrokerExecutor`] +
//! [`honba_te_core::ExecutionStream`] pair and are the only I/O layer.

pub mod freeze;
pub mod iceberg;
pub mod jitter;
pub mod slice;
pub mod twap;

pub use freeze::{FreezeLimitCatalog, FreezeLimitRecord, builtin_index_freezes};
pub use iceberg::{
    IcebergLegging, MAX_NATIVE_ICEBERG_LEGS, MIN_NATIVE_ICEBERG_LEGS, SyntheticIcebergOutcome,
    display_legs, native_leg_count, place_native_iceberg, run_synthetic_iceberg,
};
pub use jitter::JitterSource;
pub use slice::{FreezeLimitSlicer, SliceLeg, SlicePlan, place_slice_plan, plan_against_limit};
pub use twap::{TwapChase, TwapConfig, TwapLeg, TwapOutcome, TwapPlan, run_twap};

// Canonical domain re-exports for caller convenience.
pub use honba_core::{Exchange, OrderRequest, Price, Quantity};
pub use honba_te_core::{BrokerError, BrokerExecutor, ExecutionStream};
