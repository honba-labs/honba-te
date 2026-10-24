//! Observability helpers for broker gateways (plan §4 Phase 7).
//!
//! Provides Prometheus metrics (counters/histograms/gauges) for HTTP/WebSocket/rate-limit
//! behavior and helpers to attach/propagate [`honba_core::TraceContext`] on outbound
//! requests and inbound broker events.

use std::time::Duration;

use honba_core::TraceContext;
use once_cell::sync::Lazy;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, opts, register_histogram_vec,
    register_int_counter_vec, register_int_gauge_vec,
};

/// Global broker gateway metrics registry.
pub static BROKER_METRICS: Lazy<BrokerMetrics> = Lazy::new(BrokerMetrics::new);

/// Metric handles for plan §7.2.
#[derive(Debug)]
pub struct BrokerMetrics {
    /// `broker_http_request_duration_seconds{broker, endpoint, status}`
    pub http_request_duration_seconds: HistogramVec,
    /// `broker_websocket_latency_seconds{broker}`
    pub websocket_latency_seconds: HistogramVec,
    /// `broker_rate_limit_queue_depth{broker, priority}`
    pub rate_limit_queue_depth: IntGaugeVec,
    /// `broker_rate_limit_drops_total{broker}`
    pub rate_limit_drops_total: IntCounterVec,
}

impl BrokerMetrics {
    fn new() -> Self {
        Self {
            http_request_duration_seconds: register_histogram_vec!(
                HistogramOpts::new(
                    "broker_http_request_duration_seconds",
                    "Broker HTTP request round-trip duration in seconds"
                ),
                &["broker", "endpoint", "status"]
            )
            .expect("register broker_http_request_duration_seconds"),
            websocket_latency_seconds: register_histogram_vec!(
                HistogramOpts::new(
                    "broker_websocket_latency_seconds",
                    "Broker WebSocket event/processing latency in seconds"
                ),
                &["broker"]
            )
            .expect("register broker_websocket_latency_seconds"),
            rate_limit_queue_depth: register_int_gauge_vec!(
                opts!(
                    "broker_rate_limit_queue_depth",
                    "Token-bucket queue depth by broker and priority"
                ),
                &["broker", "priority"]
            )
            .expect("register broker_rate_limit_queue_depth"),
            rate_limit_drops_total: register_int_counter_vec!(
                opts!(
                    "broker_rate_limit_drops_total",
                    "Total rate-limit queue drops by broker"
                ),
                &["broker"]
            )
            .expect("register broker_rate_limit_drops_total"),
        }
    }
}

/// Record HTTP request duration with labels.
pub fn record_http_request(broker: &str, endpoint: &str, status: u16, duration: Duration) {
    BROKER_METRICS
        .http_request_duration_seconds
        .with_label_values(&[broker, endpoint, &status.to_string()])
        .observe(duration.as_secs_f64());
}

/// Record WebSocket latency with broker label.
pub fn record_websocket_latency(broker: &str, duration: Duration) {
    BROKER_METRICS
        .websocket_latency_seconds
        .with_label_values(&[broker])
        .observe(duration.as_secs_f64());
}

/// Set/update queue depth for (broker, priority).
pub fn set_queue_depth(broker: &str, priority: &str, depth: i64) {
    BROKER_METRICS
        .rate_limit_queue_depth
        .with_label_values(&[broker, priority])
        .set(depth);
}

/// Increment rate-limit drops counter.
pub fn record_rate_limit_drop(broker: &str) {
    BROKER_METRICS
        .rate_limit_drops_total
        .with_label_values(&[broker])
        .inc();
}

/// Convert canonical [`TraceContext`] into an OpenTelemetry [`SpanContext`].
pub fn span_context_from_trace_context(tc: &TraceContext) -> SpanContext {
    SpanContext::new(
        TraceId::from_bytes(tc.trace_id),
        SpanId::from_bytes(tc.span_id),
        TraceFlags::new(tc.trace_flags),
        false,
        TraceState::default(),
    )
}

/// Convert an OpenTelemetry [`SpanContext`] into canonical [`TraceContext`].
pub fn trace_context_from_span_context(sc: &SpanContext) -> TraceContext {
    TraceContext::new(
        sc.trace_id().to_bytes(),
        sc.span_id().to_bytes(),
        sc.trace_flags().to_u8(),
    )
}

/// Build standard W3C `traceparent` string from an optional [`TraceContext`].
pub fn traceparent_header_value(tc: Option<&TraceContext>) -> Option<String> {
    tc.map(|c| c.to_traceparent())
}
