//! Zerodha Kite postback (webhook) receiver.
//!
//! Kite Connect POSTs a JSON payload to the app's registered `postback_url`
//! when an order's status changes (`COMPLETE`, `REJECTED`, `CANCELLED`,
//! `UPDATE`). Per the Kite Connect postback contract:
//!
//! > `checksum` is the SHA-256 hash of (`order_id` + `order_timestamp` +
//! > `api_secret`).
//!
//! The listener verifies that digest (constant-time compare), tracks
//! cumulative `filled_quantity` per order, and emits canonical
//! [`BrokerExecutionEvent`]s into an [`ExecutionStream`] fed over mpsc.
//!
//! <https://kite.trade/docs/connect/v3/postback/>

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::Utc;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use honba_core::{FillReport, InstrumentId, Money, OrderSide, Price, Quantity};
use honba_te_auth::crypto::sha256_hex;
use honba_te_core::{
    BrokerError, BrokerExecutionEvent, ChannelExecutionStream, ExecutionEventSink,
};

/// Default postback route path.
pub const POSTBACK_DEFAULT_PATH: &str = "/postback";

/// Postback listener configuration.
#[derive(Debug, Clone)]
pub struct PostbackConfig {
    /// Bind address of the webhook server.
    pub addr: SocketAddr,
    /// Route path Kite posts to (defaults to [`POSTBACK_DEFAULT_PATH`]).
    pub path: String,
    /// Kite Connect API secret used for checksum verification.
    pub api_secret: String,
}

impl PostbackConfig {
    /// Config with the default `/postback` route path.
    pub fn new(addr: SocketAddr, api_secret: impl Into<String>) -> Self {
        Self {
            addr,
            path: POSTBACK_DEFAULT_PATH.to_string(),
            api_secret: api_secret.into(),
        }
    }
}

/// Compute the Kite postback checksum: SHA-256 hex of
/// `order_id + order_timestamp + api_secret` (lowercase hex).
pub fn postback_checksum(api_secret: &str, order_id: &str, order_timestamp: &str) -> String {
    let input = format!("{order_id}{order_timestamp}{api_secret}");
    sha256_hex(input.as_bytes())
}

/// Constant-time verification of the postback checksum.
///
/// Reveals no timing information about how many leading hex characters
/// matched; mismatches or malformed input map to `false`.
pub fn verify_postback_checksum(
    api_secret: &str,
    order_id: &str,
    order_timestamp: &str,
    checksum: &str,
) -> bool {
    let expected = postback_checksum(api_secret, order_id, order_timestamp);
    ct_bytes_equal(
        &expected.into_bytes(),
        &checksum.to_lowercase().into_bytes(),
    )
}

/// Length-checked constant-time byte equality (XOR accumulation).
const fn ct_bytes_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    let mut i = 0;
    while i < a.len() {
        acc |= a[i] ^ b[i];
        i += 1;
    }
    acc == 0
}

/// Kite postback webhook payload (required digest fields mandatory; the rest
/// tolerant because the exchange attaches `meta`/`guid` freely).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KitePostback {
    /// Unique order ID
    pub order_id: String,
    /// Exchange-generated order id (orders that never reach the exchange: null)
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    /// COMPLETE | REJECTED | CANCELLED | UPDATE
    #[serde(default)]
    pub status: Option<String>,
    /// Checksum input part 2: timestamp at which the order was registered
    pub order_timestamp: String,
    /// Cumulative filled quantity
    #[serde(default)]
    pub filled_quantity: Option<Value>,
    /// Ordered quantity
    #[serde(default)]
    pub quantity: Option<Value>,
    /// Average execution price (float on the wire)
    #[serde(default)]
    pub average_price: Option<Value>,
    /// Order price (LIMIT orders)
    #[serde(default)]
    pub price: Option<Value>,
    /// Exchange tradingsymbol
    #[serde(default)]
    pub tradingsymbol: Option<String>,
    /// Exchange segment
    #[serde(default)]
    pub exchange: Option<String>,
    /// BUY or SELL
    #[serde(default)]
    pub transaction_type: Option<String>,
    /// Margin product (CNC/MIS/...)
    #[serde(default)]
    pub product: Option<String>,
    /// SEBI algo tag placed with the order (our canonical algo_id)
    #[serde(default)]
    pub tag: Option<String>,
    /// SHA-256 digest of (order_id + order_timestamp + api_secret)
    #[serde(default)]
    pub checksum: Option<String>,
    /// Human-readable rejection/status message
    #[serde(default)]
    pub status_message: Option<String>,
    /// Raw status message from the OMS
    #[serde(default)]
    pub status_message_raw: Option<String>,
}

#[derive(Debug, Clone)]
struct PostbackState {
    api_secret: String,
    events: ExecutionEventSink,
    /// cumulative filled quantity per broker order id (for UPDATE deltas)
    fills: Arc<Mutex<HashMap<String, u64>>>,
}

/// Zerodha postback listener: spawns an axum server and hands back a
/// push-ordered [`ExecutionStream`] of broker events.
pub struct PostbackListener;

impl PostbackListener {
    /// Bind the listener and start serving postbacks in a background task.
    ///
    /// Bind failures surface synchronously; serving continues in the spawned
    /// task for the process lifetime.
    pub async fn spawn(
        config: PostbackConfig,
    ) -> Result<(ChannelExecutionStream, SocketAddr), BrokerError> {
        let PostbackConfig {
            addr,
            path,
            api_secret,
        } = config;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| BrokerError::Transport(format!("postback bind failed: {e}")))?;
        let bound = listener
            .local_addr()
            .map_err(|e| BrokerError::Transport(format!("postback local addr: {e}")))?;
        let (stream, sink) = ChannelExecutionStream::channel(4096);
        let state = Arc::new(PostbackState {
            api_secret,
            events: sink,
            fills: Arc::new(Mutex::new(HashMap::new())),
        });
        let route_path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        let app = Router::new()
            .route(
                &route_path,
                post(|state, body| async move { handle_postback(state, body).await }),
            )
            .route("/health", get(|| async { "ok" }))
            .with_state(state);
        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                tracing::error!("postback server exited: {err}");
            }
        });
        Ok((stream, bound))
    }
}

/// Axum handler: raw-body decode (single object or array of postbacks),
/// checksum verification, then event emission.
async fn handle_postback(State(state): State<Arc<PostbackState>>, body: Bytes) -> Response {
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid postback json: {e}"),
            )
                .into_response();
        }
    };
    let mut fills = state.fills.lock().await;
    let mutations = decode_postback_payload(&payload, &state.api_secret, &mut fills);
    let events = match mutations {
        Ok(events) => events,
        Err(BrokerError::InvalidRequest(msg)) => {
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
        Err(BrokerError::Auth(msg)) | Err(BrokerError::Rejected(msg)) => {
            return (StatusCode::FORBIDDEN, msg).into_response();
        }
        Err(other) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()).into_response();
        }
    };
    drop(fills);
    for event in events {
        if state.events.try_send(event).is_err() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "execution stream closed".to_string(),
            )
                .into_response();
        }
    }
    StatusCode::OK.into_response()
}

/// Decode a postback JSON body into canonical events, verifying each
/// checksum and tracking cumulative fills for delta computation.
///
/// Accepts a single payload object or an array of payloads (Kite sends
/// batched arrays when several status changes collapse).
pub fn decode_postback_payload(
    payload: &Value,
    api_secret: &str,
    fills: &mut HashMap<String, u64>,
) -> Result<Vec<BrokerExecutionEvent>, BrokerError> {
    let items: Vec<&Value> = match payload {
        Value::Array(items) => items.iter().collect(),
        Value::Object(_) => vec![payload],
        _ => {
            return Err(BrokerError::InvalidRequest(
                "postback payload must be an object or array".into(),
            ));
        }
    };
    let mut events = Vec::new();
    for item in items {
        let pb: KitePostback = serde_json::from_value(item.clone())
            .map_err(|e| BrokerError::InvalidRequest(format!("malformed postback: {e}")))?;
        verify_postback(&pb, api_secret)?;
        if let Some(event) = postback_to_event(&pb, fills) {
            events.push(event);
        }
    }
    Ok(events)
}

fn verify_postback(pb: &KitePostback, api_secret: &str) -> Result<(), BrokerError> {
    let checksum = pb
        .checksum
        .as_deref()
        .ok_or_else(|| BrokerError::InvalidRequest("postback missing checksum".into()))?;
    if !verify_postback_checksum(api_secret, &pb.order_id, &pb.order_timestamp, checksum) {
        return Err(BrokerError::Auth("postback checksum mismatch".into()));
    }
    Ok(())
}

/// Translate one verified postback into zero or one canonical event.
fn postback_to_event(
    pb: &KitePostback,
    fills: &mut HashMap<String, u64>,
) -> Option<BrokerExecutionEvent> {
    let status = pb.status.as_deref().unwrap_or_default().to_uppercase();
    let reason = pb
        .status_message
        .clone()
        .or_else(|| pb.status_message_raw.clone());
    match status.as_str() {
        "COMPLETE" | "UPDATE" => {
            let filled = pb
                .filled_quantity
                .as_ref()
                .and_then(honba_te_core::wire::integer_from_json)
                .unwrap_or(0);
            let filled = filled.max(0) as u64;
            let prev = fills.insert(pb.order_id.clone(), filled).unwrap_or(0);
            if filled <= prev {
                // UPDATE on modification with no new fill — nothing to report.
                return None;
            }
            let delta = filled - prev;
            Some(BrokerExecutionEvent::Fill(FillReport {
                fill_id: format!("{}:{filled}", pb.order_id),
                order_id: pb.order_id.clone(),
                // Postbacks carry no client correlation id; the OMS resolves
                // ownership by broker order id.
                client_order_id: uuid::Uuid::nil(),
                instrument: InstrumentId::new(
                    exchange_of(pb.exchange.as_deref()),
                    pb.tradingsymbol.clone().unwrap_or_default(),
                ),
                side: side_of(pb.transaction_type.as_deref()),
                price: position_price(pb.average_price.as_ref(), pb.price.as_ref()),
                quantity: Quantity::from_u64(delta),
                algo_id: pb.tag.clone().unwrap_or_default(),
                timestamp: wire_time(&pb.order_timestamp),
                correlation_id: None,
                client_id: None,
                fee: Money::zero_inr(),
                fee_breakdown: None,
                trace_context: None,
            }))
        }
        "REJECTED" => Some(BrokerExecutionEvent::OrderRejected {
            order_id: Some(pb.order_id.clone()),
            reason: reason.unwrap_or_else(|| "rejected by broker".into()),
        }),
        "CANCELLED" => Some(BrokerExecutionEvent::OrderCancelled {
            order_id: pb.order_id.clone(),
            reason,
        }),
        _ => None,
    }
}

fn exchange_of(exchange: Option<&str>) -> honba_core::Exchange {
    match exchange.unwrap_or_default().to_uppercase().as_str() {
        "BSE" => honba_core::Exchange::Bse,
        "MCX" => honba_core::Exchange::Mcx,
        _ => honba_core::Exchange::Nse,
    }
}

fn side_of(transaction_type: Option<&str>) -> OrderSide {
    match transaction_type.unwrap_or_default().to_uppercase().as_str() {
        "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

/// Fill price: average execution price when filled, else limit price when
/// nonzero (market fills never quote an order price).
fn position_price(average: Option<&Value>, limit: Option<&Value>) -> Price {
    let avg = average.and_then(honba_te_core::wire::price_from_json);
    if let Some(p) = avg {
        if p.0.is_zero() {
            return limit
                .and_then(honba_te_core::wire::price_from_json)
                .unwrap_or(Price::ZERO);
        }
        return p;
    }
    Price::ZERO
}

fn wire_time(timestamp: &str) -> chrono::DateTime<Utc> {
    honba_te_core::wire::parse_broker_ist_datetime(timestamp).unwrap_or_else(Utc::now)
}
