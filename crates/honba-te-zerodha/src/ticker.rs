//! Kite Ticker order-update stream (`wss://ws.kite.trade`).
//!
//! Kite Ticker emits order updates as **text JSON frames** shaped
//! `{"type":"order","data":[{...}]}`; binary frames carry market-data ticks
//! and belong to the honba-me "eye", not to execution. This module therefore
//! exposes:
//!
//! - [`parse_order_update_frame`] — pure text-frame → updates decoder
//! - [`KiteTickerOrderStream`] — [`ExecutionStream`] over a text-frame
//!   channel with cumulative-fill delta tracking and injectable order-id →
//!   client-correlation lookup
//! - [`spawn_order_updates`] — live WebSocket connector with retry/backoff
//!   and keepalive pings
//!
//! <https://kite.trade/docs/connect/v3/websocket/>

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;

use honba_core::{FillReport, InstrumentId, OrderAck, OrderSide, OrderStatus, Price, Quantity};
use honba_te_core::wire;
use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};

/// Default Kite Ticker endpoint.
pub const TICKER_WS_URL: &str = "wss://ws.kite.trade";

/// One order-update record from a Kite Ticker `type: "order"` frame.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KiteTickerOrderUpdate {
    pub order_id: String,
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub tradingsymbol: Option<String>,
    #[serde(default)]
    pub transaction_type: Option<String>,
    #[serde(default)]
    pub order_type: Option<String>,
    #[serde(default)]
    pub product: Option<String>,
    pub status: String,
    #[serde(default)]
    pub status_message: Option<String>,
    /// Ordered quantity (float on the wire)
    #[serde(default)]
    pub quantity: Option<Value>,
    /// Cumulative filled quantity (float on the wire)
    #[serde(default)]
    pub filled_quantity: Option<Value>,
    /// Order limit price
    #[serde(default)]
    pub price: Option<Value>,
    /// Average execution price
    #[serde(default)]
    pub average_price: Option<Value>,
    /// Trigger level for SL/CO legs
    #[serde(default)]
    pub trigger_at: Option<Value>,
    /// SEBI tag placed by the client (our algo_id)
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub placed_by: Option<String>,
    /// "2019-12-19T14:32:39" (IST naive) or null
    #[serde(default)]
    pub exchange_timestamp: Option<String>,
    /// Legacy timestamp field (IST naive)
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// Parse a Kite Ticker text frame into order updates.
///
/// `type: "order"` frames yield updates (object or array payloads);
/// `type: "error"` frames fail the stream; any other frame type (market data
/// pings, unknown) yields an empty vec so execution-only consumers skip it.
pub fn parse_order_update_frame(text: &str) -> Result<Vec<KiteTickerOrderUpdate>, BrokerError> {
    let value: Value = serde_json::from_str(text)
        .map_err(|e| BrokerError::InvalidRequest(format!("kite ticker frame not JSON: {e}")))?;
    match value.get("type").and_then(Value::as_str) {
        Some("order") => {
            let data = value.get("data").cloned().unwrap_or(Value::Null);
            let updates: Vec<KiteTickerOrderUpdate> = match data {
                Value::Array(_) => serde_json::from_value(data),
                Value::Object(_) => {
                    serde_json::from_value::<KiteTickerOrderUpdate>(data).map(|u| vec![u])
                }
                _ => Ok(Vec::new()),
            }
            .map_err(|e| BrokerError::InvalidRequest(format!("kite order frame malformed: {e}")))?;
            Ok(updates)
        }
        Some("error") => Err(BrokerError::Transport(format!(
            "kite ticker error frame: {}",
            value
                .get("data")
                .map(|d| d.to_string())
                .unwrap_or_else(|| "unknown".into())
        ))),
        Some(_) => Ok(Vec::new()),
        None => Err(BrokerError::InvalidRequest(
            "kite ticker frame missing `type`".into(),
        )),
    }
}

fn ticker_status_event(status: &str) -> Option<OrderStatus> {
    match status.to_uppercase().as_str() {
        "OPEN" => Some(OrderStatus::New),
        "OPEN PENDING"
        | "PUT ORDER REQ RECEIVED"
        | "VALIDATION PENDING"
        | "AMO QUEUE PENDING"
        | "TRIGGER PENDING"
        | "AMO CANCEL PENDING" => Some(OrderStatus::PendingNew),
        _ => None,
    }
}

fn ticker_instrument(update: &KiteTickerOrderUpdate) -> InstrumentId {
    let exchange = match update
        .exchange
        .as_deref()
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "BSE" => honba_core::Exchange::Bse,
        "MCX" => honba_core::Exchange::Mcx,
        _ => honba_core::Exchange::Nse,
    };
    InstrumentId::new(exchange, update.tradingsymbol.clone().unwrap_or_default())
}

fn ticker_side(update: &KiteTickerOrderUpdate) -> OrderSide {
    match update
        .transaction_type
        .as_deref()
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn ticker_timestamp(update: &KiteTickerOrderUpdate) -> chrono::DateTime<Utc> {
    (update
        .exchange_timestamp
        .as_deref()
        .or(update.timestamp.as_deref())
        .and_then(wire::parse_broker_ist_datetime))
    .unwrap_or_else(Utc::now)
}

fn ticker_price_of(update: &KiteTickerOrderUpdate) -> Price {
    let avg = update
        .average_price
        .as_ref()
        .and_then(wire::price_from_json)
        .filter(|p| !p.0.is_zero());
    if let Some(p) = avg {
        return p;
    }
    update
        .price
        .as_ref()
        .and_then(wire::price_from_json)
        .filter(|p| !p.0.is_zero())
        .unwrap_or(Price::ZERO)
}

/// Lookup function mapping broker order ids to client correlation ids.
pub type CorrelationLookup = Arc<dyn Fn(&str) -> Option<uuid::Uuid> + Send + Sync>;

/// Kite Ticker order-feed [`ExecutionStream`].
///
/// Consumes text frames pushed by any transport (see [`spawn_order_updates`]
/// for the production WebSocket connector). Fill events carry the *delta* of
/// cumulative `filled_quantity` between consecutive updates, so OMS
/// aggregation across partial fills stays correct.
pub struct KiteTickerOrderStream {
    frame_rx: mpsc::UnboundedReceiver<String>,
    pending: VecDeque<BrokerExecutionEvent>,
    fills: HashMap<String, u64>,
    client_order_id_of: CorrelationLookup,
}

impl KiteTickerOrderStream {
    /// Stream fed by text frames from a channel.
    pub fn new(frame_rx: mpsc::UnboundedReceiver<String>) -> Self {
        Self {
            frame_rx,
            pending: VecDeque::new(),
            fills: HashMap::new(),
            client_order_id_of: Arc::new(|_| None),
        }
    }

    /// Inject a lookup mapping broker order ids to client correlation ids
    /// (e.g. fed by the OMS at placement time). Unmatched ids keep the nil
    /// correlation marker.
    pub fn with_correlation_lookup(
        mut self,
        lookup: impl Fn(&str) -> Option<uuid::Uuid> + Send + Sync + 'static,
    ) -> Self {
        self.client_order_id_of = Arc::new(lookup);
        self
    }

    fn dispatch(&mut self, update: KiteTickerOrderUpdate) -> Option<BrokerExecutionEvent> {
        let client_order_id =
            (self.client_order_id_of)(&update.order_id).unwrap_or(uuid::Uuid::nil());
        let status_upper = update.status.to_uppercase();
        if let Some(st) = ticker_status_event(&status_upper) {
            return Some(BrokerExecutionEvent::OrderAck(OrderAck {
                order_id: update.order_id.clone(),
                client_order_id,
                exchange_order_id: update.exchange_order_id.clone(),
                status: st,
                timestamp: ticker_timestamp(&update),
                correlation_id: None,
                client_id: None,
            }));
        }
        match status_upper.as_str() {
            "COMPLETE" | "UPDATE" => {
                let filled = update
                    .filled_quantity
                    .as_ref()
                    .and_then(wire::integer_from_json)
                    .unwrap_or(0)
                    .max(0) as u64;
                let prev = self
                    .fills
                    .insert(update.order_id.clone(), filled)
                    .unwrap_or(0);
                if filled <= prev {
                    return None;
                }
                Some(BrokerExecutionEvent::Fill(FillReport {
                    fill_id: format!("{}:{filled}", update.order_id),
                    order_id: update.order_id.clone(),
                    client_order_id,
                    instrument: ticker_instrument(&update),
                    side: ticker_side(&update),
                    price: ticker_price_of(&update),
                    quantity: Quantity::from_u64(filled - prev),
                    algo_id: update.tag.clone().unwrap_or_default(),
                    timestamp: ticker_timestamp(&update),
                    correlation_id: None,
                    client_id: None,
                }))
            }
            "REJECTED" => Some(BrokerExecutionEvent::OrderRejected {
                order_id: Some(update.order_id.clone()),
                reason: update
                    .status_message
                    .clone()
                    .unwrap_or_else(|| "rejected by broker".into()),
            }),
            "CANCELLED" | "AMO CANCELLED" => Some(BrokerExecutionEvent::OrderCancelled {
                order_id: update.order_id.clone(),
                reason: update.status_message.clone(),
            }),
            _ => None,
        }
    }
}

#[async_trait]
impl ExecutionStream for KiteTickerOrderStream {
    async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            let frame = self
                .frame_rx
                .recv()
                .await
                .ok_or_else(|| BrokerError::Transport("kite ticker stream closed".into()))?;
            for update in parse_order_update_frame(&frame)? {
                if let Some(event) = self.dispatch(update) {
                    self.pending.push_back(event);
                }
            }
        }
    }
}

/// Ticker WebSocket connection behaviour knobs.
#[derive(Debug, Clone)]
pub struct TickerConnectConfig {
    /// Connection attempts per epoch (each epoch spans one successful
    /// connect + full lifetime; a retry epoch begins on disconnect).
    pub max_attempts: u32,
    /// Initial reconnect delay (doubles up to [`max_backoff`]).
    pub backoff: Duration,
    /// Reconnect delay cap.
    pub max_backoff: Duration,
    /// Application-level ping interval. Kite Ticker drops idle connections;
    /// the connector pings the server on this cadence.
    pub keepalive: Duration,
}

impl Default for TickerConnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(30),
            keepalive: Duration::from_secs(5),
        }
    }
}

/// Connect to Kite Ticker for order updates and spawn the read loop.
///
/// The returned stream terminates with `BrokerError::Transport` when all
/// reconnect attempts are exhausted and the channel is closed. Kite order
/// updates flow on any connection (no subscription required for the
/// `order` frame type).
pub async fn spawn_order_updates(
    api_key: &str,
    access_token: &str,
) -> Result<KiteTickerOrderStream, BrokerError> {
    spawn_order_updates_with(api_key, access_token, TickerConnectConfig::default()).await
}

/// Same as [`spawn_order_updates`] with explicit connection knobs.
pub async fn spawn_order_updates_with(
    api_key: &str,
    access_token: &str,
    config: TickerConnectConfig,
) -> Result<KiteTickerOrderStream, BrokerError> {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), BrokerError>>();

    let api_key = api_key.to_string();
    let access_token = access_token.to_string();
    tokio::spawn(async move {
        let mut delay = config.backoff;
        let mut ready = Some(ready_tx);
        for attempt in 1..=config.max_attempts {
            // Kite requires a unique connection id; reuse is rejected.
            let uid = uuid::Uuid::new_v4().simple().to_string();
            let url =
                format!("{TICKER_WS_URL}?api_key={api_key}&access_token={access_token}&uid={uid}");
            let socket = match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => ws,
                Err(err) => {
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Err(BrokerError::Transport(format!(
                            "kite ticker connect failed: {err}"
                        ))));
                    }
                    tracing::warn!(
                        attempt,
                        "kite ticker connect failed: {err}; retrying in {:?}",
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(config.max_backoff);
                    continue;
                }
            };
            if let Some(ready) = ready.take() {
                let _ = ready.send(Ok(()));
            }
            let (mut write, mut read) = socket.split();
            let mut keepalive = tokio::time::interval(config.keepalive);
            let mut connected = true;
            while connected {
                tokio::select! {
                    _ = keepalive.tick() => {
                        if write
                            .send(Message::Ping(b"honba".as_slice().into()))
                            .await
                            .is_err()
                        {
                            connected = false;
                        }
                    }
                    frame = read.next() => {
                        match frame {
                            Some(Ok(Message::Text(text))) => {
                                if tx.send(text.to_string()).is_err() {
                                    // Consumer dropped: stop reading, close socket.
                                    connected = false;
                                }
                            }
                            Some(Ok(_)) => {
                                // Binary market-data ticks, Ping/Pong, Close, raw frames — no execution content.
                            }
                            Some(Err(err)) => {
                                tracing::warn!(%err, "kite ticker read error");
                                connected = false;
                            }
                            None => connected = false,
                        }
                    }
                }
            }
            if !connected {
                // Retry epoch with capped doubling backoff.
                delay = (delay * 2).min(config.max_backoff);
                tokio::time::sleep(delay).await;
            }
        }
        // Exhausted — close the frame channel; consumers surface Transport.
        drop(tx);
    });

    match ready_rx.await {
        Ok(Ok(())) => Ok(KiteTickerOrderStream::new(rx)),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(BrokerError::Transport("kite ticker connector died".into())),
    }
}
