//! Angel One SmartStream order-update listener
//! (`wss://tns.angelone.in/smart-order-update`).
//!
//! Per the official `SmartWebSocketOrderUpdate` reference client:
//! - the handshake carries `Authorization` (JWT), `x-api-key`, `x-client-code`
//!   and `x-feed-token` headers;
//! - the client must send the literal heartbeat text `"ping"` every 10 s to
//!   keep the socket alive;
//! - order updates arrive as JSON text frames whose numeric legs are decimal
//!   strings (`averageprice: "2568"`, `quantity: "2"`) and whose `orderid` is
//!   a JSON integer; timestamps carry time-of-day only (`09:45:11`).
//!
//! <https://smartapi.angelone.in/docs/WebSocket2>

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use serde_json::Value;
use tokio::sync::mpsc;

use honba_core::{FillReport, InstrumentId, Money, OrderAck, OrderSide, OrderStatus, Price, Quantity};
use honba_te_core::wire;
use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};

/// Default SmartStream order-update endpoint.
pub const SMART_STREAM_WS_URL: &str = "wss://tns.angelone.in/smart-order-update";

/// Heartbeat text the client sends to keep the socket alive.
pub const HEARTBEAT_MESSAGE: &str = "ping";

/// One order-update record from the SmartStream order socket. Numeric legs
/// are decimal strings on the wire (the `orderid` is a JSON integer).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AngelOrderUpdate {
    #[serde(default)]
    pub orderid: Option<Value>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub tradingsymbol: Option<String>,
    #[serde(default)]
    pub symboltoken: Option<Value>,
    #[serde(default)]
    pub producttype: Option<String>,
    #[serde(default)]
    pub transactiontype: Option<String>,
    #[serde(default)]
    pub ordertype: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub averageprice: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub triggerprice: Option<String>,
    /// Cumulative filled shares on the wire
    #[serde(default)]
    pub filledshares: Option<String>,
    #[serde(default)]
    pub quantity: Option<String>,
    /// `-1` sentinel when absent
    #[serde(default)]
    pub stoploss: Option<String>,
    /// `-1` sentinel when absent
    #[serde(default)]
    pub squareoff: Option<String>,
    /// `-1` sentinel when absent
    #[serde(default)]
    pub trailingstoploss: Option<String>,
    /// Time-of-day only: "09:45:11" (IST wall clock)
    #[serde(default)]
    pub lastmodifiedtime: Option<String>,
}

fn orderid_str(update: &AngelOrderUpdate) -> String {
    match update.orderid.as_ref() {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn side_of(update: &AngelOrderUpdate) -> OrderSide {
    match update
        .transactiontype
        .as_deref()
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "SELL" => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

fn exchange_of(update: &AngelOrderUpdate) -> honba_core::Exchange {
    match update
        .exchange
        .as_deref()
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "BSE" => honba_core::Exchange::Bse,
        "MCX" => honba_core::Exchange::Mcx,
        // NFO/BFO clear under the canonical Nse/Bse umbrella venues.
        _ => honba_core::Exchange::Nse,
    }
}

fn symbol_of(update: &AngelOrderUpdate) -> String {
    let raw = update
        .tradingsymbol
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_uppercase();
    raw.strip_suffix("-EQ")
        .or_else(|| raw.strip_suffix("-BE"))
        .map(|s| s.to_string())
        .unwrap_or(raw)
}

fn dec_str(value: Option<&str>) -> Option<rust_decimal::Decimal> {
    value?.trim().parse::<rust_decimal::Decimal>().ok()
}

/// Fill price: average execution price when nonzero, else order price.
fn fill_price_of(update: &AngelOrderUpdate) -> Price {
    let avg = dec_str(update.averageprice.as_deref())
        .filter(|d| !d.is_zero())
        .or_else(|| dec_str(update.price.as_deref()).filter(|d| !d.is_zero()));
    avg.map(Price::new).unwrap_or(Price::ZERO)
}

/// Reconstruct the IST instant from a time-of-day report, combined with the
/// IST calendar day of `now` (documented behavior: SmartStream order frames
/// carry no date).
fn timestamp_of(update: &AngelOrderUpdate, now: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    let ist_today: NaiveDate = wire::ist_today(now);
    update
        .lastmodifiedtime
        .as_deref()
        .and_then(|t| wire::parse_ist_time_of_day(t, ist_today))
        .unwrap_or(now)
}

/// Parse a SmartStream text frame into order updates.
///
/// Heartbeats (`"ping"` / `"pong"` text) yield an empty vec; malformed JSON
/// fails the stream; `orderid`-less records are skipped by the stream layer.
pub fn parse_order_update_frame(text: &str) -> Result<Vec<AngelOrderUpdate>, BrokerError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    // Heartbeat frames are plain text, not JSON.
    if trimmed.eq_ignore_ascii_case(HEARTBEAT_MESSAGE) || trimmed.eq_ignore_ascii_case("pong") {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_str(trimmed)
        .map_err(|e| BrokerError::InvalidRequest(format!("smartstream frame not JSON: {e}")))?;
    match value {
        Value::Array(items) => items
            .iter()
            .cloned()
            .map(|v| {
                serde_json::from_value::<AngelOrderUpdate>(v)
                    .map_err(|e| BrokerError::InvalidRequest(format!("smartstream item: {e}")))
            })
            .collect(),
        Value::Object(_) => {
            Ok(vec![serde_json::from_value(value).map_err(|e| {
                BrokerError::InvalidRequest(format!("smartstream frame: {e}"))
            })?])
        }
        _ => Err(BrokerError::InvalidRequest(
            "smartstream frame must be an object or array".into(),
        )),
    }
}

/// SmartStream order-feed [`ExecutionStream`] with cumulative-fill delta
/// tracking and injectable order-id → client-correlation lookup.
pub struct SmartStreamOrderStream {
    frame_rx: mpsc::UnboundedReceiver<String>,
    pending: VecDeque<BrokerExecutionEvent>,
    fills: HashMap<String, u64>,
    client_order_id_of: SmartCorrelationLookup,
}

/// Lookup function type for broker→client correlation resolution.
pub type SmartCorrelationLookup = Arc<dyn Fn(&str) -> Option<uuid::Uuid> + Send + Sync>;

impl SmartStreamOrderStream {
    /// Stream fed by text frames from a channel.
    pub fn new(frame_rx: mpsc::UnboundedReceiver<String>) -> Self {
        Self {
            frame_rx,
            pending: VecDeque::new(),
            fills: HashMap::new(),
            client_order_id_of: Arc::new(|_| None),
        }
    }

    /// Inject a broker-order-id → client-order-id resolver (nil marker when
    /// unknown).
    pub fn with_correlation_lookup(
        mut self,
        lookup: impl Fn(&str) -> Option<uuid::Uuid> + Send + Sync + 'static,
    ) -> Self {
        self.client_order_id_of = Arc::new(lookup);
        self
    }

    fn dispatch(
        &mut self,
        update: AngelOrderUpdate,
        now: chrono::DateTime<Utc>,
    ) -> Option<BrokerExecutionEvent> {
        let order_id = orderid_str(&update);
        if order_id.is_empty() || order_id == "0" {
            return None;
        }
        let client_order_id = (self.client_order_id_of)(&order_id).unwrap_or(uuid::Uuid::nil());
        let status = update.status.as_deref().unwrap_or_default().to_uppercase();
        match status.as_str() {
            "OPEN" | "NEW" | "MODIFIED" => Some(BrokerExecutionEvent::OrderAck(OrderAck {
                order_id,
                client_order_id,
                exchange_order_id: None,
                status: OrderStatus::New,
                timestamp: timestamp_of(&update, now),
                correlation_id: None,
                client_id: None,
            })),
            "TRIGGER PENDING" | "VALIDATION PENDING" | "PUT ORDER REQ RECEIVED" => {
                Some(BrokerExecutionEvent::OrderAck(OrderAck {
                    order_id,
                    client_order_id,
                    exchange_order_id: None,
                    status: OrderStatus::PendingNew,
                    timestamp: timestamp_of(&update, now),
                    correlation_id: None,
                    client_id: None,
                }))
            }
            "COMPLETE" => {
                let filled = dec_str(update.filledshares.as_deref())
                    .unwrap_or(rust_decimal::Decimal::ZERO)
                    .max(rust_decimal::Decimal::ZERO);
                let filled = filled.try_into().unwrap_or(0u64);
                let prev = self.fills.insert(order_id.clone(), filled).unwrap_or(0);
                if filled <= prev {
                    return None;
                }
                Some(BrokerExecutionEvent::Fill(FillReport {
                    fill_id: format!("{order_id}:{filled}"),
                    order_id,
                    client_order_id,
                    instrument: InstrumentId::new(exchange_of(&update), symbol_of(&update)),
                    side: side_of(&update),
                    price: fill_price_of(&update),
                    quantity: Quantity::from_u64(filled - prev),
                    // SmartStream order frames carry no SEBI tag payload.
                    algo_id: String::new(),
                    timestamp: timestamp_of(&update, now),
                    correlation_id: None,
                    client_id: None,
                    fee: Money::zero_inr(),
                    fee_breakdown: None,
                }))
            }
            "REJECTED" => Some(BrokerExecutionEvent::OrderRejected {
                order_id: Some(order_id),
                reason: "rejected by broker".into(),
            }),
            "CANCELLED" => Some(BrokerExecutionEvent::OrderCancelled {
                order_id,
                reason: None,
            }),
            _ => None,
        }
    }
}

#[async_trait]
impl ExecutionStream for SmartStreamOrderStream {
    async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            let frame = self
                .frame_rx
                .recv()
                .await
                .ok_or_else(|| BrokerError::Transport("smartstream closed".into()))?;
            let now = Utc::now();
            for update in parse_order_update_frame(&frame)? {
                if let Some(event) = self.dispatch(update, now) {
                    self.pending.push_back(event);
                }
            }
        }
    }
}

/// SmartStream connection behaviour knobs (defaults follow the reference
/// client: 10 s heartbeat, 2 retry attempts with 10 s delay).
#[derive(Debug, Clone)]
pub struct SmartStreamConfig {
    /// JWT session token (`Authorization` header, `Bearer …` included).
    pub auth_token: String,
    /// SmartAPI key (`x-api-key` / `X-PrivateKey`).
    pub api_key: String,
    /// Client code (`x-client-code`).
    pub client_code: String,
    /// Feed token (`x-feed-token`, issued with the login feed bundle).
    pub feed_token: String,
    /// Heartbeat interval (client sends the literal `ping` text).
    pub heartbeat: Duration,
    /// Connection attempts per epoch.
    pub max_attempts: u32,
    /// Initial reconnect delay (doubles up to `max_backoff`).
    pub backoff: Duration,
    /// Reconnect delay cap.
    pub max_backoff: Duration,
}

impl SmartStreamConfig {
    /// Config with the reference-client defaults.
    pub fn new(
        auth_token: impl Into<String>,
        api_key: impl Into<String>,
        client_code: impl Into<String>,
        feed_token: impl Into<String>,
    ) -> Self {
        Self {
            auth_token: auth_token.into(),
            api_key: api_key.into(),
            client_code: client_code.into(),
            feed_token: feed_token.into(),
            heartbeat: Duration::from_secs(10),
            max_attempts: 8,
            backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Connect to the SmartStream order socket and spawn the read loop.
///
/// The returned stream terminates with `BrokerError::Transport` when all
/// reconnect attempts are exhausted.
pub async fn spawn_order_updates(
    auth_token: &str,
    api_key: &str,
    client_code: &str,
    feed_token: &str,
) -> Result<SmartStreamOrderStream, BrokerError> {
    spawn_order_updates_with(SmartStreamConfig::new(
        auth_token,
        api_key,
        client_code,
        feed_token,
    ))
    .await
}

/// Same as [`spawn_order_updates`] with explicit connection knobs.
pub async fn spawn_order_updates_with(
    config: SmartStreamConfig,
) -> Result<SmartStreamOrderStream, BrokerError> {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), BrokerError>>();

    tokio::spawn(async move {
        let mut delay = config.backoff;
        let mut ready = Some(ready_tx);
        for attempt in 1..=config.max_attempts {
            let connect = || async {
                let mut request = SMART_STREAM_WS_URL.into_client_request().map_err(|e| {
                    BrokerError::Transport(format!("smartstream request build: {e}"))
                })?;
                let headers = request.headers_mut();
                let auth = format!("Bearer {}", config.auth_token.trim_start_matches("Bearer "));
                headers.insert(
                    "Authorization",
                    auth.parse()
                        .map_err(|e| BrokerError::Auth(format!("smartstream auth header: {e}")))?,
                );
                for (name, value) in [
                    ("x-api-key", config.api_key.as_str()),
                    ("x-client-code", config.client_code.as_str()),
                    ("x-feed-token", config.feed_token.as_str()),
                ] {
                    headers.insert(
                        name,
                        value
                            .parse()
                            .map_err(|e| BrokerError::Auth(format!("smartstream {name}: {e}")))?,
                    );
                }
                match tokio_tungstenite::connect_async(request).await {
                    Ok((ws, _)) => Ok(ws),
                    Err(err) => Err(BrokerError::Transport(format!(
                        "smartstream connect failed: {err}"
                    ))),
                }
            };
            let socket = match connect().await {
                Ok(ws) => ws,
                Err(err) => {
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(Err(err));
                    }
                    tracing::warn!(
                        attempt,
                        "smartstream connect failed; retrying in {:?}",
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
            let mut heartbeat = tokio::time::interval(config.heartbeat);
            let mut connected = true;
            while connected {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        // Literal text heartbeat per the reference client.
                        if write
                            .send(Message::Text(HEARTBEAT_MESSAGE.into()))
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
                                    connected = false;
                                }
                            }
                            Some(Ok(Message::Ping(data))) => {
                                // Reflect application-level pongs the server expects.
                                let _ = write
                                    .send(Message::Pong(data))
                                    .await;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(err)) => {
                                tracing::warn!(%err, "smartstream read error");
                                connected = false;
                            }
                            None => connected = false,
                        }
                    }
                }
            }
            delay = (delay * 2).min(config.max_backoff);
            tokio::time::sleep(delay).await;
        }
        drop(tx);
    });

    match ready_rx.await {
        Ok(Ok(())) => Ok(SmartStreamOrderStream::new(rx)),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(BrokerError::Transport("smartstream connector died".into())),
    }
}
