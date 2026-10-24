//! Upstox portfolio RSS WebSocket order feed (`/v2/feed/portfolio-stream-feed`).
//!
//! The feed pushes JSON order-update objects (`update_type: "order"`) with
//! float-typed prices on the wire; decoding keeps values exact through
//! serde_json's shortest textual round-trip. A live connector handles the
//! documented authorization redirect: the handshake responds `302` with a
//! `Location` header pointing at the dedicated feed host, which is followed
//! manually (max hops bounded).
//!
//! <https://upstox.com/developer/api-documentation/get-portfolio-stream-feed/>

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;

use honba_core::{FillReport, InstrumentId, Money, OrderAck, OrderSide, OrderStatus, Price, Quantity};
use honba_te_core::wire;
use honba_te_core::{BrokerError, BrokerExecutionEvent, ExecutionStream};

/// Default portfolio-stream-feed endpoint (base host carries the redirect).
pub const UPSTOX_WS_URL: &str = "wss://ws.upstox.com/v2/feed/portfolio-stream-feed";

/// One order update from the portfolio-stream feed.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpstoxFeedUpdate {
    /// "order" (stream defaults to order updates only)
    #[serde(default)]
    pub update_type: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    /// `NSE_EQ|INE...` instrument key
    #[serde(default)]
    pub instrument_token: Option<String>,
    #[serde(default, alias = "trading_symbol")]
    pub tradingsymbol: Option<String>,
    /// isin when present
    #[serde(default)]
    pub isin: Option<String>,
    #[serde(default)]
    pub product: Option<String>,
    #[serde(default)]
    pub order_type: Option<String>,
    #[serde(default)]
    pub average_price: Option<Value>,
    #[serde(default)]
    pub price: Option<Value>,
    #[serde(default)]
    pub trigger_price: Option<Value>,
    #[serde(default)]
    pub quantity: Option<Value>,
    #[serde(default)]
    pub disclosed_quantity: Option<Value>,
    #[serde(default)]
    pub filled_quantity: Option<Value>,
    #[serde(default)]
    pub transaction_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub is_amo: Option<bool>,
    #[serde(default)]
    pub variety: Option<String>,
    /// SEBI tag (our algo_id)
    #[serde(default)]
    pub tag: Option<String>,
    /// Naive IST wall clock: "2024-02-21 14:40:02"
    #[serde(default)]
    pub exchange_timestamp: Option<String>,
    #[serde(default)]
    pub status_message: Option<String>,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub order_request_id: Option<String>,
    #[serde(default)]
    pub parent_order_id: Option<String>,
}

/// Parse a portfolio-stream text frame into order updates.
///
/// Frames carry a single object; unknown `update_type`s yield an empty vec,
/// malformed JSON fails the stream.
pub fn parse_portfolio_feed_frame(text: &str) -> Result<Vec<UpstoxFeedUpdate>, BrokerError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_str(trimmed)
        .map_err(|e| BrokerError::InvalidRequest(format!("upstox feed frame not JSON: {e}")))?;

    match value {
        Value::Array(items) => {
            let updates = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<UpstoxFeedUpdate>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    BrokerError::InvalidRequest(format!("upstox feed item malformed: {e}"))
                })?;
            Ok(updates
                .into_iter()
                .filter(|u| u.update_type.as_deref() == Some("order"))
                .collect())
        }
        Value::Object(_) => {
            let update_type = value.get("update_type").and_then(Value::as_str);
            if update_type.map(|t| t != "order").unwrap_or(false) {
                return Ok(Vec::new());
            }
            let update: UpstoxFeedUpdate = serde_json::from_value(value).map_err(|e| {
                BrokerError::InvalidRequest(format!("upstox feed frame malformed: {e}"))
            })?;
            Ok(vec![update])
        }
        _ => Err(BrokerError::InvalidRequest(
            "upstox feed frame must be an object or array".into(),
        )),
    }
}

fn feed_instrument(update: &UpstoxFeedUpdate) -> InstrumentId {
    let exchange = match update
        .instrument_token
        .as_deref()
        .unwrap_or_default()
        .split('|')
        .next()
        .unwrap_or_default()
        .to_uppercase()
        .as_str()
    {
        "BSE_EQ" | "BSE_FO" => honba_core::Exchange::Bse,
        "MCX_FO" | "MCX_EQ" => honba_core::Exchange::Mcx,
        _ => honba_core::Exchange::Nse,
    };
    let symbol = update
        .tradingsymbol
        .as_deref()
        .map(|s| s.to_uppercase())
        .or_else(|| update.isin.clone().map(|i| i.to_uppercase()))
        .map(|s| {
            s.strip_suffix("-EQ")
                .or_else(|| s.strip_suffix("-BE"))
                .map(|t| t.to_string())
                .unwrap_or(s)
        })
        .unwrap_or_default();
    InstrumentId::new(exchange, symbol)
}

fn feed_side(update: &UpstoxFeedUpdate) -> OrderSide {
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

fn feed_timestamp(update: &UpstoxFeedUpdate) -> chrono::DateTime<Utc> {
    (update
        .exchange_timestamp
        .as_deref()
        .and_then(wire::parse_broker_ist_datetime))
    .unwrap_or_else(Utc::now)
}

fn feed_fill_price(update: &UpstoxFeedUpdate) -> Price {
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

/// Upstox order-feed [`ExecutionStream`].
///
/// Delta-tracks cumulative `filled_quantity` exactly like the Zerodha ticker
/// stream, and supports injectable broker-order-id → client-correlation
/// resolution.
/// Lookup function type for broker→client correlation resolution.
pub type FeedCorrelationLookup = Arc<dyn Fn(&str) -> Option<uuid::Uuid> + Send + Sync>;

pub struct UpstoxOrderStream {
    frame_rx: mpsc::UnboundedReceiver<String>,
    pending: VecDeque<BrokerExecutionEvent>,
    fills: HashMap<String, u64>,
    client_order_id_of: FeedCorrelationLookup,
}

impl UpstoxOrderStream {
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
    fn dispatch(&mut self, update: UpstoxFeedUpdate) -> Option<BrokerExecutionEvent> {
        let order_id = update.order_id.clone().unwrap_or_default();
        if order_id.is_empty() {
            return None;
        }
        let client_order_id = (self.client_order_id_of)(&order_id).unwrap_or(uuid::Uuid::nil());
        let status = update.status.as_deref().unwrap_or_default().to_uppercase();
        match status.as_str() {
            "OPEN" | "TRIGGER PENDING" | "NEW" | "PUT ORDER REQ RECEIVED" => {
                Some(BrokerExecutionEvent::OrderAck(OrderAck {
                    order_id,
                    client_order_id,
                    exchange_order_id: None,
                    status: if status == "OPEN" || status == "NEW" {
                        OrderStatus::New
                    } else {
                        OrderStatus::PendingNew
                    },
                    timestamp: feed_timestamp(&update),
                    correlation_id: None,
                    client_id: None,
                    trace_context: None,
                }))
            }
            "COMPLETE" | "PARTIAL FILL" | "PARTIAL_FILL" => {
                let filled = update
                    .filled_quantity
                    .as_ref()
                    .and_then(wire::integer_from_json)
                    .unwrap_or(0)
                    .max(0) as u64;
                let prev = self.fills.insert(order_id.clone(), filled).unwrap_or(0);
                if filled <= prev {
                    return None;
                }
                Some(BrokerExecutionEvent::Fill(FillReport {
                    fill_id: format!("{order_id}:{filled}"),
                    order_id,
                    client_order_id,
                    instrument: feed_instrument(&update),
                    side: feed_side(&update),
                    price: feed_fill_price(&update),
                    quantity: Quantity::from_u64(filled - prev),
                    algo_id: update.tag.clone().unwrap_or_default(),
                    timestamp: feed_timestamp(&update),
                    correlation_id: None,
                    client_id: None,
                    fee: Money::zero_inr(),
                    fee_breakdown: None,
                    trace_context: None,
                }))
            }
            "REJECTED" => Some(BrokerExecutionEvent::OrderRejected {
                order_id: Some(order_id),
                reason: update
                    .status_message
                    .clone()
                    .unwrap_or_else(|| "rejected by broker".into()),
            }),
            "CANCELLED" | "AMO CANCELLED" => Some(BrokerExecutionEvent::OrderCancelled {
                order_id,
                reason: update.status_message.clone(),
            }),
            _ => None,
        }
    }
}

#[async_trait]
impl ExecutionStream for UpstoxOrderStream {
    async fn next_event(&mut self) -> Result<BrokerExecutionEvent, BrokerError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            let frame = self
                .frame_rx
                .recv()
                .await
                .ok_or_else(|| BrokerError::Transport("upstox feed stream closed".into()))?;
            for update in parse_portfolio_feed_frame(&frame)? {
                if let Some(event) = self.dispatch(update) {
                    self.pending.push_back(event);
                }
            }
        }
    }
}

/// Feed connection behaviour knobs.
#[derive(Debug, Clone)]
pub struct FeedConnectConfig {
    /// Signed-in JWT token (Authorization: Bearer …) on the handshake.
    pub access_token: String,
    /// Max redirect hops for the dedicated-feed-host handshake redirect.
    pub max_redirect_hops: u32,
    /// Max reconnect attempts per epoch.
    pub max_attempts: u32,
    /// Initial reconnect delay (doubles up to `max_backoff`).
    pub backoff: Duration,
    /// Reconnect delay cap.
    pub max_backoff: Duration,
    /// Application-level ping interval.
    pub keepalive: Duration,
}

impl FeedConnectConfig {
    /// Config with production defaults for the given access token.
    pub fn new(access_token: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            max_redirect_hops: 3,
            max_attempts: 8,
            backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(30),
            keepalive: Duration::from_secs(5),
        }
    }
}

/// Connect to the Upstox portfolio stream and spawn the read loop.
pub async fn spawn_order_updates(access_token: &str) -> Result<UpstoxOrderStream, BrokerError> {
    spawn_order_updates_with(FeedConnectConfig::new(access_token)).await
}

/// Same as [`spawn_order_updates`] with explicit connection knobs.
pub async fn spawn_order_updates_with(
    config: FeedConnectConfig,
) -> Result<UpstoxOrderStream, BrokerError> {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION as WS_AUTH;

    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), BrokerError>>();

    tokio::spawn(async move {
        let mut delay = config.backoff;
        let mut ready = Some(ready_tx);
        for attempt in 1..=config.max_attempts {
            let connect = || async {
                let mut current_url = UPSTOX_WS_URL.to_string();
                let mut hops = 0;
                loop {
                    let mut request = current_url.as_str().into_client_request().map_err(|e| {
                        BrokerError::Transport(format!("upstox feed request build: {e}"))
                    })?;
                    request.headers_mut().insert(
                        WS_AUTH,
                        format!("Bearer {}", config.access_token)
                            .parse()
                            .map_err(|e| {
                                BrokerError::Auth(format!("upstox feed auth header: {e}"))
                            })?,
                    );
                    match tokio_tungstenite::connect_async(request).await {
                        // Upstox documents a redirect to the dedicated feed host;
                        // tungstenite surfaces it as an HTTP-error handshake.
                        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                            let location = response
                                .headers()
                                .get("Location")
                                .and_then(|v| v.to_str().ok().map(|s| s.to_string()));
                            match location {
                                Some(next) if hops < config.max_redirect_hops => {
                                    hops += 1;
                                    current_url = next;
                                    continue;
                                }
                                Some(_) => {
                                    return Err(BrokerError::Transport(
                                        "upstox feed redirect loop".into(),
                                    ));
                                }
                                None => {
                                    return Err(BrokerError::Transport(format!(
                                        "upstox feed handshake rejected: HTTP {}",
                                        response.status()
                                    )));
                                }
                            }
                        }
                        Err(err) => {
                            return Err(BrokerError::Transport(format!(
                                "upstox feed connect failed: {err}"
                            )));
                        }
                        Ok((ws, _)) => return Ok(ws),
                    }
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
                        "upstox feed connect failed; retrying in {:?}",
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
                        if write.send(Message::Ping(b"honba".as_slice().into())).await.is_err() {
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
                            Some(Ok(_)) => {
                                // Binary protobuf market ticks / pings carry no
                                // order content on this feed.
                            }
                            Some(Err(err)) => {
                                tracing::warn!(%err, "upstox feed read error");
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
        Ok(Ok(())) => Ok(UpstoxOrderStream::new(rx)),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(BrokerError::Transport("upstox feed connector died".into())),
    }
}
