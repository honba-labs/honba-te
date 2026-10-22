//! Upstox v2 API execution adapter.
//!
//! M2 full surface:
//! - REST: place, modify (read-then-patch semantics), cancel, order book,
//!   short-term positions, funds and margin — envelope-driven with exact
//!   [`rust_decimal::Decimal`] decoding (`price`/`trigger_price` serialize as
//!   exact strings on request axes; broker float responses decode through
//!   serde_json's shortest textual round-trip, never binary artifacts).
//! - Order execution stream over the portfolio RSS WebSocket feed: [`ws`].
//!
//! Order-channel rate limits are enforced by wrapping with
//! [`honba_te_core::RateLimitedBroker`] via [`UpstoxExecutor::rate_limited`].

mod ws;

pub use ws::{
    UPSTOX_WS_URL, UpstoxFeedUpdate, UpstoxOrderStream, parse_portfolio_feed_frame,
    spawn_order_updates, spawn_order_updates_with,
};

use async_trait::async_trait;
use chrono::Utc;
use reqwest::Method;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::{Duration, Instant};

use honba_core::{
    InstrumentId, Money, OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, Price,
    Quantity, TimeInForce,
};
use honba_te_core::decimal::{format_fixed_exact, quantity_to_u32_exact};
use honba_te_core::wire;
use honba_te_core::{
    BrokerError, BrokerExecutor, BrokerHealth, BrokerMargins, BrokerPosition, BrokerRateCatalog,
    OrderSnapshot, PositionSide, ProductType, RateLimitedBroker,
};

/// Upstox execution adapter.
pub struct UpstoxExecutor {
    /// Upstox API key
    pub api_key: String,
    /// OAuth Bearer access token
    pub access_token: String,
    /// Base URL (configurable for testing)
    pub base_url: String,
    /// Default product used when an order carries no explicit taxonomy choice
    pub product: ProductType,
    client: reqwest::Client,
}

impl UpstoxExecutor {
    /// Default Upstox v2 endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://api.upstox.com/v2";

    /// Request timeout applied to every REST call.
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

    /// Create a new Upstox execution adapter (default product: `D` delivery).
    pub fn new(api_key: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self::with_base_url(api_key, access_token, Self::DEFAULT_BASE_URL)
    }

    /// Create adapter with custom base URL.
    pub fn with_base_url(
        api_key: impl Into<String>,
        access_token: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            access_token: access_token.into(),
            base_url: base_url.into(),
            product: ProductType::CncDelivery,
            client: reqwest::Client::builder()
                .timeout(Self::REQUEST_TIMEOUT)
                .build()
                .expect("reqwest client builds without a TLS backend failure"),
        }
    }

    /// Override the default product taxonomy for orders placed through this adapter.
    pub fn with_product(mut self, product: ProductType) -> Self {
        self.product = product;
        self
    }

    /// Wrap with the Upstox rate-limit preset.
    pub fn rate_limited(self) -> Result<RateLimitedBroker<Self>, BrokerError> {
        RateLimitedBroker::with_config(self, BrokerRateCatalog::for_broker("upstox"))
    }

    fn auth_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let auth_val = HeaderValue::from_str(&format!("Bearer {}", self.access_token))
            .map_err(|e| BrokerError::Auth(format!("invalid token format: {e}")))?;
        headers.insert(AUTHORIZATION, auth_val);
        Ok(headers)
    }

    /// Build the Upstox v2 order placement JSON payload.
    ///
    /// `price` and `trigger_price` serialize as exact strings carrying scale 2
    /// (paise); `quantity` is a validated integral u32. No f64 anywhere.
    pub fn build_order_payload(
        req: &OrderRequest,
        product: ProductType,
    ) -> Result<UpstoxOrderPayload, BrokerError> {
        let quantity_int = quantity_to_u32_exact(req.quantity.0)?;

        let (order_type, price, trigger_price) = match req.order_type {
            OrderType::Market => ("MARKET", None, None),
            OrderType::Limit(p) => ("LIMIT", Some(p), None),
            OrderType::StopLoss(p) => ("SL-M", None, Some(p)),
            OrderType::StopLimit { trigger, limit } => ("SL", Some(limit), Some(trigger)),
        };

        let transaction_type = match req.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let validity = match req.tif {
            TimeInForce::Day => "DAY",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Gtc => "DAY",
        };

        let instrument_token = req.instrument.token.clone().unwrap_or_else(|| {
            format!(
                "{}|{}",
                upstox_exchange_segment(req.instrument.exchange),
                req.instrument.symbol
            )
        });

        Ok(UpstoxOrderPayload {
            quantity: quantity_int,
            product: product.upstox().to_string(),
            validity: validity.to_string(),
            price: match price {
                Some(p) => Some(format_fixed_exact(p.0, 2)?),
                None => None,
            },
            tag: Some(req.algo_id.clone()),
            instrument_token,
            order_type: order_type.to_string(),
            transaction_type: transaction_type.to_string(),
            disclosed_quantity: 0,
            trigger_price: match trigger_price {
                Some(t) => Some(format_fixed_exact(t.0, 2)?),
                None => None,
            },
            is_amo: false,
            order_id: None,
        })
    }

    /// Send an Upstox v2 request and return the parsed JSON body.
    ///
    /// Envelope `status: "error"` and non-2xx HTTP map through
    /// [`map_upstox_error`]; non-JSON replies (gateway pages) map to Transport.
    async fn request_upstox(
        &self,
        method: Method,
        path: &str,
        json_body: Option<Value>,
    ) -> Result<Value, BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!("{}{}", self.base_url, path);
        let req = self.client.request(method, &url).headers(headers);
        let req = if let Some(body) = json_body {
            req.json(&body)
        } else {
            req
        };
        let res = req
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;
        let status = res.status().as_u16();
        let text = res
            .text()
            .await
            .map_err(|e| BrokerError::Transport(format!("failed to read upstox reply: {e}")))?;
        let value: Value = serde_json::from_str(&text).map_err(|_| {
            BrokerError::Transport(format!("non-JSON upstox reply (HTTP {status})"))
        })?;
        if status >= 400 || value.get("status").and_then(Value::as_str) == Some("error") {
            let errors = value
                .get("errors")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            return Err(map_upstox_error(status, &errors));
        }
        Ok(value)
    }

    /// Extract Upstox's `data` member and deserialize into the canonical shape.
    fn upstox_data<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, BrokerError> {
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        serde_json::from_value(data)
            .map_err(|e| BrokerError::Transport(format!("unparseable upstox data: {e}")))
    }

    async fn get_upstox(&self, path: &str) -> Result<Value, BrokerError> {
        self.request_upstox(Method::GET, path, None).await
    }
}

/// Canonical exchange → Upstox instrument-key segment.
pub fn upstox_exchange_segment(exchange: honba_core::Exchange) -> &'static str {
    match exchange {
        honba_core::Exchange::Nse => "NSE_EQ",
        honba_core::Exchange::Bse => "BSE_EQ",
        honba_core::Exchange::Mcx => "MCX_FO",
    }
}

/// Canonical symbol: Upstox `trading_symbol` strip `-EQ`/`-BE` equity suffixes
/// (documented v2 equity tokens carry the segment suffix).
fn canonical_symbol(tradingsymbol: Option<&str>, isin: Option<&str>) -> String {
    let raw = tradingsymbol
        .filter(|s| !s.is_empty())
        .map(|s| s.to_uppercase())
        .or_else(|| isin.map(|i| i.to_uppercase()))
        .unwrap_or_default();
    raw.strip_suffix("-EQ")
        .or_else(|| raw.strip_suffix("-BE"))
        .map(|s| s.to_string())
        .unwrap_or(raw)
}

/// Map Upstox v2 errors into the canonical [`BrokerError`] taxonomy.
///
/// Upstox surfaces `{"errors":[{"errorCode","errorMessage","errorType"}]}`;
/// known error types carry Authentication/Validation/RateLimit semantics.
pub fn map_upstox_error(status: u16, errors: &[Value]) -> BrokerError {
    let first = errors.first().cloned().unwrap_or(Value::Null);
    let message = first
        .get("errorMessage")
        .and_then(Value::as_str)
        .or_else(|| first.get("message").and_then(Value::as_str))
        .unwrap_or("unknown upstox error")
        .to_string();
    let error_code = first
        .get("errorCode")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_uppercase();
    let error_type = first
        .get("errorType")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_uppercase();

    if status == 429
        || error_code.contains("UPI1001")
        || error_type.contains("RATELIMIT")
        || message.to_uppercase().contains("RATE LIMIT")
    {
        return BrokerError::RateLimited(message);
    }
    if status == 401
        || status == 403
        || error_code == "UDC1002"
        || error_type.contains("AUTHENTICATION")
        || message.to_uppercase().contains("ACCESS TOKEN")
        || message.to_uppercase().contains("SESSION EXPIRED")
    {
        return BrokerError::Auth(message);
    }
    if error_type.contains("INVALID") || error_code.starts_with("UDI") {
        return BrokerError::InvalidRequest(message);
    }
    BrokerError::Rejected(message)
}

/// Upstox v2 order placement JSON request (exact decimal strings).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct UpstoxOrderPayload {
    pub quantity: u32,
    pub product: String,
    pub validity: String,
    /// Exact decimal price, e.g. "3800.00" (None for market orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    pub instrument_token: String,
    pub order_type: String,
    pub transaction_type: String,
    pub disclosed_quantity: u32,
    /// Exact decimal trigger price (None for non-stop orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<String>,
    pub is_amo: bool,
    /// Order to modify (modify requests only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
}

fn upstox_status(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "COMPLETE" | "FILLED" => OrderStatus::Filled,
        "REJECTED" => OrderStatus::Rejected,
        "CANCELLED" => OrderStatus::Cancelled,
        "OPEN" => OrderStatus::New,
        _ => OrderStatus::PendingNew,
    }
}

#[derive(Debug, serde::Deserialize)]
struct UserProfile {
    user_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct OrderIdData {
    order_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct OrderHistoryEntry {
    status: String,
}

/// Upstox order-book wire record (numeric legs stay on the wire and decode
/// exactly via [`wire`]).
#[derive(Debug, Clone, serde::Deserialize)]
struct UpstoxOrderWire {
    order_id: String,
    #[serde(default)]
    exchange_order_id: Option<String>,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default, alias = "tradingsymbol", alias = "trading_symbol")]
    tradingsymbol: Option<String>,
    #[serde(default, alias = "isin")]
    instrument_isin: Option<String>,
    #[serde(default)]
    instrument_token: Option<String>,
    #[serde(default)]
    transaction_type: Option<String>,
    #[serde(default)]
    product: Option<String>,
    #[serde(default)]
    order_type: Option<String>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    quantity: Option<Value>,
    #[serde(default)]
    filled_quantity: Option<Value>,
    #[serde(default)]
    price: Option<Value>,
    #[serde(default)]
    trigger_price: Option<Value>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    exchange_timestamp: Option<String>,
}

/// Upstox short-term-position wire record.
#[derive(Debug, serde::Deserialize)]
struct UpstoxPositionWire {
    #[serde(default)]
    instrument_token: Option<String>,
    #[serde(default, alias = "trading_symbol")]
    tradingsymbol: Option<String>,
    #[serde(default)]
    isin: Option<String>,
    #[serde(default)]
    product: Option<String>,
    #[serde(default)]
    quantity: Option<Value>,
    #[serde(default)]
    net_price: Option<Value>,
    #[serde(default)]
    pnl: Option<Value>,
    #[serde(default)]
    last_price: Option<Value>,
}

fn status_of(order: &UpstoxOrderWire) -> OrderStatus {
    upstox_status(&order.status)
}

fn side_of(order: &UpstoxOrderWire) -> OrderSide {
    match order
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

fn order_type_of(order: &UpstoxOrderWire) -> OrderType {
    let price = order
        .price
        .as_ref()
        .and_then(wire::price_from_json)
        .unwrap_or(Price::ZERO);
    let trigger = order
        .trigger_price
        .as_ref()
        .and_then(wire::price_from_json)
        .unwrap_or(Price::ZERO);
    match order.order_type.as_deref().unwrap_or_default() {
        "LIMIT" => OrderType::Limit(price),
        "SL-M" => OrderType::StopLoss(trigger),
        "SL" => OrderType::StopLimit {
            trigger,
            limit: price,
        },
        _ => OrderType::Market,
    }
}

fn instrument_of(
    exchange_text: Option<&str>,
    token: Option<&str>,
    tradingsymbol: Option<&str>,
    isin: Option<&str>,
) -> InstrumentId {
    let exchange = match token
        .unwrap_or_default()
        .split('|')
        .next()
        .filter(|seg| !seg.is_empty())
        .unwrap_or_else(|| exchange_text.unwrap_or_default())
        .to_uppercase()
        .as_str()
    {
        "BSE" | "BSE_EQ" | "BSE_FO" => honba_core::Exchange::Bse,
        "MCX" | "MCX_EQ" | "MCX_FO" => honba_core::Exchange::Mcx,
        _ => honba_core::Exchange::Nse,
    };
    InstrumentId::new(exchange, canonical_symbol(tradingsymbol, isin))
}

fn quantity_field(value: &Option<Value>) -> Quantity {
    value
        .as_ref()
        .and_then(wire::quantity_from_json)
        .unwrap_or(Quantity::ZERO)
}

#[async_trait]
impl BrokerExecutor for UpstoxExecutor {
    fn name(&self) -> &str {
        "upstox"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let started = Instant::now();
        let body = self.get_upstox("/user/profile").await?;
        let latency_ms = latency_millis(started.elapsed());
        let profile: UserProfile = Self::upstox_data(body)?;
        if profile.user_id.is_empty() {
            return Err(BrokerError::Auth("empty upstox user id".into()));
        }
        Ok(BrokerHealth {
            broker: "upstox".into(),
            session_valid: true,
            authenticated_user: Some(profile.user_id),
            latency: Some(latency_ms),
            detail: "upstox session active".into(),
        })
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let payload = Self::build_order_payload(&req, self.product)?;
        let body = self
            .request_upstox(
                Method::POST,
                "/order/place",
                Some(serde_json::to_value(&payload).map_err(|e| {
                    BrokerError::InvalidRequest(format!("order payload serialize: {e}"))
                })?),
            )
            .await?;
        let data: OrderIdData = Self::upstox_data(body)?;
        Ok(OrderAck {
            order_id: data.order_id,
            client_order_id: req.client_order_id,
            exchange_order_id: None,
            status: OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: req.correlation_id,
            client_id: req.client_id.clone(),
        })
    }

    /// Upstox modify semantics require the full order definition; the current
    /// order detail is read first, then patched with the new legs.
    async fn modify_order(
        &self,
        order_id: &str,
        new_price: Option<Price>,
        new_quantity: Option<Quantity>,
    ) -> Result<OrderAck, BrokerError> {
        if new_price.is_none() && new_quantity.is_none() {
            return Err(BrokerError::InvalidRequest(
                "modify_order requires a new price or quantity".into(),
            ));
        }
        let current: UpstoxOrderWire = {
            let value = self
                .get_upstox(&format!("/order/details/{order_id}"))
                .await?;
            let history: Vec<UpstoxOrderWire> = Self::upstox_data(value)?;
            history
                .last()
                .cloned()
                .ok_or_else(|| BrokerError::OrderNotFound(order_id.to_string()))?
        };
        let payload = UpstoxOrderPayload {
            quantity: quantity_to_u32_exact(
                new_quantity
                    .map(|q| q.0)
                    .unwrap_or_else(|| quantity_field(&current.quantity).0),
            )?,
            product: current
                .product
                .clone()
                .unwrap_or_else(|| self.product.upstox().to_string()),
            validity: "DAY".to_string(),
            price: Some(match new_price {
                Some(p) => format_fixed_exact(p.0, 2)?,
                None => current
                    .price
                    .as_ref()
                    .and_then(wire::decimal_from_json)
                    .map(|d| format_fixed_exact(d, 2))
                    .transpose()?
                    .unwrap_or_else(|| "0.00".to_string()),
            }),
            tag: None,
            instrument_token: current.instrument_token.clone().unwrap_or_default(),
            order_type: current
                .order_type
                .clone()
                .unwrap_or_else(|| "MARKET".to_string()),
            transaction_type: match side_of(&current) {
                OrderSide::Sell => "SELL".to_string(),
                OrderSide::Buy => "BUY".to_string(),
            },
            disclosed_quantity: 0,
            trigger_price: None,
            is_amo: false,
            order_id: Some(order_id.to_string()),
        };
        let body = self
            .request_upstox(
                Method::PUT,
                "/order/modify",
                Some(serde_json::to_value(&payload).map_err(|e| {
                    BrokerError::InvalidRequest(format!("modify payload serialize: {e}"))
                })?),
            )
            .await?;
        let data: OrderIdData = Self::upstox_data(body)?;
        Ok(OrderAck::new(
            data.order_id,
            uuid::Uuid::nil(),
            OrderStatus::New,
            Utc::now(),
        ))
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let path = format!("/order/cancel?order_id={order_id}");
        let _ = self.request_upstox(Method::DELETE, &path, None).await?;
        Ok(())
    }

    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        let value = self.get_upstox("/order/book").await?;
        let orders: Vec<UpstoxOrderWire> = Self::upstox_data(value)?;
        Ok(orders
            .into_iter()
            .map(|o| OrderSnapshot {
                order_id: o.order_id.clone(),
                client_order_id: o.tag.as_ref().map(|_| uuid::Uuid::nil()),
                exchange_order_id: o.exchange_order_id.clone(),
                instrument: instrument_of(
                    o.exchange.as_deref(),
                    o.instrument_token.as_deref(),
                    o.tradingsymbol.as_deref(),
                    o.instrument_isin.as_deref(),
                ),
                side: side_of(&o),
                order_type: order_type_of(&o),
                quantity: quantity_field(&o.quantity),
                filled_quantity: quantity_field(&o.filled_quantity),
                price: o.price.as_ref().and_then(wire::price_from_json),
                trigger_price: o.trigger_price.as_ref().and_then(wire::price_from_json),
                status: status_of(&o),
                product: o.product.as_deref().and_then(ProductType::from_upstox),
                timestamp: o
                    .exchange_timestamp
                    .as_deref()
                    .and_then(wire::parse_broker_ist_datetime)
                    .unwrap_or_else(Utc::now),
            })
            .collect())
    }

    async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError> {
        let value = self.get_upstox("/portfolio/short-term-positions").await?;
        let positions: Vec<UpstoxPositionWire> = Self::upstox_data(value)?;
        Ok(positions
            .into_iter()
            .map(|p| {
                let qty = p
                    .quantity
                    .as_ref()
                    .and_then(wire::decimal_from_json)
                    .unwrap_or(Decimal::ZERO);
                let side = if qty.is_zero() {
                    PositionSide::Flat
                } else if qty.is_sign_negative() {
                    PositionSide::Short
                } else {
                    PositionSide::Long
                };
                BrokerPosition {
                    instrument: instrument_of(
                        None,
                        p.instrument_token.as_deref(),
                        p.tradingsymbol.as_deref(),
                        p.isin.as_deref(),
                    ),
                    product: p
                        .product
                        .as_deref()
                        .and_then(ProductType::from_upstox)
                        .unwrap_or(self.product),
                    side,
                    quantity: Quantity::new(qty.abs()),
                    average_price: p
                        .net_price
                        .as_ref()
                        .and_then(wire::price_from_json)
                        .unwrap_or(Price::ZERO),
                    realized_pnl: p
                        .pnl
                        .as_ref()
                        .and_then(wire::decimal_from_json)
                        .map(Money::inr),
                    last_price: p.last_price.as_ref().and_then(wire::price_from_json),
                }
            })
            .collect())
    }

    async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError> {
        // GET /user/funds-and-margin — segment-float wire numbers decode
        // exactly; utilisations sum across segments.
        let value = self.get_upstox("/user/funds-and-margin").await?;
        let data = value.get("data").cloned().unwrap_or(Value::Null);

        let available_cash = data
            .get("available_margin")
            .and_then(wire::decimal_from_json)
            .or_else(|| {
                let equity = data.get("equity_cash").and_then(wire::decimal_from_json);
                let commodity = data.get("commodity_cash").and_then(wire::decimal_from_json);
                match (equity, commodity) {
                    (Some(e), Some(c)) => Some(e + c),
                    (Some(e), None) => Some(e),
                    (None, Some(c)) => Some(c),
                    (None, None) => None,
                }
            })
            .unwrap_or(Decimal::ZERO);
        let used = data
            .get("equity_margin_utilised")
            .and_then(|seg| seg.get("debits"))
            .and_then(wire::decimal_from_json)
            .unwrap_or(Decimal::ZERO)
            + data
                .get("commodity_margin_utilised")
                .and_then(|seg| seg.get("debits"))
                .and_then(wire::decimal_from_json)
                .unwrap_or(Decimal::ZERO);
        let collateral = data
            .get("collateral")
            .and_then(wire::decimal_from_json)
            .unwrap_or(Decimal::ZERO);
        Ok(BrokerMargins {
            available_cash: Money::inr(available_cash),
            used_margin: Money::inr(used),
            collateral: Money::inr(collateral),
        })
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let value = self
            .get_upstox(&format!("/order/details/{order_id}"))
            .await?;
        let history: Vec<OrderHistoryEntry> = Self::upstox_data(value)?;
        if let Some(last) = history.last() {
            return Ok(upstox_status(&last.status));
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

fn latency_millis(d: Duration) -> Decimal {
    Decimal::new(d.as_millis() as i64, 0)
}
