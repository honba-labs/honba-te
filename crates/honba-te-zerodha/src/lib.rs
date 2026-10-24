//! Zerodha (Kite Connect v3) execution adapter.
//!
//! M2 full surface:
//! - REST: place/modify/cancel, order book, positions, margins — with exact
//!   [`rust_decimal::Decimal`] decoding of broker JSON (exchange floats never
//!   reach the domain layer; serde_json's shortest round-trip text keeps
//!   rupee/paise values exact).
//! - Postback webhook listener (axum + SHA-256 checksum): [`postback`].
//! - Kite Ticker order-update stream: [`ticker`].
//!
//! Kite Connect v3 rate limits (3 order calls/s, 10 general calls/s) are
//! enforced by wrapping with [`honba_te_core::RateLimitedBroker`] via
//! [`ZerodhaExecutor::rate_limited`].

mod postback;
mod ticker;

pub use postback::{
    KitePostback, PostbackConfig, PostbackListener, decode_postback_payload, postback_checksum,
    verify_postback_checksum,
};
pub use ticker::{
    KiteTickerOrderStream, KiteTickerOrderUpdate, TICKER_WS_URL, TickerConnectConfig,
    parse_order_update_frame, spawn_order_updates, spawn_order_updates_with,
};

use async_trait::async_trait;
use chrono::Utc;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use honba_core::{
    InstrumentId, Money, OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, Price,
    Quantity, TimeInForce,
};
use honba_te_core::decimal::{format_fixed_exact, quantity_to_u32_exact};
use honba_te_core::wire;
use honba_te_core::{
    BrokerError, BrokerExecutor, BrokerHealth, BrokerMargins, BrokerPosition, BrokerRateCatalog,
    IcebergOrderExecutor, OrderSnapshot, PositionSide, ProductType, RateLimitedBroker,
};

/// Zerodha Kite Connect execution adapter.
pub struct ZerodhaExecutor {
    /// Zerodha API key
    pub api_key: String,
    /// User session access token
    pub access_token: String,
    /// Base URL (configurable for testing)
    pub base_url: String,
    /// Default product used when an order carries no explicit taxonomy choice
    pub product: ProductType,
    client: reqwest::Client,
}

impl ZerodhaExecutor {
    /// Default Kite Connect API endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://api.kite.trade";

    /// Request timeout applied to every REST call.
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

    /// Create a new Zerodha execution adapter (default product: CNC delivery).
    pub fn new(api_key: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self::with_base_url(api_key, access_token, Self::DEFAULT_BASE_URL)
    }

    /// Create adapter with custom base URL (useful for integration testing and local mocks).
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

    /// Wrap with the Kite Connect rate-limit preset (order bucket 3/s burst 3,
    /// general bucket 10/s per official Kite Connect limits).
    pub fn rate_limited(self) -> Result<RateLimitedBroker<Self>, BrokerError> {
        RateLimitedBroker::with_config(self, BrokerRateCatalog::for_broker("zerodha"))
    }

    fn auth_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut headers = HeaderMap::new();
        headers.insert("X-Kite-Version", HeaderValue::from_static("3"));
        let auth_str = format!("token {}:{}", self.api_key, self.access_token);
        let auth_val = HeaderValue::from_str(&auth_str)
            .map_err(|e| BrokerError::Auth(format!("invalid auth header: {e}")))?;
        headers.insert(reqwest::header::AUTHORIZATION, auth_val);
        Ok(headers)
    }

    /// Convert canonical [`OrderRequest`] into a Zerodha form payload.
    ///
    /// All numeric fields are exact decimal strings with explicit scale:
    /// prices and trigger prices at scale 2 (paise), quantities as positive
    /// integral share counts. Misaligned values are rejected, never rounded.
    pub fn build_order_payload(
        req: &OrderRequest,
        product: ProductType,
    ) -> Result<HashMap<&'static str, String>, BrokerError> {
        let quantity = quantity_to_u32_exact(req.quantity.0)?;

        let mut form = HashMap::new();
        form.insert("tradingsymbol", req.instrument.symbol.clone());
        form.insert("exchange", req.instrument.exchange.to_string());

        let trans_type = match req.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        form.insert("transaction_type", trans_type.to_string());

        let (order_type_str, price, trigger) = match req.order_type {
            OrderType::Market => ("MARKET", None, None),
            OrderType::Limit(p) => ("LIMIT", Some(format_fixed_exact(p.0, 2)?), None),
            OrderType::StopLoss(p) => ("SL-M", None, Some(format_fixed_exact(p.0, 2)?)),
            OrderType::StopLimit { trigger, limit } => (
                "SL",
                Some(format_fixed_exact(limit.0, 2)?),
                Some(format_fixed_exact(trigger.0, 2)?),
            ),
        };
        form.insert("order_type", order_type_str.to_string());
        if let Some(p) = price {
            form.insert("price", p);
        }
        if let Some(t) = trigger {
            form.insert("trigger_price", t);
        }

        form.insert("quantity", quantity.to_string());
        form.insert("product", product.zerodha().to_string());

        let validity = match req.tif {
            TimeInForce::Day => "DAY",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Gtc => "TTL",
        };
        form.insert("validity", validity.to_string());
        // SEBI algo id travels in the compliance tag.
        form.insert("tag", req.algo_id.clone());

        Ok(form)
    }

    /// Send a Kite request and return the parsed JSON body.
    ///
    /// Transport and envelope errors are mapped through [`map_kite_error`];
    /// non-JSON replies (gateway maintenance pages) become Transport errors.
    async fn request_kite(
        &self,
        method: Method,
        path: &str,
        form: Option<HashMap<&'static str, String>>,
    ) -> Result<Value, BrokerError> {
        self.request_kite_traced(method, path, form, None).await
    }

    /// Instrumented HTTP dispatch attaching OpenTelemetry span, W3C `traceparent`,
    /// and exporting Prometheus latency metrics.
    async fn request_kite_traced(
        &self,
        method: Method,
        path: &str,
        form: Option<HashMap<&'static str, String>>,
        trace_context: Option<&honba_core::TraceContext>,
    ) -> Result<Value, BrokerError> {
        let span = tracing::info_span!(
            "broker_http_request",
            broker = "zerodha",
            endpoint = path,
            http.method = %method,
            http.status_code = tracing::field::Empty,
        );
        let _guard = span.enter();

        let mut headers = self.auth_headers()?;
        if let Some(tc) = trace_context {
            if let Ok(val) = HeaderValue::from_str(&tc.to_traceparent()) {
                headers.insert("traceparent", val);
            }
        }

        let url = format!("{}{}", self.base_url, path);
        let req = self.client.request(method, &url).headers(headers);
        let req = if let Some(form) = form {
            req.form(&form)
        } else {
            req
        };
        let started = Instant::now();
        let res = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                let duration = started.elapsed();
                honba_te_core::telemetry::record_http_request("zerodha", path, 0, duration);
                return Err(BrokerError::Transport(e.to_string()));
            }
        };
        let status = res.status().as_u16();
        let duration = started.elapsed();
        span.record("http.status_code", status);
        honba_te_core::telemetry::record_http_request("zerodha", path, status, duration);

        let text = res
            .text()
            .await
            .map_err(|e| BrokerError::Transport(format!("failed to read kite reply: {e}")))?;
        let value: Value = serde_json::from_str(&text)
            .map_err(|_| BrokerError::Transport(format!("non-JSON kite reply (HTTP {status})")))?;
        if status >= 400 || value.get("status").and_then(Value::as_str) == Some("error") {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown kite error")
                .to_string();
            let error_type = value
                .get("error_type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            return Err(map_kite_error(status, &message, error_type));
        }
        Ok(value)
    }

    /// Extract Kite's `data` member and deserialize into the canonical shape.
    fn kite_data<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, BrokerError> {
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        serde_json::from_value(data)
            .map_err(|e| BrokerError::Transport(format!("unparseable kite data: {e}")))
    }

    async fn get_kite(&self, path: &str) -> Result<Value, BrokerError> {
        self.request_kite(Method::GET, path, None).await
    }
}

/// Map Kite Connect errors into the canonical [`BrokerError`] taxonomy.
///
/// Kite surfaces both an HTTP status and an `error_type` literal; both carry
/// signal (429 with `TooManyRequests`, session expiry with `TokenException`,
/// gateway outages with `NetworkException`).
pub fn map_kite_error(status: u16, message: &str, error_type: &str) -> BrokerError {
    if status == 429
        || error_type.eq_ignore_ascii_case("TooManyRequests")
        || error_type.eq_ignore_ascii_case("RateLimitException")
    {
        return BrokerError::RateLimited(message.to_string());
    }
    if status == 401 || status == 403 {
        return BrokerError::Auth(message.to_string());
    }
    match error_type {
        "TokenException" | "TwoFactorAuthException" => BrokerError::Auth(message.to_string()),
        "InputException" | "DataException" | "ParseException" => {
            BrokerError::InvalidRequest(message.to_string())
        }
        "NetworkException" | "GatewayException" => BrokerError::Transport(message.to_string()),
        _ => BrokerError::Rejected(message.to_string()),
    }
}

#[derive(Debug, serde::Deserialize)]
struct UserProfile {
    user_id: String,
}

fn kite_status(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "COMPLETE" => OrderStatus::Filled,
        "REJECTED" => OrderStatus::Rejected,
        "CANCELLED" => OrderStatus::Cancelled,
        "OPEN" => OrderStatus::New,
        _ => OrderStatus::PendingNew,
    }
}

/// Map the Kite `order_type`+price/trigger trio into the canonical type.
fn kite_order_type(order_type: &str, price: Option<Value>, trigger: Option<Value>) -> OrderType {
    let price = price
        .and_then(|v| wire::price_from_json(&v))
        .unwrap_or(Price::ZERO);
    let trigger = trigger
        .and_then(|v| wire::price_from_json(&v))
        .unwrap_or(Price::ZERO);
    match order_type {
        "LIMIT" => OrderType::Limit(price),
        "SL-M" => OrderType::StopLoss(trigger),
        "SL" => OrderType::StopLimit {
            trigger,
            limit: price,
        },
        _ => OrderType::Market,
    }
}

fn kite_exchange(exchange: &str) -> honba_core::Exchange {
    match exchange.to_uppercase().as_str() {
        "BSE" => honba_core::Exchange::Bse,
        "MCX" => honba_core::Exchange::Mcx,
        _ => honba_core::Exchange::Nse,
    }
}

/// Map the Kite margin product token to the canonical taxonomy, defaulting
/// unmapped tokens (`BO` verkettet specials, carry products) to CNC delivery.
fn kite_product_or_default(token: &str) -> ProductType {
    ProductType::from_zerodha(token).unwrap_or(ProductType::CncDelivery)
}

fn latency_millis(d: Duration) -> Decimal {
    Decimal::new(d.as_millis() as i64, 0)
}

#[async_trait]
impl BrokerExecutor for ZerodhaExecutor {
    fn name(&self) -> &str {
        "zerodha"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let started = Instant::now();
        let body = self.get_kite("/user/profile").await?;
        let latency_ms = latency_millis(started.elapsed());
        let profile: UserProfile = Self::kite_data(body)?;
        Ok(BrokerHealth {
            broker: "zerodha".into(),
            session_valid: true,
            authenticated_user: Some(profile.user_id),
            latency: Some(latency_ms),
            detail: "kite session active".into(),
        })
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let form = Self::build_order_payload(&req, self.product)?;
        let value = self
            .request_kite_traced(
                Method::POST,
                "/orders/regular",
                Some(form),
                req.trace_context.as_ref(),
            )
            .await?;
        let data: PlaceOrderData = Self::kite_data(value)?;
        Ok(OrderAck {
            order_id: data.order_id,
            client_order_id: req.client_order_id,
            exchange_order_id: None,
            status: OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: req.correlation_id,
            client_id: req.client_id.clone(),
            trace_context: req.trace_context,
        })
    }

    async fn modify_order(
        &self,
        order_id: &str,
        new_price: Option<Price>,
        new_quantity: Option<Quantity>,
    ) -> Result<OrderAck, BrokerError> {
        // Kite v3 modifies carry only the changed legs; build the exact form.
        let mut form: HashMap<&'static str, String> = HashMap::new();
        if let Some(p) = new_price {
            form.insert("price", format_fixed_exact(p.0, 2)?);
        }
        if let Some(q) = new_quantity {
            form.insert("quantity", quantity_to_u32_exact(q.0)?.to_string());
        }
        if form.is_empty() {
            return Err(BrokerError::InvalidRequest(
                "modify_order requires a new price or quantity".into(),
            ));
        }
        let value = self
            .request_kite(
                Method::PUT,
                &format!("/orders/regular/{order_id}"),
                Some(form),
            )
            .await?;
        let data: PlaceOrderData = Self::kite_data(value)?;
        Ok(OrderAck::new(
            data.order_id,
            uuid::Uuid::nil(),
            OrderStatus::New,
            Utc::now(),
        ))
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let path = format!("/orders/regular/{order_id}");
        let _ = self.request_kite(Method::DELETE, &path, None).await?;
        Ok(())
    }

    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        let value = self.get_kite("/orders").await?;
        let orders: Vec<KiteOrder> = Self::kite_data(value)?;
        Ok(orders
            .into_iter()
            .map(|o| {
                let quantity = o
                    .quantity
                    .and_then(|v| wire::quantity_from_json(&v))
                    .unwrap_or(Quantity::ZERO);
                let filled = o
                    .filled_quantity
                    .and_then(|v| wire::quantity_from_json(&v))
                    .unwrap_or(Quantity::ZERO);
                OrderSnapshot {
                    order_id: o.order_id,
                    client_order_id: None,
                    exchange_order_id: o.exchange_order_id,
                    instrument: InstrumentId::new(
                        kite_exchange(&o.exchange),
                        o.tradingsymbol.clone(),
                    ),
                    side: match o.transaction_type.as_str() {
                        "SELL" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    order_type: kite_order_type(
                        &o.order_type,
                        o.price.clone(),
                        o.trigger_price.clone(),
                    ),
                    quantity,
                    filled_quantity: filled,
                    price: o.price.and_then(|v| wire::price_from_json(&v)),
                    trigger_price: o.trigger_price.and_then(|v| wire::price_from_json(&v)),
                    status: kite_status(&o.status),
                    product: ProductType::from_zerodha(&o.product),
                    timestamp: o
                        .order_timestamp
                        .as_deref()
                        .and_then(wire::parse_broker_ist_datetime)
                        .unwrap_or_else(Utc::now),
                }
            })
            .collect())
    }

    async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError> {
        let value = self.get_kite("/portfolio/positions").await?;
        let data: KitePositions = Self::kite_data(value)?;
        Ok(data
            .net
            .into_iter()
            .map(|p| {
                let qty = p
                    .quantity
                    .and_then(|v| wire::decimal_from_json(&v))
                    .unwrap_or(Decimal::ZERO);
                let side = if qty.is_zero() {
                    PositionSide::Flat
                } else if qty.is_sign_negative() {
                    PositionSide::Short
                } else {
                    PositionSide::Long
                };
                BrokerPosition {
                    instrument: InstrumentId::new(
                        kite_exchange(&p.exchange),
                        p.tradingsymbol.clone(),
                    ),
                    product: kite_product_or_default(&p.product),
                    side,
                    quantity: Quantity::new(qty.abs()),
                    average_price: p
                        .average_price
                        .and_then(|v| wire::price_from_json(&v))
                        .unwrap_or(Price::ZERO),
                    realized_pnl: p
                        .pnl
                        .and_then(|v| wire::decimal_from_json(&v))
                        .map(Money::inr),
                    last_price: p.last_price.and_then(|v| wire::price_from_json(&v)),
                }
            })
            .collect())
    }

    async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError> {
        let value = self.get_kite("/user/margins").await?;
        let segments: HashMap<String, KiteMarginSegment> = Self::kite_data(value)?;
        let equity = segments
            .get("equity")
            .ok_or_else(|| BrokerError::Transport("kite margins missing equity segment".into()))?;
        let available_cash = equity
            .available
            .get("cash")
            .and_then(wire::decimal_from_json)
            .or_else(|| {
                equity
                    .available
                    .get("live_balance")
                    .and_then(wire::decimal_from_json)
            })
            .unwrap_or(Decimal::ZERO);
        let used_margin = equity
            .utilised
            .values()
            .filter_map(wire::decimal_from_json)
            .sum::<Decimal>();
        let collateral = equity
            .available
            .get("collateral")
            .and_then(wire::decimal_from_json)
            .unwrap_or(Decimal::ZERO);
        Ok(BrokerMargins {
            available_cash: Money::inr(available_cash),
            used_margin: Money::inr(used_margin),
            collateral: Money::inr(collateral),
        })
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let value = self.get_kite(&format!("/orders/{order_id}")).await?;
        let orders: Vec<KiteOrderData> = Self::kite_data(value)?;
        if let Some(last) = orders.last() {
            return Ok(kite_status(&last.status));
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

/// Broker-native iceberg placement on the Kite `iceberg` order variety.
#[async_trait]
impl IcebergOrderExecutor for ZerodhaExecutor {
    async fn place_iceberg_order(
        &self,
        parent: OrderRequest,
        display_quantity: Quantity,
    ) -> Result<OrderAck, BrokerError> {
        let form = Self::build_iceberg_payload(&parent, self.product, display_quantity)?;
        let value = self
            .request_kite_traced(
                Method::POST,
                "/orders/iceberg",
                Some(form),
                parent.trace_context.as_ref(),
            )
            .await?;
        let data: PlaceOrderData = Self::kite_data(value)?;
        Ok(OrderAck {
            order_id: data.order_id,
            client_order_id: parent.client_order_id,
            exchange_order_id: None,
            status: OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: parent.correlation_id,
            client_id: parent.client_id.clone(),
            trace_context: parent.trace_context,
        })
    }
}

/// Construct a Kite iceberg order payload: the regular form legs plus the
/// documented `iceberg_quantity` (per-leg visible quantity) and derived
/// `iceberg_legs` count (2..=50).
impl ZerodhaExecutor {
    pub fn build_iceberg_payload(
        req: &OrderRequest,
        product: ProductType,
        display_quantity: Quantity,
    ) -> Result<HashMap<&'static str, String>, BrokerError> {
        let mut form = Self::build_order_payload(req, product)?;
        let legs = honba_te_core::validate_native_legs(req.quantity, display_quantity)?;
        form.insert("iceberg_legs", legs.to_string());
        form.insert(
            "iceberg_quantity",
            quantity_to_u32_exact(display_quantity.0)?.to_string(),
        );
        Ok(form)
    }
}

#[derive(Debug, serde::Deserialize)]
struct PlaceOrderData {
    order_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct KiteOrderData {
    status: String,
}

#[derive(Debug, serde::Deserialize)]
struct KiteOrder {
    order_id: String,
    #[serde(default)]
    exchange_order_id: Option<String>,
    exchange: String,
    tradingsymbol: String,
    transaction_type: String,
    status: String,
    product: String,
    order_type: String,
    #[serde(default)]
    quantity: Option<Value>,
    #[serde(default)]
    filled_quantity: Option<Value>,
    #[serde(default)]
    price: Option<Value>,
    #[serde(default)]
    trigger_price: Option<Value>,
    #[serde(default)]
    order_timestamp: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct KitePositions {
    #[serde(default)]
    net: Vec<KitePosition>,
}

#[derive(Debug, serde::Deserialize)]
struct KitePosition {
    tradingsymbol: String,
    exchange: String,
    product: String,
    #[serde(default)]
    quantity: Option<Value>,
    #[serde(default)]
    average_price: Option<Value>,
    #[serde(default)]
    pnl: Option<Value>,
    #[serde(default)]
    last_price: Option<Value>,
}

#[derive(Debug, serde::Deserialize)]
struct KiteMarginSegment {
    #[serde(default)]
    available: HashMap<String, Value>,
    #[serde(default)]
    utilised: HashMap<String, Value>,
}
