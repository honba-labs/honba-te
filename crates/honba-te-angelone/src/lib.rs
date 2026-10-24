//! Angel One SmartAPI 2.0 execution adapter.
//!
//! M2 full surface:
//! - REST: place, modify (read-then-patch over getOrderBook), cancel, order
//!   book, positions (getPositions), RMS margins — envelope-driven with the
//!   mandatory SmartAPI header bundle; exact [`rust_decimal::Decimal`]
//!   decoding (SmartAPI numeric fields arrive as decimal strings on both
//!   REST and stream paths).
//! - SmartStream order-update listener (`wss://tns.angelone.in/smart-order-update`):
//!   [`ws`].
//!
//! Error taxonomy: documented SmartAPI codes (`AB1004` invalid token → Auth,
//! `AB1010` request limit → RateLimited, `AB2001` checksum → InvalidRequest,
//! `AG8001` generic → Rejected); unknown codes default to Rejected
//! (community-documented behavior — overridable at the call site).

mod ws;

pub use ws::{
    AngelOrderUpdate, SMART_STREAM_WS_URL, SmartCorrelationLookup, SmartStreamConfig,
    SmartStreamOrderStream, parse_order_update_frame, spawn_order_updates,
    spawn_order_updates_with,
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

/// Angel One SmartAPI execution adapter.
pub struct AngeloneExecutor {
    /// SmartAPI key (X-PrivateKey)
    pub api_key: String,
    /// SmartAPI JWT session token
    pub jwt_token: String,
    /// Client code (e.g. "A123456")
    pub client_code: String,
    /// Base URL (configurable for testing)
    pub base_url: String,
    /// Default product used when an order carries no explicit taxonomy choice
    pub product: ProductType,
    client: reqwest::Client,
}

impl AngeloneExecutor {
    /// Default Angel One SmartAPI endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://apiconnect.angelone.in";

    /// Request timeout applied to every REST call.
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

    /// Create a new Angel One execution adapter (default product: DELIVERY).
    pub fn new(
        api_key: impl Into<String>,
        jwt_token: impl Into<String>,
        client_code: impl Into<String>,
    ) -> Self {
        Self::with_base_url(api_key, jwt_token, client_code, Self::DEFAULT_BASE_URL)
    }

    /// Create adapter with custom base URL.
    pub fn with_base_url(
        api_key: impl Into<String>,
        jwt_token: impl Into<String>,
        client_code: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            jwt_token: jwt_token.into(),
            client_code: client_code.into(),
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

    /// Wrap with the Angel One rate-limit preset.
    pub fn rate_limited(self) -> Result<RateLimitedBroker<Self>, BrokerError> {
        RateLimitedBroker::with_config(self, BrokerRateCatalog::for_broker("angelone"))
    }

    /// Build the mandatory SmartAPI header bundle.
    fn auth_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "X-PrivateKey",
            HeaderValue::from_str(&self.api_key)
                .map_err(|e| BrokerError::Auth(format!("invalid api key: {e}")))?,
        );
        let auth_val = HeaderValue::from_str(&format!("Bearer {}", self.jwt_token))
            .map_err(|e| BrokerError::Auth(format!("invalid jwt token: {e}")))?;
        headers.insert(AUTHORIZATION, auth_val);
        headers.insert("X-UserType", HeaderValue::from_static("USER"));
        headers.insert("X-SourceID", HeaderValue::from_static("WEB"));
        headers.insert("X-ClientLocalIP", HeaderValue::from_static("127.0.0.1"));
        headers.insert("X-ClientPublicIP", HeaderValue::from_static("127.0.0.1"));
        headers.insert(
            "X-MACAddress",
            HeaderValue::from_static("00:00:00:00:00:00"),
        );
        Ok(headers)
    }

    /// Build Angel One SmartAPI order payload.
    pub fn build_order_payload(
        req: &OrderRequest,
        product: ProductType,
    ) -> Result<AngeloneOrderPayload, BrokerError> {
        let (order_type, trigger_price) = match req.order_type {
            OrderType::Market => ("MARKET", None),
            OrderType::Limit(_) => ("LIMIT", None),
            OrderType::StopLoss(p) => ("STOPLOSS_MARKET", Some(p)),
            OrderType::StopLimit { trigger, .. } => ("STOPLOSS_LIMIT", Some(trigger)),
        };
        let price = match req.order_type {
            OrderType::Limit(p)
            | OrderType::StopLimit { limit: p, .. }
            | OrderType::StopLoss(p) => format_fixed_exact(p.0, 2)?,
            OrderType::Market => "0".to_string(),
        };
        let trigger = match trigger_price {
            Some(t) => format_fixed_exact(t.0, 2)?,
            None => "0".to_string(),
        };

        let trans_type = match req.side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let duration = match req.tif {
            TimeInForce::Day => "DAY",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Gtc => "DAY",
        };

        let symbol_token = req
            .instrument
            .token
            .clone()
            .unwrap_or_else(|| "0".to_string());

        Ok(AngeloneOrderPayload {
            variety: "NORMAL".to_string(),
            tradingsymbol: req.instrument.symbol.clone(),
            symboltoken: symbol_token,
            transactiontype: trans_type.to_string(),
            exchange: req.instrument.exchange.to_string(),
            ordertype: order_type.to_string(),
            producttype: product.angelone().to_string(),
            duration: duration.to_string(),
            price,
            triggerprice: trigger,
            squareoff: "0".to_string(),
            stoploss: "0".to_string(),
            quantity: req.quantity.0.to_string(),
            orderid: None,
        })
    }

    /// Send a SmartAPI REST request and return the parsed JSON body.
    ///
    /// Envelope `status: false` and non-2xx HTTP map through
    /// [`map_smartapi_error`]; non-JSON replies map to Transport.
    async fn request_smartapi(
        &self,
        method: Method,
        path: &str,
        json_body: Option<Value>,
    ) -> Result<Value, BrokerError> {
        self.request_smartapi_traced(method, path, json_body, None)
            .await
    }

    async fn request_smartapi_traced(
        &self,
        method: Method,
        path: &str,
        json_body: Option<Value>,
        trace_context: Option<&honba_core::TraceContext>,
    ) -> Result<Value, BrokerError> {
        let span = tracing::info_span!(
            "broker_http_request",
            broker = "angelone",
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
        let req = if let Some(body) = json_body {
            req.json(&body)
        } else {
            req
        };
        let started = Instant::now();
        let res = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                let duration = started.elapsed();
                honba_te_core::telemetry::record_http_request("angelone", path, 0, duration);
                return Err(BrokerError::Transport(e.to_string()));
            }
        };
        let status = res.status().as_u16();
        let duration = started.elapsed();
        span.record("http.status_code", status);
        honba_te_core::telemetry::record_http_request("angelone", path, status, duration);

        let text = res
            .text()
            .await
            .map_err(|e| BrokerError::Transport(format!("failed to read smartapi reply: {e}")))?;
        let value: Value = serde_json::from_str(&text).map_err(|_| {
            BrokerError::Transport(format!("non-JSON smartapi reply (HTTP {status})"))
        })?;
        let envelope_ok = value
            .get("status")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !envelope_ok || status >= 400 {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown smartapi error")
                .to_string();
            let code = value
                .get("errorcode")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            return Err(map_smartapi_error(status, &code, &message));
        }
        Ok(value)
    }

    fn smartapi_data<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, BrokerError> {
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        serde_json::from_value(data)
            .map_err(|e| BrokerError::Transport(format!("unparseable smartapi data: {e}")))
    }
}

/// Map SmartAPI errorcodes into the canonical [`BrokerError`] taxonomy.
///
/// `AB1004` invalid session token → Auth; `AB1010` request limit reached →
/// RateLimited; `AB2001` invalid checksum → InvalidRequest; `AG8001` generic
/// → Rejected. Message text carries the fallback signal (token/expiry phrasing
/// → Auth, rate-limit phrasing → RateLimited); unknown codes stay Rejected.
pub fn map_smartapi_error(status: u16, code: &str, message: &str) -> BrokerError {
    let message_u = message.to_uppercase();
    if status == 429 || code == "AB1010" || code == "AB1007" || message_u.contains("REQUEST LIMIT") || message_u.contains("RATE LIMIT") {
        return BrokerError::RateLimited(format!("{code}: {message}"));
    }
    if status == 401
        || status == 403
        || code == "AB1004"
        || code == "AB1011"
        || message_u.contains("TOKEN")
        || message_u.contains("EXPIRED")
    {
        return BrokerError::Auth(format!("{code}: {message}"));
    }
    if code == "AB2001" || status == 400 || message_u.contains("INVALID") {
        return BrokerError::InvalidRequest(format!("{code}: {message}"));
    }
    if status == 503 || code == "AB503" || message_u.contains("GATEWAY") || message_u.contains("MAINTENANCE") || message_u.contains("SERVER ERROR") {
        return BrokerError::Transport(format!("{code}: {message}"));
    }
    BrokerError::Rejected(format!("{code}: {message}"))
}

/// Angel One SmartAPI JSON order payload (exact decimal strings).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AngeloneOrderPayload {
    pub variety: String,
    pub tradingsymbol: String,
    pub symboltoken: String,
    pub transactiontype: String,
    pub exchange: String,
    pub ordertype: String,
    pub producttype: String,
    pub duration: String,
    pub price: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orderid: Option<String>,
    /// Stop-loss trigger price ("0" when not applicable)
    #[serde(default)]
    pub triggerprice: String,
    pub squareoff: String,
    pub stoploss: String,
    pub quantity: String,
}

fn angel_status(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "COMPLETE" => OrderStatus::Filled,
        "REJECTED" => OrderStatus::Rejected,
        "CANCELLED" => OrderStatus::Cancelled,
        "OPEN" | "MODIFIED" => OrderStatus::New,
        _ => OrderStatus::PendingNew,
    }
}

#[derive(Debug, serde::Deserialize)]
struct AngelProfile {
    #[serde(alias = "clientcode")]
    client_code: String,
}

/// SmartAPI order-book wire record: numeric legs are decimal strings.
#[derive(Debug, Clone, serde::Deserialize)]
struct SmartOrderDetail {
    #[serde(default)]
    orderid: Option<String>,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default)]
    symboltoken: Option<String>,
    #[serde(default)]
    producttype: Option<String>,
    #[serde(default)]
    tradingsymbol: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    triggerprice: Option<String>,
    #[serde(default)]
    quantity: Option<String>,
    #[serde(default)]
    transactiontype: Option<String>,
    #[serde(default)]
    ordertype: Option<String>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    filledshares: Option<String>,
    #[serde(default)]
    lastmodifiedtime: Option<String>,
    #[serde(default)]
    exchangetimestamp: Option<String>,
}

/// SmartAPI getPositions wire record.
#[derive(Debug, serde::Deserialize)]
struct SmartPosition {
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default)]
    symbolname: Option<String>,
    #[serde(default)]
    symboltoken: Option<String>,
    #[serde(default)]
    producttype: Option<String>,
    /// Net quantity, signed (short positions are negative)
    #[serde(default)]
    netqty: Option<String>,
    #[serde(default)]
    netprice: Option<String>,
    #[serde(default)]
    pnl: Option<String>,
}

/// Instrument with the broker token threaded through.
fn smart_instrument(
    exchange: Option<&str>,
    token: Option<&str>,
    symbol: Option<&str>,
) -> InstrumentId {
    let mut instrument = InstrumentId::new(exchange_of(exchange), canonical_symbol(symbol));
    instrument.token = token.map(|t| t.to_string());
    instrument
}

fn side_of(detail: &SmartOrderDetail) -> OrderSide {
    match detail
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

/// SmartAPI numeric strings; "-1" sentinels count as absent.
fn dec_str(value: Option<&str>) -> Option<Decimal> {
    value?
        .trim()
        .parse::<Decimal>()
        .ok()
        .filter(|d| !(d.trunc() == Decimal::from(-1) && d.fract().is_zero()))
}

fn order_type_of(detail: &SmartOrderDetail) -> OrderType {
    let price = dec_str(detail.price.as_deref()).unwrap_or(Decimal::ZERO);
    let trigger = dec_str(detail.triggerprice.as_deref()).unwrap_or(Decimal::ZERO);
    match detail.ordertype.as_deref().unwrap_or_default() {
        "LIMIT" => OrderType::Limit(Price::new(price)),
        "STOPLOSS_MARKET" => OrderType::StopLoss(Price::new(trigger)),
        "STOPLOSS_LIMIT" => OrderType::StopLimit {
            trigger: Price::new(trigger),
            limit: Price::new(price),
        },
        _ => OrderType::Market,
    }
}

fn exchange_of(text: Option<&str>) -> honba_core::Exchange {
    match text.unwrap_or_default().to_uppercase().as_str() {
        "BSE" => honba_core::Exchange::Bse,
        "MCX" => honba_core::Exchange::Mcx,
        // NFO/BFO (F&O venues) are cleared under the NSE/BSE umbrellas in the
        // canonical three-venue taxonomy.
        _ => honba_core::Exchange::Nse,
    }
}

/// SmartAPI tradingsymbols carry `-EQ` suffixes on cash tokens.
fn canonical_symbol(value: Option<&str>) -> String {
    let raw = value.unwrap_or_default().trim().to_uppercase();
    raw.strip_suffix("-EQ")
        .or_else(|| raw.strip_suffix("-BE"))
        .map(|s| s.to_string())
        .unwrap_or(raw)
}

fn quantity_of(detail: &SmartOrderDetail) -> Quantity {
    dec_str(detail.quantity.as_deref())
        .map(Quantity::new)
        .unwrap_or(Quantity::ZERO)
}

#[async_trait]
impl BrokerExecutor for AngeloneExecutor {
    fn name(&self) -> &str {
        "angelone"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let started = Instant::now();
        let body = self
            .request_smartapi(
                Method::POST,
                "/rest/secure/angelbroking/user/v1/getProfile",
                Some(serde_json::json!({})),
            )
            .await?;
        let latency = latency_millis(started.elapsed());
        let profile: AngelProfile = Self::smartapi_data(body)?;
        if profile.client_code.is_empty() {
            return Err(BrokerError::Auth("empty smartapi client code".into()));
        }
        Ok(BrokerHealth {
            broker: "angelone".into(),
            session_valid: true,
            authenticated_user: Some(profile.client_code),
            latency: Some(latency),
            detail: "smartapi session active".into(),
        })
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let payload = Self::build_order_payload(&req, self.product)?;
        let body = self
            .request_smartapi_traced(
                Method::POST,
                "/rest/secure/angelbroking/order/v1/placeOrder",
                Some(
                    serde_json::to_value(&payload)
                        .map_err(|e| BrokerError::InvalidRequest(format!("payload: {e}")))?,
                ),
                req.trace_context.as_ref(),
            )
            .await?;
        let data: OrderIdData = Self::smartapi_data(body)?;
        Ok(OrderAck {
            order_id: data.orderid,
            client_order_id: req.client_order_id,
            exchange_order_id: None,
            status: OrderStatus::New,
            timestamp: Utc::now(),
            correlation_id: req.correlation_id,
            client_id: req.client_id.clone(),
            trace_context: req.trace_context,
        })
    }

    /// SmartAPI modify carries the full order definition; the current order
    /// detail is read from the order book first, then patched with the
    /// changed legs.
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
        let book = self.fetch_order_book().await?;
        let current: Value = {
            let found = book
                .iter()
                .find(|o| o.order_id == order_id)
                .ok_or_else(|| BrokerError::OrderNotFound(order_id.to_string()))?;
            // Rebuild a full definition from the canonical snapshot.
            let t = match found.order_type {
                OrderType::Market => "MARKET",
                OrderType::Limit(_) => "LIMIT",
                OrderType::StopLoss(_) => "STOPLOSS_MARKET",
                OrderType::StopLimit { .. } => "STOPLOSS_LIMIT",
            };
            let price = match new_price {
                Some(p) => format_fixed_exact(p.0, 2)?,
                None => match found.order_type {
                    OrderType::Limit(p)
                    | OrderType::StopLimit { limit: p, .. }
                    | OrderType::StopLoss(p) => format_fixed_exact(p.0, 2)?,
                    OrderType::Market => "0".to_string(),
                },
            };
            let trigger = match found.order_type {
                OrderType::StopLoss(t) => format_fixed_exact(t.0, 2)?,
                OrderType::StopLimit { trigger, .. } => format_fixed_exact(trigger.0, 2)?,
                _ => "0".to_string(),
            };
            let qty = quantity_to_u32_exact(if let Some(q) = new_quantity {
                q.0
            } else {
                found.quantity.0
            })?;
            serde_json::json!({
                "variety": "NORMAL",
                "orderid": order_id,
                "exchange": format!("{}", found.instrument.exchange),
                "tradingsymbol": found.instrument.symbol,
                "symboltoken": found.instrument.token.clone().unwrap_or_else(|| "0".to_string()),
                "producttype": found.product.unwrap_or(self.product).angelone(),
                "transactiontype": match found.side { OrderSide::Sell => "SELL", OrderSide::Buy => "BUY" },
                "ordertype": t,
                "duration": "DAY",
                "price": price,
                "triggerprice": trigger,
                "quantity": qty.to_string()
            })
        };
        let body = self
            .request_smartapi(
                Method::PUT,
                "/rest/secure/angelbroking/order/v1/modifyOrder",
                Some(current),
            )
            .await?;
        let data: Value = Self::smartapi_data(body)?;
        let modified = data
            .get("orderid")
            .and_then(Value::as_str)
            .unwrap_or(order_id)
            .to_string();
        Ok(OrderAck::new(
            modified,
            uuid::Uuid::nil(),
            OrderStatus::New,
            Utc::now(),
        ))
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let _ = self
            .request_smartapi(
                Method::POST,
                "/rest/secure/angelbroking/order/v1/cancelOrder",
                Some(serde_json::json!({
                    "variety": "NORMAL",
                    "orderid": order_id
                })),
            )
            .await?;
        Ok(())
    }

    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        let body = self
            .request_smartapi(
                Method::GET,
                "/rest/secure/angelbroking/order/v1/getOrderBook",
                None,
            )
            .await?;
        let orders: Vec<SmartOrderDetail> = Self::smartapi_data(body)?;
        Ok(orders
            .into_iter()
            .map(|o| OrderSnapshot {
                order_id: o.orderid.clone().unwrap_or_default(),
                client_order_id: None,
                exchange_order_id: None,
                instrument: smart_instrument(
                    o.exchange.as_deref(),
                    o.symboltoken.as_deref(),
                    o.tradingsymbol.as_deref(),
                ),
                side: side_of(&o),
                order_type: order_type_of(&o),
                quantity: quantity_of(&o),
                filled_quantity: {
                    let filled = dec_str(o.filledshares.as_deref()).unwrap_or(Decimal::ZERO);
                    Quantity::new(filled.abs())
                },
                price: dec_str(o.price.as_deref()).map(Price::new),
                trigger_price: dec_str(o.triggerprice.as_deref()).map(Price::new),
                status: angel_status(&o.status),
                product: o
                    .producttype
                    .as_deref()
                    .and_then(ProductType::from_angelone),
                timestamp: o
                    .lastmodifiedtime
                    .as_deref()
                    .or(o.exchangetimestamp.as_deref())
                    .and_then(wire::parse_broker_ist_datetime)
                    .unwrap_or_else(Utc::now),
            })
            .collect())
    }

    async fn fetch_positions(&self) -> Result<Vec<BrokerPosition>, BrokerError> {
        let body = self
            .request_smartapi(
                Method::GET,
                "/rest/secure/angelbroking/order/v1/getPositions",
                None,
            )
            .await?;
        let positions: Vec<SmartPosition> = Self::smartapi_data(body)?;
        Ok(positions
            .into_iter()
            .map(|p| {
                let qty = p
                    .netqty
                    .as_deref()
                    .and_then(|s| s.trim().parse::<Decimal>().ok())
                    .unwrap_or(Decimal::ZERO);
                let side = if qty.is_zero() {
                    PositionSide::Flat
                } else if qty.is_sign_negative() {
                    PositionSide::Short
                } else {
                    PositionSide::Long
                };
                BrokerPosition {
                    instrument: smart_instrument(
                        p.exchange.as_deref(),
                        p.symboltoken.as_deref(),
                        p.symbolname.as_deref(),
                    ),
                    product: p
                        .producttype
                        .as_deref()
                        .and_then(ProductType::from_angelone)
                        .unwrap_or(self.product),
                    side,
                    quantity: Quantity::new(qty.abs()),
                    average_price: p
                        .netprice
                        .as_deref()
                        .and_then(|s| s.trim().parse::<Decimal>().ok())
                        .map(Price::new)
                        .unwrap_or(Price::ZERO),
                    realized_pnl: p
                        .pnl
                        .as_deref()
                        .and_then(|s| s.trim().parse::<Decimal>().ok())
                        .map(Money::inr),
                    last_price: None,
                }
            })
            .collect())
    }

    async fn fetch_margins(&self) -> Result<BrokerMargins, BrokerError> {
        // RMS envelope: equityamount/equitylimit/commodityamount (decimal strings).
        let body = self
            .request_smartapi(
                Method::GET,
                "/rest/secure/angelbroking/user/v1/getRms",
                None,
            )
            .await?;
        let data = value_or_zero(body.get("data"));
        let get = |key: &str| -> Decimal {
            data.get(key)
                .and_then(Value::as_str)
                .and_then(|s| s.trim().parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO)
        };
        let available_cash = if data.get("availablecash").is_some() {
            get("availablecash")
        } else {
            get("equityamount") + get("commodityamount")
        };
        let used_margin = if data.get("utiliseddebits").is_some() {
            get("utiliseddebits") + get("utilisedspan") + get("utilisedexposure")
        } else {
            let equity_limit = get("equitylimit");
            (equity_limit - get("equityamount")).max(Decimal::ZERO)
        };
        let collateral = get("collateral");
        Ok(BrokerMargins {
            available_cash: Money::inr(available_cash),
            used_margin: Money::inr(used_margin),
            collateral: Money::inr(collateral),
        })
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        // SmartAPI exposes no single-order detail GET for v2 REST; the order
        // book scan is the documented resolution path.
        let book = self.fetch_order_book().await?;
        if let Some(found) = book.iter().find(|o| o.order_id == order_id) {
            return Ok(found.status);
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

fn value_or_zero(value: Option<&Value>) -> Value {
    value.cloned().unwrap_or(Value::Null)
}

#[derive(Debug, serde::Deserialize)]
struct OrderIdData {
    orderid: String,
}

fn latency_millis(d: Duration) -> Decimal {
    Decimal::new(d.as_millis() as i64, 0)
}
