//! Zerodha (Kite Connect v3) execution adapter.
//!
//! Canonical [`OrderRequest`] values are translated into Kite form payloads
//! with exact decimal numbers (never floating point) and the canonical Indian
//! [`ProductType`] taxonomy (CNC/MIS/NRML/MTF).

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::HashMap;
use std::time::Instant;

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, Price, TimeInForce};
use honba_te_core::decimal::{format_fixed_exact, quantity_to_u32_exact};
use honba_te_core::{BrokerError, BrokerExecutor, BrokerHealth, OrderSnapshot, ProductType};

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
    /// HTTP client
    client: reqwest::Client,
}

impl ZerodhaExecutor {
    /// Default Kite Connect API endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://api.kite.trade";

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
            client: reqwest::Client::new(),
        }
    }

    /// Override the default product taxonomy for orders placed through this adapter.
    pub fn with_product(mut self, product: ProductType) -> Self {
        self.product = product;
        self
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

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<KiteResponse<T>, BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!("{}{}", self.base_url, path);
        let res = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;
        res.json()
            .await
            .map_err(|e| BrokerError::Transport(format!("failed to parse json response: {e}")))
    }
}

/// Standard Kite envelope: `{"status": "success"|"error", "data": ...}`.
#[derive(Debug, serde::Deserialize)]
struct KiteResponse<T> {
    status: String,
    data: Option<T>,
    message: Option<String>,
    error_type: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct OrderData {
    status: String,
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

#[async_trait]
impl BrokerExecutor for ZerodhaExecutor {
    fn name(&self) -> &str {
        "zerodha"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let started = Instant::now();
        let body: KiteResponse<UserProfile> = self.get_json("/user/profile").await?;
        let latency_ms = latency_millis(started.elapsed());
        match (body.status.as_str(), body.data) {
            ("success", Some(profile)) => Ok(BrokerHealth {
                broker: "zerodha".into(),
                session_valid: true,
                authenticated_user: Some(profile.user_id),
                latency: Some(latency_ms),
                detail: "kite session active".into(),
            }),
            ("error", _) | (_, None) => {
                let msg = body
                    .message
                    .unwrap_or_else(|| "unknown kite profile error".into());
                if body.error_type.as_deref() == Some("TokenException") {
                    Err(BrokerError::Auth(msg))
                } else {
                    Err(BrokerError::Rejected(msg))
                }
            }
            _ => Ok(BrokerHealth::session_ok(
                "zerodha",
                format!("unexpected status {:?}", body.status),
            )),
        }
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let headers = self.auth_headers()?;
        let form = Self::build_order_payload(&req, self.product)?;
        let url = format!("{}/orders/regular", self.base_url);

        let res = self
            .client
            .post(&url)
            .headers(headers)
            .form(&form)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: KiteResponse<PlaceOrderData> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(format!("failed to parse json response: {e}")))?;

        if body.status == "success" {
            if let Some(data) = body.data {
                Ok(OrderAck {
                    order_id: data.order_id,
                    client_order_id: req.client_order_id,
                    exchange_order_id: None,
                    status: OrderStatus::New,
                    timestamp: Utc::now(),
                    correlation_id: req.correlation_id,
                    client_id: req.client_id.clone(),
                })
            } else {
                Err(BrokerError::Rejected("empty data payload from Kite".into()))
            }
        } else {
            let msg = body.message.unwrap_or_else(|| "unknown kite error".into());
            if body.error_type.as_deref() == Some("TokenException") {
                Err(BrokerError::Auth(msg))
            } else {
                Err(BrokerError::Rejected(msg))
            }
        }
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!("{}/orders/regular/{}", self.base_url, order_id);

        let res = self
            .client
            .delete(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: KiteResponse<PlaceOrderData> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        if body.status == "success" {
            Ok(())
        } else {
            Err(BrokerError::Rejected(
                body.message.unwrap_or_else(|| "cancel failed".into()),
            ))
        }
    }

    async fn fetch_order_book(&self) -> Result<Vec<OrderSnapshot>, BrokerError> {
        let body: KiteResponse<Vec<KiteOrder>> = self.get_json("/orders").await?;
        let data = body
            .data
            .ok_or_else(|| BrokerError::Rejected("empty order book".into()))?;
        Ok(data
            .into_iter()
            .map(|o| OrderSnapshot {
                order_id: o.order_id,
                client_order_id: None,
                exchange_order_id: o.exchange_order_id,
                instrument: honba_core::InstrumentId::new(
                    o.exchange.parse().unwrap_or(honba_core::Exchange::Nse),
                    o.tradingsymbol,
                ),
                side: match o.transaction_type.as_str() {
                    "SELL" => OrderSide::Sell,
                    _ => OrderSide::Buy,
                },
                order_type: match o.order_type.as_str() {
                    "MARKET" => OrderType::Market,
                    "SL-M" => OrderType::StopLoss(Price::ZERO),
                    "SL" => OrderType::StopLimit {
                        trigger: Price::ZERO,
                        limit: Price::ZERO,
                    },
                    _ => OrderType::Market,
                },
                quantity: honba_core::Quantity::new(o.quantity),
                filled_quantity: honba_core::Quantity::new(o.filled_quantity),
                price: o.price.map(Price::new),
                trigger_price: o.trigger_price.map(Price::new),
                status: kite_status(&o.status),
                product: ProductType::from_zerodha(&o.product),
                timestamp: Utc::now(),
            })
            .collect())
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let body: KiteResponse<Vec<OrderData>> =
            self.get_json(&format!("/orders/{order_id}")).await?;

        if let Some(last) = body.data.as_ref().and_then(|l| l.last()) {
            return Ok(kite_status(&last.status));
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

fn latency_millis(d: std::time::Duration) -> rust_decimal::Decimal {
    rust_decimal::Decimal::new(d.as_millis() as i64, 0)
}

#[derive(Debug, serde::Deserialize)]
struct PlaceOrderData {
    order_id: String,
}

#[derive(Debug, serde::Deserialize)]
struct KiteOrder {
    order_id: String,
    exchange_order_id: Option<String>,
    exchange: String,
    tradingsymbol: String,
    transaction_type: String,
    status: String,
    product: String,
    order_type: String,
    quantity: rust_decimal::Decimal,
    #[serde(default)]
    filled_quantity: rust_decimal::Decimal,
    #[serde(default)]
    price: Option<rust_decimal::Decimal>,
    #[serde(default)]
    trigger_price: Option<rust_decimal::Decimal>,
}
