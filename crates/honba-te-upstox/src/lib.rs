//! Upstox v2 API execution adapter.
//!
//! All numeric wire payloads are stringified exact [`rust_decimal::Decimal`]
//! values — the previous `f64` representation is gone, eliminating
//! floating-point precision loss on order prices and quantities.

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::time::Instant;

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, TimeInForce};
use honba_te_core::decimal::{format_fixed_exact, quantity_to_u32_exact};
use honba_te_core::{BrokerError, BrokerExecutor, BrokerHealth, ProductType};

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

        let instrument_token =
            req.instrument.token.clone().unwrap_or_else(|| {
                format!("{}_EQ|{}", req.instrument.exchange, req.instrument.symbol)
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
        })
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<UpstoxResponse<T>, BrokerError> {
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

/// Upstox v2 order placement JSON request (exact decimal strings).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpstoxOrderPayload {
    pub quantity: u32,
    pub product: String,
    pub validity: String,
    /// Exact decimal price, e.g. "3800.00" (None for market orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub tag: Option<String>,
    pub instrument_token: String,
    pub order_type: String,
    pub transaction_type: String,
    pub disclosed_quantity: u32,
    /// Exact decimal trigger price (None for non-stop orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<String>,
    pub is_amo: bool,
}

/// Standard Upstox envelope.
#[derive(Debug, Deserialize)]
struct UpstoxResponse<T> {
    status: String,
    data: Option<T>,
    errors: Option<Vec<UpstoxError>>,
}

#[derive(Debug, Deserialize)]
struct UpstoxError {
    message: String,
    #[allow(dead_code)]
    error_code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpstoxOrderData {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct UpstoxProfile {
    user_id: String,
}

#[derive(Debug, Deserialize)]
struct UpstoxHistoryData {
    status: String,
}

fn upstox_status(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "COMPLETE" | "FILLED" => OrderStatus::Filled,
        "REJECTED" => OrderStatus::Rejected,
        "CANCELLED" => OrderStatus::Cancelled,
        "OPEN" | "PUT ORDER REQ RECEIVED" => OrderStatus::New,
        _ => OrderStatus::PendingNew,
    }
}

#[async_trait]
impl BrokerExecutor for UpstoxExecutor {
    fn name(&self) -> &str {
        "upstox"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let started = Instant::now();
        let body: UpstoxResponse<UpstoxProfile> = self.get_json("/user/profile").await?;
        let latency_ms = latency_millis(started.elapsed());

        if let Some(profile) = body.data
            && !profile.user_id.is_empty()
        {
            return Ok(BrokerHealth {
                broker: "upstox".into(),
                session_valid: true,
                authenticated_user: Some(profile.user_id),
                latency: Some(latency_ms),
                detail: "upstox session active".into(),
            });
        }
        let msg = body
            .errors
            .and_then(|errs| errs.into_iter().next())
            .map(|e| e.message)
            .unwrap_or_else(|| "unknown upstox profile error".into());
        Err(BrokerError::Auth(msg))
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let headers = self.auth_headers()?;
        let payload = Self::build_order_payload(&req, self.product)?;
        let url = format!("{}/order/place", self.base_url);

        let res = self
            .client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: UpstoxResponse<UpstoxOrderData> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(format!("invalid json: {e}")))?;

        if body.status == "success" {
            if let Some(d) = body.data {
                Ok(OrderAck {
                    order_id: d.order_id,
                    client_order_id: req.client_order_id,
                    exchange_order_id: None,
                    status: OrderStatus::New,
                    timestamp: Utc::now(),
                    correlation_id: req.correlation_id,
                    client_id: req.client_id.clone(),
                })
            } else {
                Err(BrokerError::Rejected("empty order data from upstox".into()))
            }
        } else {
            let msg = body
                .errors
                .and_then(|errs| errs.into_iter().next())
                .map(|e| e.message)
                .unwrap_or_else(|| "unknown upstox error".into());
            Err(BrokerError::Rejected(msg))
        }
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!("{}/order/cancel?order_id={}", self.base_url, order_id);

        let res = self
            .client
            .delete(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: UpstoxResponse<UpstoxOrderData> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        if body.status == "success" {
            Ok(())
        } else {
            Err(BrokerError::Rejected("cancel failed".into()))
        }
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let body: UpstoxResponse<Vec<UpstoxHistoryData>> =
            self.get_json(&format!("/order/details/{order_id}")).await?;

        if let Some(last) = body.data.as_ref().and_then(|h| h.last()) {
            return Ok(upstox_status(&last.status));
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

fn latency_millis(d: std::time::Duration) -> rust_decimal::Decimal {
    rust_decimal::Decimal::new(d.as_millis() as i64, 0)
}
