//! Upstox v2 API execution adapter.

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, TimeInForce};
use honba_te_core::{BrokerError, BrokerExecutor};

/// Upstox execution adapter.
pub struct UpstoxExecutor {
    /// Upstox API key
    pub api_key: String,
    /// OAuth Bearer access token
    pub access_token: String,
    /// Base URL (configurable for testing)
    pub base_url: String,
    client: reqwest::Client,
}

impl UpstoxExecutor {
    /// Default Upstox v2 endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://api.upstox.com/v2";

    /// Create a new Upstox execution adapter.
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
            client: reqwest::Client::new(),
        }
    }

    fn auth_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let auth_val = HeaderValue::from_str(&format!("Bearer {}", self.access_token))
            .map_err(|e| BrokerError::Auth(format!("invalid token format: {e}")))?;
        headers.insert(AUTHORIZATION, auth_val);
        Ok(headers)
    }

    /// Build Upstox order payload.
    pub fn build_order_payload(req: &OrderRequest) -> UpstoxOrderPayload {
        let (order_type, price, trigger_price) = match req.order_type {
            OrderType::Market => ("MARKET", 0.0, 0.0),
            OrderType::Limit(p) => ("LIMIT", p.0.to_string().parse().unwrap_or(0.0), 0.0),
            OrderType::StopLoss(p) => ("SL-M", 0.0, p.0.to_string().parse().unwrap_or(0.0)),
            OrderType::StopLimit { trigger, limit } => (
                "SL",
                limit.0.to_string().parse().unwrap_or(0.0),
                trigger.0.to_string().parse().unwrap_or(0.0),
            ),
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

        UpstoxOrderPayload {
            quantity: req.quantity.0.to_string().parse().unwrap_or(1),
            product: "D".to_string(), // Delivery
            validity: validity.to_string(),
            price,
            tag: Some(req.algo_id.clone()),
            instrument_token,
            order_type: order_type.to_string(),
            transaction_type: transaction_type.to_string(),
            disclosed_quantity: 0,
            trigger_price,
            is_amo: false,
        }
    }
}

/// Upstox order placement JSON request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstoxOrderPayload {
    pub quantity: i32,
    pub product: String,
    pub validity: String,
    pub price: f64,
    pub tag: Option<String>,
    pub instrument_token: String,
    pub order_type: String,
    pub transaction_type: String,
    pub disclosed_quantity: i32,
    pub trigger_price: f64,
    pub is_amo: bool,
}

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
struct UpstoxHistoryData {
    #[allow(dead_code)]
    order_id: String,
    status: String,
}

#[async_trait]
impl BrokerExecutor for UpstoxExecutor {
    fn name(&self) -> &str {
        "upstox"
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let headers = self.auth_headers()?;
        let payload = Self::build_order_payload(&req);
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
        let headers = self.auth_headers()?;
        let url = format!("{}/order/history?order_id={}", self.base_url, order_id);

        let res = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: UpstoxResponse<Vec<UpstoxHistoryData>> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        if let Some(last) = body.data.as_ref().and_then(|h| h.last()) {
            let st = match last.status.to_uppercase().as_str() {
                "COMPLETE" | "FILLED" => OrderStatus::Filled,
                "REJECTED" => OrderStatus::Rejected,
                "CANCELLED" => OrderStatus::Cancelled,
                "OPEN" | "PUT ORDER REQ RECEIVED" => OrderStatus::New,
                _ => OrderStatus::PendingNew,
            };
            return Ok(st);
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use honba_core::*;

    #[test]
    fn test_upstox_payload_construction() {
        let inst = InstrumentId::new(Exchange::Nse, "TCS");
        let req = OrderRequest::new(
            inst,
            OrderSide::Sell,
            OrderType::Limit(Price::from_rupees("3800.00").unwrap()),
            Quantity::from_u64(5),
            "algo-upstox",
        );

        let payload = UpstoxExecutor::build_order_payload(&req);
        assert_eq!(payload.transaction_type, "SELL");
        assert_eq!(payload.order_type, "LIMIT");
        assert_eq!(payload.quantity, 5);
        assert_eq!(payload.price, 3800.0);
        assert_eq!(payload.tag.as_deref(), Some("algo-upstox"));
    }
}
