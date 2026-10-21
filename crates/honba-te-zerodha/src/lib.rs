//! Zerodha (Kite Connect v3) execution adapter.

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use std::collections::HashMap;

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, TimeInForce};
use honba_te_core::{BrokerError, BrokerExecutor};

/// Zerodha Kite Connect execution adapter.
pub struct ZerodhaExecutor {
    /// Zerodha API key
    pub api_key: String,
    /// User session access token
    pub access_token: String,
    /// Base URL (configurable for testing)
    pub base_url: String,
    /// HTTP client
    client: reqwest::Client,
}

impl ZerodhaExecutor {
    /// Default Kite Connect API endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://api.kite.trade";

    /// Create a new Zerodha execution adapter.
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
            client: reqwest::Client::new(),
        }
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

    /// Convert canonical OrderRequest into Zerodha form payload.
    pub fn build_order_payload(req: &OrderRequest) -> HashMap<&'static str, String> {
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
            OrderType::Limit(p) => ("LIMIT", Some(p.0.to_string()), None),
            OrderType::StopLoss(p) => ("SL-M", None, Some(p.0.to_string())),
            OrderType::StopLimit { trigger, limit } => {
                ("SL", Some(limit.0.to_string()), Some(trigger.0.to_string()))
            }
        };
        form.insert("order_type", order_type_str.to_string());
        if let Some(p) = price {
            form.insert("price", p);
        }
        if let Some(t) = trigger {
            form.insert("trigger_price", t);
        }

        form.insert("quantity", req.quantity.0.to_string());
        form.insert("product", "CNC".to_string()); // Default cash delivery

        let validity = match req.tif {
            TimeInForce::Day => "DAY",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Gtc => "TTL",
        };
        form.insert("validity", validity.to_string());
        form.insert("tag", req.algo_id.clone());

        form
    }
}

#[derive(Debug, Deserialize)]
struct KiteResponse<T> {
    status: String,
    data: Option<T>,
    message: Option<String>,
    error_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrderData {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct OrderStatusData {
    #[allow(dead_code)]
    order_id: String,
    status: String,
}

#[async_trait]
impl BrokerExecutor for ZerodhaExecutor {
    fn name(&self) -> &str {
        "zerodha"
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let headers = self.auth_headers()?;
        let form = Self::build_order_payload(&req);
        let url = format!("{}/orders/regular", self.base_url);

        let res = self
            .client
            .post(&url)
            .headers(headers)
            .form(&form)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: KiteResponse<OrderData> = res
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

        let body: KiteResponse<OrderData> = res
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

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!("{}/orders/{}", self.base_url, order_id);

        let res = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: KiteResponse<Vec<OrderStatusData>> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        if let Some(last) = body.data.as_ref().and_then(|l| l.last()) {
            let st = match last.status.to_uppercase().as_str() {
                "COMPLETE" => OrderStatus::Filled,
                "REJECTED" => OrderStatus::Rejected,
                "CANCELLED" => OrderStatus::Cancelled,
                "OPEN" => OrderStatus::New,
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
    fn test_zerodha_payload_construction() {
        let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
        let req = OrderRequest::new(
            inst,
            OrderSide::Buy,
            OrderType::Limit(Price::from_rupees("2500.50").unwrap()),
            Quantity::from_u64(10),
            "algo-alpha",
        );

        let payload = ZerodhaExecutor::build_order_payload(&req);
        assert_eq!(payload.get("tradingsymbol").unwrap(), "RELIANCE");
        assert_eq!(payload.get("exchange").unwrap(), "NSE");
        assert_eq!(payload.get("transaction_type").unwrap(), "BUY");
        assert_eq!(payload.get("order_type").unwrap(), "LIMIT");
        assert_eq!(payload.get("price").unwrap(), "2500.50");
        assert_eq!(payload.get("quantity").unwrap(), "10");
        assert_eq!(payload.get("tag").unwrap(), "algo-alpha");
    }
}
