//! Angel One (SmartAPI) execution adapter.

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, TimeInForce};
use honba_te_core::{BrokerError, BrokerExecutor};

/// Angel One SmartAPI execution adapter.
pub struct AngeloneExecutor {
    /// SmartAPI key
    pub api_key: String,
    /// SmartAPI JWT session token
    pub jwt_token: String,
    /// Client code (e.g. "A123456")
    pub client_code: String,
    /// Base URL (configurable for testing)
    pub base_url: String,
    client: reqwest::Client,
}

impl AngeloneExecutor {
    /// Default Angel One SmartAPI endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://apiconnect.angelone.in";

    /// Create a new Angel One execution adapter.
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
            client: reqwest::Client::new(),
        }
    }

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
    pub fn build_order_payload(req: &OrderRequest) -> AngeloneOrderPayload {
        let (order_type, price) = match req.order_type {
            OrderType::Market => ("MARKET", "0".to_string()),
            OrderType::Limit(p) => ("LIMIT", p.0.to_string()),
            OrderType::StopLoss(p) => ("STOPLOSS_MARKET", p.0.to_string()),
            OrderType::StopLimit { limit, .. } => ("STOPLOSS_LIMIT", limit.0.to_string()),
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

        AngeloneOrderPayload {
            variety: "NORMAL".to_string(),
            tradingsymbol: req.instrument.symbol.clone(),
            symboltoken: symbol_token,
            transactiontype: trans_type.to_string(),
            exchange: req.instrument.exchange.to_string(),
            ordertype: order_type.to_string(),
            producttype: "DELIVERY".to_string(),
            duration: duration.to_string(),
            price,
            squareoff: "0".to_string(),
            stoploss: "0".to_string(),
            quantity: req.quantity.0.to_string(),
        }
    }
}

/// Angel One SmartAPI JSON order payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub squareoff: String,
    pub stoploss: String,
    pub quantity: String,
}

#[derive(Debug, Deserialize)]
struct AngeloneResponse<T> {
    status: bool,
    message: String,
    #[serde(rename = "errorcode")]
    error_code: Option<String>,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct AngeloneOrderData {
    orderid: String,
}

#[async_trait]
impl BrokerExecutor for AngeloneExecutor {
    fn name(&self) -> &str {
        "angelone"
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let headers = self.auth_headers()?;
        let payload = Self::build_order_payload(&req);
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/placeOrder",
            self.base_url
        );

        let res = self
            .client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: AngeloneResponse<AngeloneOrderData> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(format!("invalid json: {e}")))?;

        if body.status {
            if let Some(d) = body.data {
                Ok(OrderAck {
                    order_id: d.orderid,
                    client_order_id: req.client_order_id,
                    exchange_order_id: None,
                    status: OrderStatus::New,
                    timestamp: Utc::now(),
                    correlation_id: req.correlation_id,
                    client_id: req.client_id.clone(),
                })
            } else {
                Err(BrokerError::Rejected(
                    "empty order payload from angelone".into(),
                ))
            }
        } else {
            Err(BrokerError::Rejected(format!(
                "{}: {}",
                body.error_code.unwrap_or_default(),
                body.message
            )))
        }
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/cancelOrder",
            self.base_url
        );
        let payload = serde_json::json!({
            "variety": "NORMAL",
            "orderid": order_id
        });

        let res = self
            .client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: AngeloneResponse<serde_json::Value> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        if body.status {
            Ok(())
        } else {
            Err(BrokerError::Rejected(body.message))
        }
    }

    async fn get_order_status(&self, order_id: &str) -> Result<OrderStatus, BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/details/{}",
            self.base_url, order_id
        );

        let res = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let body: AngeloneResponse<serde_json::Value> = res
            .json()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;

        let status_val = if body.status {
            body.data.as_ref().and_then(|d| d.get("status"))
        } else {
            None
        };

        if let Some(st_val) = status_val {
            let st_str = st_val.as_str().unwrap_or("").to_uppercase();
            let status = match st_str.as_str() {
                "COMPLETE" => OrderStatus::Filled,
                "REJECTED" => OrderStatus::Rejected,
                "CANCELLED" => OrderStatus::Cancelled,
                "OPEN" => OrderStatus::New,
                _ => OrderStatus::PendingNew,
            };
            return Ok(status);
        }
        Err(BrokerError::OrderNotFound(order_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use honba_core::*;

    #[test]
    fn test_angelone_payload_construction() {
        let inst = InstrumentId::new(Exchange::Nse, "HDFCBANK").with_token("1333");
        let req = OrderRequest::new(
            inst,
            OrderSide::Buy,
            OrderType::Limit(Price::from_rupees("1650.00").unwrap()),
            Quantity::from_u64(20),
            "algo-angel",
        );

        let payload = AngeloneExecutor::build_order_payload(&req);
        assert_eq!(payload.tradingsymbol, "HDFCBANK");
        assert_eq!(payload.symboltoken, "1333");
        assert_eq!(payload.transactiontype, "BUY");
        assert_eq!(payload.ordertype, "LIMIT");
        assert_eq!(payload.quantity, "20");
        assert_eq!(payload.price, "1650.00");
    }
}
