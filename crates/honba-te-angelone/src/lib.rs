//! Angel One SmartAPI 2.0 execution adapter with the canonical [`ProductType`]
//! taxonomy and session health probing.

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::time::Instant;

use honba_core::{OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType, TimeInForce};
use honba_te_core::decimal::format_fixed_exact;
use honba_te_core::{BrokerError, BrokerExecutor, BrokerHealth, ProductType};

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
    /// Default product used when an order carries no explicit taxonomy choice
    pub product: ProductType,
    client: reqwest::Client,
}

impl AngeloneExecutor {
    /// Default Angel One SmartAPI endpoint.
    pub const DEFAULT_BASE_URL: &'static str = "https://apiconnect.angelone.in";

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
            client: reqwest::Client::new(),
        }
    }

    /// Override the default product taxonomy for orders placed through this adapter.
    pub fn with_product(mut self, product: ProductType) -> Self {
        self.product = product;
        self
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
        let (order_type, price) = match req.order_type {
            OrderType::Market => ("MARKET", None),
            OrderType::Limit(p) => ("LIMIT", Some(p)),
            OrderType::StopLoss(p) => ("STOPLOSS_MARKET", Some(p)),
            OrderType::StopLimit { limit, .. } => ("STOPLOSS_LIMIT", Some(limit)),
        };
        let price = match price {
            Some(p) => format_fixed_exact(p.0, 2)?,
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
            squareoff: "0".to_string(),
            stoploss: "0".to_string(),
            quantity: req.quantity.0.to_string(),
        })
    }

    async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<AngeloneResponse<T>, BrokerError> {
        let headers = self.auth_headers()?;
        let url = format!("{}{}", self.base_url, path);
        let res = self
            .client
            .post(&url)
            .headers(headers)
            .json(body)
            .send()
            .await
            .map_err(|e| BrokerError::Transport(e.to_string()))?;
        res.json()
            .await
            .map_err(|e| BrokerError::Transport(format!("failed to parse json response: {e}")))
    }
}

/// Angel One SmartAPI JSON order payload (exact decimal strings).
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

#[derive(Debug, Deserialize)]
struct AngeloneProfile {
    #[serde(alias = "clientcode")]
    client_code: String,
}

#[async_trait]
impl BrokerExecutor for AngeloneExecutor {
    fn name(&self) -> &str {
        "angelone"
    }

    async fn health_check(&self) -> Result<BrokerHealth, BrokerError> {
        let started = Instant::now();
        let body: AngeloneResponse<AngeloneProfile> = self
            .post_json(
                "/rest/secure/angelbroking/user/v1/getProfile",
                &serde_json::json!({"status": "true"}),
            )
            .await?;
        let latency_ms = rust_decimal::Decimal::new(started.elapsed().as_millis() as i64, 0);

        if body.status
            && let Some(profile) = body.data
        {
            return Ok(BrokerHealth {
                broker: "angelone".into(),
                session_valid: true,
                authenticated_user: Some(profile.client_code),
                latency: Some(latency_ms),
                detail: "smartapi session active".into(),
            });
        }
        // Explicit SmartAPI error code taxonomy surfaces through Auth for
        // AB1004 (invalid token) style refusals.
        let code = body.error_code.unwrap_or_default();
        let msg = format!("{}: {}", code, body.message);
        if code.starts_with("AB100") {
            Err(BrokerError::Auth(msg))
        } else {
            Err(BrokerError::Rejected(msg))
        }
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        let payload = Self::build_order_payload(&req, self.product)?;
        let body: AngeloneResponse<AngeloneOrderData> = self
            .post_json(
                "/rest/secure/angelbroking/order/v1/placeOrder",
                &serde_json::to_value(&payload)
                    .map_err(|e| BrokerError::InvalidRequest(format!("payload: {e}")))?,
            )
            .await?;

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
        let body: AngeloneResponse<serde_json::Value> = self
            .post_json(
                "/rest/secure/angelbroking/order/v1/cancelOrder",
                &serde_json::json!({
                    "variety": "NORMAL",
                    "orderid": order_id
                }),
            )
            .await?;

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
