//! CDSL eDIS demat-delivery authorization pipeline.
//!
//! SEBI requires CDSL eDIS authorization (TPIN-based) before equity delivery
//! (`CncDelivery`) sells. The [`EdisPreAuthorizer`] batches the day's
//! authorized holdings in the pre-market window so intraday sells never block
//! on TPIN entry during high-speed market hours.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::error::AuthError;
/// One ISIN + quantity pair to authorize for delivery sale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdisEntry {
    /// ISIN (e.g. INE467B01029)
    pub isin: String,
    /// Quantity to authorize (exact decimal; integral share count)
    pub quantity: Decimal,
}

/// Lifecycle status of an eDIS authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdisStatus {
    /// Prepared, not yet submitted to CDSL.
    Draft,
    /// Submitted to CDSL, awaiting TPIN/OTP completion.
    Submitted,
    /// Authorization accepted by CDSL.
    Authorized,
    /// Rejected (TPIN invalid, holdings mismatch, ...).
    Rejected {
        /// Rejection reason from CDSL
        reason: String,
    },
    /// Transport / processing failure.
    Errored {
        /// Error detail
        detail: String,
    },
}

/// An eDIS authorization batch for one client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdisRequest {
    /// Unique request id (client-side correlation).
    pub request_id: Uuid,
    /// Broker client id.
    pub client_id: String,
    /// ISIN/quantity entries in this batch.
    pub entries: Vec<EdisEntry>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// CDSL-observed status.
    pub status: EdisStatus,
}

impl EdisRequest {
    /// A fresh draft batch.
    pub fn draft(client_id: &str, entries: Vec<EdisEntry>, now: DateTime<Utc>) -> Self {
        Self {
            request_id: Uuid::new_v4(),
            client_id: client_id.to_string(),
            entries,
            created_at: now,
            status: EdisStatus::Draft,
        }
    }
}

/// Gateway to the CDSL eDIS authorization endpoint (broker-wrapped).
#[async_trait]
pub trait CdslGateway: Send + Sync {
    /// Broker identifier the gateway is registered for.
    fn broker(&self) -> &str;

    /// Submit a TPIN-authorized batch. Returns the observed status.
    async fn submit(&self, request: &EdisRequest, tpin: &str) -> Result<EdisStatus, AuthError>;

    /// Poll CDSL status for a previously submitted request.
    async fn check(&self, request: &EdisRequest) -> Result<EdisStatus, AuthError>;
}

/// Outcome records for the morning pre-authorization batch.
#[derive(Debug, Clone)]
pub struct EdisOutcome {
    /// The request that was submitted.
    pub request_id: Uuid,
    /// Status observed by the gateway.
    pub status: EdisStatus,
}

/// Default HTTP DE gateway talking the CDSL-style JSON contract
/// (`POST {base}/edis/authorize` with client id + TPIN + entries).
pub struct HttpCdslGateway {
    http: Arc<dyn crate::login::AuthHttpClient>,
    base_url: String,
}

impl HttpCdslGateway {
    /// Gateway against the given CDSL base (overridable for WireMock).
    pub fn new(http: Arc<dyn crate::login::AuthHttpClient>, base_url: impl Into<String>) -> Self {
        Self {
            http,
            base_url: base_url.into(),
        }
    }
}

#[async_trait]
impl CdslGateway for HttpCdslGateway {
    fn broker(&self) -> &str {
        "cdsl"
    }

    async fn submit(&self, request: &EdisRequest, tpin: &str) -> Result<EdisStatus, AuthError> {
        let reply = self
            .http
            .execute(crate::login::AuthHttpRequest {
                method: "POST",
                url: format!("{}/edis/authorize", self.base_url),
                headers: vec![("Content-Type".into(), "application/json".into())],
                form: None,
                json: Some(serde_json::json!({
                    "client_id": request.client_id,
                    "request_id": request.request_id.to_string(),
                    "tpin": tpin,
                    "entries": request.entries,
                })),
            })
            .await?;
        ensure_2xx(&reply)?;
        let json = reply.json().map_err(|e| AuthError::Edis(e.to_string()))?;
        let status = json
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        Ok(match status {
            "authorized" => EdisStatus::Authorized,
            "submitted" => EdisStatus::Submitted,
            "rejected" => EdisStatus::Rejected {
                reason: json
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("dis authorization rejected")
                    .to_string(),
            },
            other => EdisStatus::Errored {
                detail: format!("unexpected edis status '{other}'"),
            },
        })
    }

    async fn check(&self, request: &EdisRequest) -> Result<EdisStatus, AuthError> {
        let reply = self
            .http
            .execute(crate::login::AuthHttpRequest {
                method: "POST",
                url: format!("{}/edis/status", self.base_url),
                headers: vec![("Content-Type".into(), "application/json".into())],
                form: None,
                json: Some(serde_json::json!({
                    "request_id": request.request_id.to_string(),
                })),
            })
            .await?;
        ensure_2xx(&reply)?;
        let json = reply.json().map_err(|e| AuthError::Edis(e.to_string()))?;
        let status = json
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        Ok(parse_status(status, &json))
    }
}

fn ensure_2xx(reply: &crate::login::HttpReply) -> Result<(), AuthError> {
    if reply.status >= 400 {
        return Err(AuthError::Edis(format!(
            "cdsl rejected request [status {}]: {}",
            reply.status,
            reply.truncated()
        )));
    }
    Ok(())
}

fn parse_status(status: &str, json: &serde_json::Value) -> EdisStatus {
    match status {
        "authorized" => EdisStatus::Authorized,
        "submitted" => EdisStatus::Submitted,
        "rejected" => EdisStatus::Rejected {
            reason: json
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("dis authorize rejected")
                .to_string(),
        },
        other => EdisStatus::Errored {
            detail: format!("unexpected edis status '{other}'"),
        },
    }
}

/// Morning batch pre-authorization engine.
pub struct EdisPreAuthorizer {
    gateway: Arc<dyn CdslGateway>,
    /// Maximum entries per CDSL request (provider limits, default 50).
    max_entries_per_request: usize,
}

impl EdisPreAuthorizer {
    /// Authorizer over a gateway with the default batch size.
    pub fn new(gateway: Arc<dyn CdslGateway>) -> Self {
        Self {
            gateway,
            max_entries_per_request: 50,
        }
    }

    /// Override the max entries per request.
    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        if max_entries > 0 {
            self.max_entries_per_request = max_entries;
        }
        self
    }

    /// Batch size produced for `total_entries`.
    pub fn batch_count(&self, total_entries: usize) -> usize {
        total_entries.div_ceil(self.max_entries_per_request.max(1))
    }

    /// Morning pre-authorization: chunk `holdings` into CDSL-sized batches,
    /// submit each with the client TPIN, and report per-batch outcomes.
    pub async fn preauthorize(
        &self,
        client_id: &str,
        tpin: &str,
        holdings: &[EdisEntry],
        now: DateTime<Utc>,
    ) -> Result<Vec<EdisOutcome>, AuthError> {
        if tpin.trim().is_empty() {
            return Err(AuthError::MissingTpin(client_id.to_string()));
        }

        let mut outcomes = Vec::with_capacity(self.batch_count(holdings.len()));
        for chunk in holdings.chunks(self.max_entries_per_request) {
            let request = EdisRequest::draft(client_id, chunk.to_vec(), now);
            let status = self.gateway.submit(&request, tpin).await.map_err(|e| {
                let mut e = e;
                if let AuthError::Edis(detail) = &mut e {
                    *detail = format!("batch {}: {detail}", request.request_id);
                }
                e
            })?;
            tracing::info!(
                "eDIS batch {}: {} entries → {:?}",
                request.request_id,
                request.entries.len(),
                status
            );
            outcomes.push(EdisOutcome {
                request_id: request.request_id,
                status,
            });
        }
        Ok(outcomes)
    }
}
