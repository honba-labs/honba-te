//! CDSL eDIS pipeline tests: batching, submission, statuses, TPIN handling.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use honba_te_auth::edis::{
    CdslGateway, EdisEntry, EdisPreAuthorizer, EdisRequest, EdisStatus, HttpCdslGateway,
};
use honba_te_auth::error::AuthError;
use honba_te_auth::login::{AuthHttpClient, AuthHttpRequest, HttpReply};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::Mutex;

fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-22T02:30:00+00:00")
        .unwrap()
        .into()
}

fn entry(isin: &str, qty: u64) -> EdisEntry {
    EdisEntry {
        isin: isin.to_string(),
        quantity: Decimal::from(qty),
    }
}

// ---------------------------------------------------------------------------
// Mock CDSL gateway
// ---------------------------------------------------------------------------

struct RecordingGateway {
    statuses: Mutex<std::collections::VecDeque<Result<EdisStatus, AuthError>>>,
    submitted: Mutex<Vec<(EdisRequest, String)>>,
}

impl RecordingGateway {
    fn new(statuses: Vec<Result<EdisStatus, AuthError>>) -> Self {
        Self {
            statuses: Mutex::new(statuses.into()),
            submitted: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl CdslGateway for RecordingGateway {
    fn broker(&self) -> &str {
        "cdsl-mock"
    }

    async fn submit(&self, request: &EdisRequest, tpin: &str) -> Result<EdisStatus, AuthError> {
        self.submitted
            .lock()
            .await
            .push((request.clone(), tpin.to_string()));
        self.statuses
            .lock()
            .await
            .pop_front()
            .unwrap_or(Err(AuthError::Edis("script exhausted".into())))
    }

    async fn check(&self, _request: &EdisRequest) -> Result<EdisStatus, AuthError> {
        Err(AuthError::Edis("not scripted".into()))
    }
}

#[tokio::test]
async fn preauthorize_chunks_holdings_by_max_entries() {
    let gateway = Arc::new(RecordingGateway::new(vec![
        Ok(EdisStatus::Submitted),
        Ok(EdisStatus::Submitted),
        Ok(EdisStatus::Authorized),
    ]));
    let authorizer = EdisPreAuthorizer::new(gateway.clone()).with_max_entries(10);

    let holdings: Vec<EdisEntry> = (0..23).map(|i| entry(&format!("ISIN{i}"), 5 + i)).collect();
    let outcomes = authorizer
        .preauthorize("AB1234", "070164", &holdings, now())
        .await
        .unwrap();

    // 23 entries under max 10 → ceil = 3 batches, sizes 10/10/3.
    assert_eq!(outcomes.len(), 3);
    let submitted = gateway.submitted.lock().await;
    assert_eq!(submitted[0].0.entries.len(), 10);
    assert_eq!(submitted[1].0.entries.len(), 10);
    assert_eq!(submitted[2].0.entries.len(), 3);
    assert_eq!(submitted[0].1, "070164", "TPIN forwarded to the gateway");
    assert!(matches!(outcomes[0].status, EdisStatus::Submitted));
    assert!(matches!(outcomes[1].status, EdisStatus::Submitted));
    assert!(matches!(outcomes[2].status, EdisStatus::Authorized));
}

#[tokio::test]
async fn preauthorize_unsized_batches_use_default_50() {
    let gateway = Arc::new(RecordingGateway::new(vec![Ok(EdisStatus::Submitted)]));
    let authorizer = EdisPreAuthorizer::new(gateway.clone());

    assert_eq!(authorizer.batch_count(50), 1);
    assert_eq!(authorizer.batch_count(51), 2);
    assert_eq!(authorizer.batch_count(0), 0);
    // zero max is ignored (must never divide by zero).
    let lost = EdisPreAuthorizer::new(gateway.clone()).with_max_entries(0);
    assert_eq!(lost.batch_count(100), 2);

    let holdings: Vec<EdisEntry> = (0..50).map(|i| entry("INE", i)).collect();
    let outcomes = lost
        .preauthorize("AB1234", "070164", &holdings, now())
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(gateway.submitted.lock().await[0].0.entries.len(), 50);
}

#[tokio::test]
async fn preauthorize_requires_nonblank_tpin() {
    let gateway = RecordingGateway::new(vec![]);
    let authorizer = EdisPreAuthorizer::new(Arc::new(gateway));
    let err = authorizer
        .preauthorize("AB1234", "   ", &[entry("INE", 1)], now())
        .await
        .unwrap_err();
    assert!(
        matches!(err, AuthError::MissingTpin(ref c) if c == "AB1234"),
        "got {err:?}"
    );
}

#[tokio::test]
async fn preauthorize_propagates_gateway_errors_with_batch_context() {
    let gateway = Arc::new(RecordingGateway::new(vec![Err(AuthError::Edis(
        "tpin mismatch".into(),
    ))]));
    let authorizer = EdisPreAuthorizer::new(gateway);
    let err = authorizer
        .preauthorize("AB1234", "070164", &[entry("INE", 1)], now())
        .await
        .unwrap_err();
    match err {
        AuthError::Edis(detail) => {
            assert!(detail.contains("tpin mismatch"), "detail: {detail}");
            assert!(
                detail.contains("batch "),
                "detail must name the batch: {detail}"
            );
        }
        other => panic!("expected edis error, got {other:?}"),
    }
}

#[test]
fn edis_request_draft_has_unique_ids() {
    let r1 = EdisRequest::draft("AB1234", vec![entry("INE", 1)], now());
    let r2 = EdisRequest::draft("AB1234", vec![entry("INE", 2)], now());
    assert_ne!(r1.request_id, r2.request_id);
    assert!(matches!(r1.status, EdisStatus::Draft));
}

// ---------------------------------------------------------------------------
// HttpCdslGateway over the scripted transport
// ---------------------------------------------------------------------------

#[async_trait]
impl AuthHttpClient for ScriptedHttp {
    async fn execute(&self, req: AuthHttpRequest) -> Result<HttpReply, AuthError> {
        self.sent.lock().await.push(req);
        Ok(self.reply.lock().await.clone())
    }
}

struct ScriptedHttp {
    reply: Mutex<HttpReply>,
    sent: Mutex<Vec<AuthHttpRequest>>,
}

#[tokio::test]
async fn http_gateway_maps_wire_status_to_edis_status() {
    let http = Arc::new(ScriptedHttp {
        reply: Mutex::new(HttpReply {
            status: 200,
            body: r#"{"status":"authorized"}"#.into(),
        }),
        sent: Mutex::new(Vec::new()),
    });
    let gateway = HttpCdslGateway::new(http.clone(), "https://cdsl.local");

    let request = EdisRequest::draft("AB1234", vec![entry("INE467B01", 10)], now());
    let status = gateway.submit(&request, "070164").await.unwrap();
    assert!(matches!(status, EdisStatus::Authorized));

    // Wire payload carries client id, request id, TPIN, and entries.
    let sent = http.sent.lock().await;
    let payload = sent[0]
        .json
        .as_ref()
        .expect("ed is submit must be a json body");
    assert_eq!(payload["client_id"], "AB1234");
    assert_eq!(payload["tpin"], "070164");
    assert_eq!(payload["request_id"], request.request_id.to_string());
    assert_eq!(payload["entries"][0]["isin"], "INE467B01");
    // rust_decimal serializes Decimal entries as exact decimal strings.
    assert_eq!(payload["entries"][0]["quantity"], "10");
}

#[tokio::test]
async fn http_gateway_maps_rejected_with_reason() {
    let http = Arc::new(ScriptedHttp {
        reply: Mutex::new(HttpReply {
            status: 200,
            body: r#"{"status":"rejected","message":"TPIN invalid"}"#.into(),
        }),
        sent: Mutex::new(Vec::new()),
    });
    let gateway = HttpCdslGateway::new(http, "https://cdsl.local");
    let status = gateway
        .submit(&EdisRequest::draft("AB1234", vec![], now()), "000000")
        .await
        .unwrap();
    match status {
        EdisStatus::Rejected { reason } => assert_eq!(reason, "TPIN invalid"),
        other => panic!("expected rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn http_gateway_surfaces_http_failures_as_edis_errors() {
    let http = Arc::new(ScriptedHttp {
        reply: Mutex::new(HttpReply {
            status: 502,
            body: "bad gateway".into(),
        }),
        sent: Mutex::new(Vec::new()),
    });
    let gateway = HttpCdslGateway::new(http, "https://cdsl.local");
    let err = gateway
        .submit(&EdisRequest::draft("AB1234", vec![], now()), "070164")
        .await
        .unwrap_err();
    match err {
        AuthError::Edis(detail) => assert!(detail.contains("502")),
        other => panic!("expected edis error, got {other:?}"),
    }
}

#[tokio::test]
async fn http_gateway_status_poll_uses_check_endpoint() {
    let http = Arc::new(ScriptedHttp {
        reply: Mutex::new(HttpReply {
            status: 200,
            body: r#"{"status":"submitted"}"#.into(),
        }),
        sent: Mutex::new(Vec::new()),
    });
    let gateway = HttpCdslGateway::new(http.clone(), "https://cdsl.local");
    let request = EdisRequest::draft("AB1234", vec![], now());
    let status = gateway.check(&request).await.unwrap();
    assert!(matches!(status, EdisStatus::Submitted));
    let sent = http.sent.lock().await;
    assert!(sent[0].url.ends_with("/edis/status"));
    assert_eq!(
        sent[0].json.as_ref().unwrap()["request_id"],
        request.request_id.to_string()
    );
}
