//! Headless login flow conformance tests driven by a scripted HTTP mock.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use honba_te_auth::credentials::BrokerCredentials;
use honba_te_auth::crypto::{angelone_password_b64, base64_encode, sha256};
use honba_te_auth::error::AuthError;
use honba_te_auth::login::{
    AngelOneSmartLogin, AuthHttpClient, AuthHttpRequest, HttpReply, LoginFlow, UpstoxOauthLogin,
    ZerodhaHeadlessLogin, extract_code_from_redirect,
};
use honba_te_auth::tokens::SessionTokens;
use honba_te_auth::totp::Totp;
use std::sync::Arc;
use tokio::sync::Mutex;

const TOTP_SEED: &str = "GEZDGNBVGY3TQOJQ";
const NOW: &str = "2026-10-22T02:30:00+00:00";

fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(NOW).unwrap().into()
}

fn creds() -> BrokerCredentials {
    BrokerCredentials {
        broker: "zerodha".into(),
        api_key: "api-key-1".into(),
        api_secret: "api-secret-2".into(),
        user_id: "AB1234".into(),
        password: "hunter2".into(),
        totp_seed: TOTP_SEED.into(),
        tpin: "070164".into(),
        redirect_uri: "https://example.local/cb".into(),
    }
}

// ---------------------------------------------------------------------------
// Scripted mock transport
// ---------------------------------------------------------------------------

struct MockHttp {
    replies: Mutex<Vec<HttpReply>>,
    sent: Mutex<Vec<AuthHttpRequest>>,
}

impl MockHttp {
    fn with_bodies(bodies: Vec<String>) -> Self {
        Self {
            replies: Mutex::new(
                bodies
                    .into_iter()
                    .map(|body| HttpReply { status: 200, body })
                    .collect(),
            ),
            sent: Mutex::new(Vec::new()),
        }
    }

    async fn sent(&self) -> Vec<AuthHttpRequest> {
        self.sent.lock().await.clone()
    }
}

#[async_trait]
impl AuthHttpClient for MockHttp {
    async fn execute(&self, req: AuthHttpRequest) -> Result<HttpReply, AuthError> {
        self.sent.lock().await.push(req);
        let mut replies = self.replies.lock().await;
        if replies.is_empty() {
            return Err(AuthError::Network("script exhausted".into()));
        }
        Ok(replies.remove(0))
    }
}

fn form(req: &AuthHttpRequest, key: &str) -> Option<String> {
    req.form
        .as_ref()?
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

fn body(req: &AuthHttpRequest) -> serde_json::Value {
    serde_json::from_str(req.json.as_ref().expect("json body").to_string().as_str())
        .unwrap_or(serde_json::Value::Null)
}

// ---------------------------------------------------------------------------
// Zerodha
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zerodha_flow_mints_tokens_with_exact_checksum() {
    let http = Arc::new(MockHttp::with_bodies(vec![
        r#"{"status":"success","data":{"request_id":"req-id-7"}}"#.into(),
        r#"{"status":"success","data":{"request_token":"req-tok-9"}}"#.into(),
        r#"{"status":"success","data":{"access_token":"at-42","refresh_token":"rf-42"}}"#.into(),
    ]));
    let flow = ZerodhaHeadlessLogin::new(http.clone() as Arc<dyn AuthHttpClient>);
    let tokens = flow.login(&creds(), now()).await.unwrap();

    assert_eq!(tokens.access_token, "at-42");
    assert_eq!(tokens.refresh_token.as_deref(), Some("rf-42"));
    assert_eq!(tokens.issued_at, now());
    assert!(tokens.expires_at > now());

    let sent = http.sent().await;
    assert_eq!(sent.len(), 3, "three protocol steps");

    // Step 1 target + form.
    assert!(sent[0].url.ends_with("/api/login"));
    assert_eq!(form(&sent[0], "user_id").as_deref(), Some("AB1234"));

    // Step 2 consumes the step-1 request_id and the live TOTP.
    assert!(sent[1].url.ends_with("/api/totp"));
    let codes = Totp::from_base32(TOTP_SEED).unwrap();
    assert_eq!(form(&sent[1], "request_id").as_deref(), Some("req-id-7"));
    assert_eq!(
        form(&sent[1], "totp").as_deref(),
        Some(codes.code_at(now().timestamp() as u64).as_str()),
        "totp must be minted for the supplied `now` timestamp"
    );

    // Step 3: official session exchange with a checksum over the secret.
    assert!(sent[2].url.contains("api.kite.trade/session/token"));
    let checksum = ZerodhaHeadlessLogin::checksum("api-key-1", "req-tok-9", "api-secret-2");
    assert_eq!(form(&sent[2], "checksum").unwrap(), checksum);
    assert_eq!(form(&sent[2], "api_key").as_deref(), Some("api-key-1"));
    assert_eq!(
        form(&sent[2], "request_token").as_deref(),
        Some("req-tok-9")
    );
    assert!(
        sent[2]
            .headers
            .iter()
            .any(|(k, v)| k == "X-Kite-Version" && v == "3"),
        "session exchange must carry the Kite version header"
    );
}

#[tokio::test]
async fn zerodha_step1_failure_is_a_protocol_error() {
    let http = MockHttp::with_bodies(vec!["unreachable".into()]);
    http.replies.lock().await[0].status = 401;
    let flow = ZerodhaHeadlessLogin::new(Arc::new(http));
    let err = flow.login(&creds(), now()).await.unwrap_err();
    match err {
        AuthError::Protocol { broker, detail } => {
            assert_eq!(broker, "zerodha");
            assert!(detail.contains("login initiation"), "detail: {detail}");
        }
        other => panic!("expected protocol error, got {other:?}"),
    }
}

#[tokio::test]
async fn zerodha_requires_user_id() {
    let c = BrokerCredentials {
        user_id: String::new(),
        ..creds()
    };
    let flow = ZerodhaHeadlessLogin::new(Arc::new(MockHttp::with_bodies(vec![])));
    let err = flow.login(&c, now()).await.unwrap_err();
    assert!(matches!(err, AuthError::Config(_)));
}

// ---------------------------------------------------------------------------
// Upstox
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upstox_flow_exchanges_totp_code_for_oauth_tokens() {
    let http = Arc::new(MockHttp::with_bodies(vec![
        r#"{"redirect_uri":"https://api.upstox.com/v2/redirect?code=AUTH-XYZ&state=1"}"#.into(),
        r#"{"data":{"access_token":"upx-at","refresh_token":"upx-rf","expires_in":86400}}"#.into(),
    ]));
    let flow = UpstoxOauthLogin::new(http.clone() as Arc<dyn AuthHttpClient>);
    let tokens = flow.login(&creds(), now()).await.unwrap();

    assert_eq!(tokens.access_token, "upx-at");
    assert_eq!(tokens.refresh_token.as_deref(), Some("upx-rf"));
    assert_eq!(
        (tokens.expires_at - now()).num_seconds(),
        86_400,
        "expires_in seconds must be honored"
    );

    let sent = http.sent().await;
    assert_eq!(sent.len(), 2);
    assert!(sent[0].url.contains("/service/login/authenticate/v3/totp"));
    let totp = Totp::from_base32(TOTP_SEED)
        .unwrap()
        .code_at(now().timestamp() as u64);
    assert_eq!(body(&sent[0])["totp"], serde_json::json!(totp));
    assert_eq!(body(&sent[0])["client_id"], "api-key-1");

    assert!(sent[1].url.contains("/v2/login/authorized_code"));
    assert_eq!(body(&sent[1])["code"], "AUTH-XYZ");
    assert_eq!(body(&sent[1])["client_secret"], "api-secret-2");
    assert_eq!(body(&sent[1])["grant_type"], "authorization_code");
}

#[tokio::test]
async fn upstox_totp_reply_without_authorization_code_is_rejected() {
    let http = MockHttp::with_bodies(vec![r#"{"status":"ok"}"#.to_string()]);
    let flow = UpstoxOauthLogin::new(Arc::new(http));
    let err = flow.login(&creds(), now()).await.unwrap_err();
    match err {
        AuthError::Protocol { broker, detail } => {
            assert_eq!(broker, "upstox");
            assert!(detail.contains("'code'"), "detail: {detail}");
        }
        other => panic!("expected protocol error, got {other:?}"),
    }
}

#[tokio::test]
async fn upstox_exchange_failure_surfaces_broker_message() {
    let http = MockHttp::with_bodies(vec![
        r#"{"redirect_uri":"https://x?code=GOOD"}"#.into(),
        r#"{"error_description":"invalid grant"}"#.into(),
    ]);
    http.replies.lock().await[1].status = 400;
    let flow = UpstoxOauthLogin::new(Arc::new(http));
    let err = flow.login(&creds(), now()).await.unwrap_err();
    match err {
        AuthError::Protocol { broker, detail } => {
            assert_eq!(broker, "upstox");
            assert!(detail.contains("invalid grant"), "detail: {detail}");
        }
        other => panic!("expected protocol error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Angel One (SmartAPI)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn angelone_flow_hashes_password_and_keeps_feed_token() {
    let http = Arc::new(MockHttp::with_bodies(vec![
        r#"{
        "status": true,
        "message": "LOGIN SUCCESS",
        "data": {
            "jwtToken": "jwt-9",
            "refreshToken": "rf-9",
            "feedToken": "feed-9"
        }
    }"#
        .into(),
    ]));
    let flow = AngelOneSmartLogin::new(http.clone() as Arc<dyn AuthHttpClient>);
    let tokens = flow.login(&creds(), now()).await.unwrap();

    assert_eq!(tokens.access_token, "jwt-9");
    assert_eq!(tokens.refresh_token.as_deref(), Some("rf-9"));
    assert_eq!(tokens.feed_token.as_deref(), Some("feed-9"));

    let sent = http.sent().await;
    assert_eq!(sent.len(), 1);
    let payload = body(&sent[0]);
    assert_eq!(payload["clientcode"], "AB1234");
    let expected_password = base64_encode(&sha256(b"hunter2"));
    assert_eq!(payload["password"], serde_json::json!(expected_password));
    let totp = Totp::from_base32(TOTP_SEED)
        .unwrap()
        .code_at(now().timestamp() as u64);
    assert_eq!(payload["totp"], serde_json::json!(totp));

    // SmartAPI header contract.
    assert!(
        sent[0]
            .headers
            .iter()
            .any(|(k, v)| k == "X-PrivateKey" && v == "api-key-1")
    );
    assert!(
        sent[0]
            .url
            .contains("/rest/auth/angelbroking/user/v1/loginByPassword")
    );

    // SmartAPI sessions expire at the next 03:30 IST.
    use honba_te_auth::tokens::next_ist_run;
    assert_eq!(tokens.expires_at, next_ist_run(now(), 3, 30));
}

#[tokio::test]
async fn angelone_status_false_is_a_protocol_error_with_code() {
    let http = MockHttp::with_bodies(vec![
        r#"{"status":false,"message":"Invalid totp","errorCode":"AB1004"}"#.into(),
    ]);
    let flow = AngelOneSmartLogin::new(Arc::new(http));
    let err = flow.login(&creds(), now()).await.unwrap_err();
    match err {
        AuthError::Protocol { broker, detail } => {
            assert_eq!(broker, "angelone");
            assert!(detail.contains("AB1004") && detail.contains("Invalid totp"));
        }
        other => panic!("expected protocol error, got {other:?}"),
    }
}

#[tokio::test]
async fn angelone_requires_password() {
    let c = BrokerCredentials {
        password: String::new(),
        ..creds()
    };
    let flow = AngelOneSmartLogin::new(Arc::new(MockHttp::with_bodies(vec![])));
    let err = flow.login(&c, now()).await.unwrap_err();
    assert!(matches!(err, AuthError::Config(_)));
}

#[test]
fn angelone_password_helper_is_sha256_base64() {
    assert_eq!(
        angelone_password_b64("hunter2").unwrap(),
        base64_encode(&sha256(b"hunter2"))
    );
}

// ---------------------------------------------------------------------------
// Redirect code extraction
// ---------------------------------------------------------------------------

#[test]
fn extract_code_parses_query_parameter() {
    assert_eq!(
        extract_code_from_redirect("https://api.upstox.com/v2/login/redirect?code=ABC123&state=x"),
        Some("ABC123".to_string())
    );
    assert_eq!(
        extract_code_from_redirect("code=RAW456"),
        Some("RAW456".to_string())
    );
    assert_eq!(extract_code_from_redirect("https://x?code="), None);
    assert_eq!(extract_code_from_redirect("https://x?other=1"), None);
}

// ---------------------------------------------------------------------------
// SessionTokens shape (used across flows and the cache)
// ---------------------------------------------------------------------------

#[test]
fn session_token_expiry_helpers() {
    let t = SessionTokens {
        access_token: "x".into(),
        refresh_token: None,
        feed_token: None,
        issued_at: now(),
        expires_at: now() + chrono::Duration::hours(1),
    };
    assert!(!t.is_expired(now()));
    assert!(t.is_expired(now() + chrono::Duration::hours(1)));
    // 60 minutes of remaining TTL expires within a 2h buffer but not 30min.
    assert!(t.expires_within(now(), chrono::Duration::hours(2)));
    assert!(!t.expires_within(now(), chrono::Duration::minutes(30)));
}
