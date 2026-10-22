//! Headless broker login flows (M1: zero-touch morning authentication).
//!
//! Each broker implements [`LoginFlow`], driving an HTTP state machine with a
//! TOTP generated from the stored RFC 6238 seed:
//!
//! | Broker | Flow |
//! |--------|------|
//! | [`ZerodhaHeadlessLogin`] | `user_id` login → TOTP 2FA → `request_token` → checksum session exchange |
//! | [`UpstoxOauthLogin`]     | TOTP headless authenticate → authorization code → OAuth2 token exchange |
//! | [`AngelOneSmartLogin`]   | SmartAPI `loginByPassword` (SHA-256 password + TOTP) |
//!
//! Endpoint URLs are overridable fields so WireMock conformance harnesses can
//! point flows at local mock servers.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::sync::Arc;

use crate::credentials::BrokerCredentials;
use crate::crypto;
use crate::error::AuthError;
use crate::tokens::SessionTokens;

/// HTTP request description issued to [`AuthHttpClient`].
#[derive(Debug, Clone)]
pub struct AuthHttpRequest {
    /// "GET" or "POST"
    pub method: &'static str,
    /// Absolute URL
    pub url: String,
    /// Header name/value pairs
    pub headers: Vec<(String, String)>,
    /// HTML form body (`application/x-www-form-urlencoded`)
    pub form: Option<Vec<(String, String)>>,
    /// JSON body (`application/json`)
    pub json: Option<Value>,
}

/// HTTP response wrapper.
#[derive(Debug, Clone)]
pub struct HttpReply {
    /// HTTP status code
    pub status: u16,
    /// Raw response body
    pub body: String,
}

impl HttpReply {
    /// Parse the body as JSON.
    pub fn json(&self) -> Result<Value, AuthError> {
        serde_json::from_str(&self.body).map_err(|e| AuthError::Protocol {
            broker: "?".into(),
            detail: format!(
                "response is not valid JSON ({e}); body={}",
                self.truncated()
            ),
        })
    }

    /// Truncated body for error messages.
    pub fn truncated(&self) -> String {
        let mut s = self.body.clone();
        if s.len() > 400 {
            s.truncate(400);
            s.push('…');
        }
        s
    }
}

/// Abstract HTTP transport so flows are testable against scripted replays.
#[async_trait]
pub trait AuthHttpClient: Send + Sync {
    /// Execute one request and translate transport failures to [`AuthError::Network`].
    async fn execute(&self, req: AuthHttpRequest) -> Result<HttpReply, AuthError>;
}

/// `reqwest`-based production transport.
#[derive(Debug, Clone)]
pub struct ReqwestAuthHttpClient {
    client: reqwest::Client,
}

impl ReqwestAuthHttpClient {
    /// Client with sane defaults (no internal redirects for OAuth intercepts).
    pub fn new() -> reqwest::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent("honba-te-auth/0.1")
                .build()?,
        })
    }
}

impl Default for ReqwestAuthHttpClient {
    fn default() -> Self {
        Self::new().expect("default TLS-backed reqwest client")
    }
}

#[async_trait]
impl AuthHttpClient for ReqwestAuthHttpClient {
    async fn execute(&self, req: AuthHttpRequest) -> Result<HttpReply, AuthError> {
        let mut builder = match req.method {
            "GET" => self.client.get(&req.url),
            "POST" => self.client.post(&req.url),
            other => {
                return Err(AuthError::Config(format!("unsupported method {other}")));
            }
        };
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        if let Some(form) = &req.form {
            builder = builder.form(form);
        }
        if let Some(json) = &req.json {
            builder = builder.json(json);
        }

        let res = builder
            .send()
            .await
            .map_err(|e| AuthError::Network(e.to_string()))?;
        let status = res.status().as_u16();
        let body = res
            .text()
            .await
            .map_err(|e| AuthError::Network(e.to_string()))?;
        Ok(HttpReply { status, body })
    }
}

/// Broker-specific session bootstrap.
#[async_trait]
pub trait LoginFlow: Send + Sync {
    /// Broker identifier.
    fn broker(&self) -> &str;

    /// Perform the full headless login and return session tokens.
    async fn login(
        &self,
        creds: &BrokerCredentials,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError>;
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Broker envelope `{"status": ..., "data": {...}}` → data part.
fn data_part(broker: &str, reply: &HttpReply) -> Result<Value, AuthError> {
    let json = reply.json()?;
    let data = json.get("data").cloned().unwrap_or(json);
    if data.is_null() {
        return Err(AuthError::Protocol {
            broker: broker.into(),
            detail: format!("no data in login response: {}", reply.truncated()),
        });
    }
    Ok(data)
}

fn require_str(broker: &str, value: &Value, keys: &[&str]) -> Result<String, AuthError> {
    for key in keys {
        if let Some(s) = value.get(*key).and_then(|v| v.as_str())
            && !s.is_empty()
        {
            return Ok(s.to_string());
        }
    }
    Err(AuthError::Protocol {
        broker: broker.into(),
        detail: format!(
            "missing non-empty field {} in login response: {value}",
            keys.first().unwrap_or(&"?")
        ),
    })
}

fn optional_str(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(s) = value.get(*key).and_then(|v| v.as_str())
            && !s.is_empty()
        {
            return Some(s.to_string());
        }
    }
    None
}

fn expires_secs(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(n) = v.as_u64() {
                return Some(n);
            }
            if let Some(s) = v.as_str()
                && let Ok(n) = s.parse::<u64>()
            {
                return Some(n);
            }
        }
    }
    None
}

/// Extract an OAuth `code` parameter from a redirect URL fragment.
pub fn extract_code_from_redirect(redirect: &str) -> Option<String> {
    let after_query = redirect.split_once('?').map(|(_, b)| b).unwrap_or(redirect);
    for pair in after_query.split('&') {
        if let Some(rest) = pair.strip_prefix("code=")
            && !rest.is_empty()
        {
            return Some(rest.to_string());
        }
    }
    None
}

fn missing_cred(broker: &str, cred_field: &str) -> AuthError {
    AuthError::Config(format!(
        "broker '{broker}' headless login requires '{cred_field}' in stored credentials"
    ))
}

/// Check HTTP status and short-circuit non-2xx replies into protocol errors.
fn ensure_success(broker: &str, reply: &HttpReply, stage: &str) -> Result<(), AuthError> {
    if reply.status >= 400 {
        return Err(AuthError::Protocol {
            broker: broker.into(),
            detail: format!(
                "{stage} failed [status {}]: {}",
                reply.status,
                reply.truncated()
            ),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Zerodha (Kite Connect v3)
// ---------------------------------------------------------------------------

/// Zerodha headless login endpoints (community-documented TOTP flow).
#[derive(Debug, Clone)]
pub struct ZerodhaLoginConfig {
    /// Kite web login API base (login / TOTP steps).
    pub login_api_base: String,
    /// Official Kite Connect session exchange base.
    pub session_api_base: String,
}

impl Default for ZerodhaLoginConfig {
    fn default() -> Self {
        Self {
            login_api_base: "https://kite.zerodha.com/api".to_string(),
            session_api_base: "https://api.kite.trade".to_string(),
        }
    }
}

/// Zerodha (Kite) headless TOTP login flow.
///
/// Steps:
/// 1. `POST {login_api_base}/login` with `user_id` → `data.request_id`
/// 2. `POST {login_api_base}/totp` with `user_id`/`request_id`/`totp` → `data.request_token`
/// 3. `POST {session_api_base}/session/token` with `api_key`, `request_token`,
///    `checksum = sha256_hex(api_key + request_token + api_secret)` → `data.access_token`
pub struct ZerodhaHeadlessLogin {
    http: Arc<dyn AuthHttpClient>,
    cfg: ZerodhaLoginConfig,
}

impl ZerodhaHeadlessLogin {
    /// Flow with the official endpoints.
    pub fn new(http: Arc<dyn AuthHttpClient>) -> Self {
        Self {
            http,
            cfg: ZerodhaLoginConfig::default(),
        }
    }

    /// Flow against custom endpoints (WireMock conformance).
    pub fn with_config(http: Arc<dyn AuthHttpClient>, cfg: ZerodhaLoginConfig) -> Self {
        Self { http, cfg }
    }

    /// The session-exchange checksum: sha256hex(api_key + request_token + api_secret).
    pub fn checksum(api_key: &str, request_token: &str, api_secret: &str) -> String {
        crypto::zerodha_checksum(api_key, request_token, api_secret)
    }
}

#[async_trait]
impl LoginFlow for ZerodhaHeadlessLogin {
    fn broker(&self) -> &str {
        "zerodha"
    }

    async fn login(
        &self,
        creds: &BrokerCredentials,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError> {
        let broker = "zerodha";
        if creds.api_key.is_empty() {
            return Err(missing_cred(broker, "api_key"));
        }
        if creds.api_secret.is_empty() {
            return Err(missing_cred(broker, "api_secret"));
        }
        if creds.user_id.is_empty() {
            return Err(missing_cred(broker, "user_id"));
        }
        let totp_code = creds.totp_code_at(now.timestamp() as u64)?;

        // Step 1: initiate the login session for the user id.
        let reply = self
            .http
            .execute(AuthHttpRequest {
                method: "POST",
                url: format!("{}/login", self.cfg.login_api_base),
                headers: vec![(
                    "Referer".into(),
                    format!(
                        "https://kite.zerodha.com/connect/login?user_id={}",
                        creds.user_id
                    ),
                )],
                form: Some(vec![("user_id".into(), creds.user_id.clone())]),
                json: None,
            })
            .await?;
        ensure_success(broker, &reply, "login initiation")?;
        let request_id = require_str(broker, &data_part(broker, &reply)?, &["request_id"])?;

        // Step 2: TOTP two-factor completion → request_token.
        let reply = self
            .http
            .execute(AuthHttpRequest {
                method: "POST",
                url: format!("{}/totp", self.cfg.login_api_base),
                headers: vec![(
                    "Referer".into(),
                    format!(
                        "https://kite.zerodha.com/connect/login?user_id={}",
                        creds.user_id
                    ),
                )],
                form: Some(vec![
                    ("user_id".into(), creds.user_id.clone()),
                    ("request_id".into(), request_id),
                    ("totp".into(), totp_code),
                    ("skip_session".into(), "true".into()),
                ]),
                json: None,
            })
            .await?;
        ensure_success(broker, &reply, "totp 2fa")?;
        let request_token = require_str(broker, &data_part(broker, &reply)?, &["request_token"])?;

        // Step 3: official Connect session exchange (checksum over the secret).
        let checksum = Self::checksum(&creds.api_key, &request_token, &creds.api_secret);
        let reply = self
            .http
            .execute(AuthHttpRequest {
                method: "POST",
                url: format!("{}/session/token", self.cfg.session_api_base),
                headers: vec![("X-Kite-Version".into(), "3".into())],
                form: Some(vec![
                    ("api_key".into(), creds.api_key.clone()),
                    ("request_token".into(), request_token),
                    ("checksum".into(), checksum),
                ]),
                json: None,
            })
            .await?;
        ensure_success(broker, &reply, "session exchange")?;
        let data = data_part(broker, &reply)?;
        let access_token = require_str(broker, &data, &["access_token"])?;
        let refresh_token = optional_str(&data, &["refresh_token"]);

        Ok(SessionTokens {
            access_token,
            refresh_token,
            feed_token: None,
            issued_at: now,
            expires_at: now + Duration::hours(23),
        })
    }
}

// ---------------------------------------------------------------------------
// Upstox (v2 OAuth2)
// ---------------------------------------------------------------------------

/// Upstox v2 headless OAuth endpoints (TOTP-authenticated code fetch).
#[derive(Debug, Clone)]
pub struct UpstoxLoginConfig {
    /// Upstox API base.
    pub api_base: String,
    /// Redirect URI registered on the developer application.
    pub redirect_uri: String,
}

impl Default for UpstoxLoginConfig {
    fn default() -> Self {
        Self {
            api_base: "https://api.upstox.com".to_string(),
            redirect_uri: "https://redirect_html.upstox.com/doc".to_string(),
        }
    }
}

/// Upstox v2 headless TOTP OAuth2 login flow.
///
/// Steps:
/// 1. `POST {api_base}/service/login/authenticate/v3/totp` with `client_id` +
///    `totp` → broker returns a redirect target carrying `code=`
/// 2. `POST {api_base}/v2/login/authorized_code` with `code`, `client_secret`,
///    `grant_type=authorization_code` → `access_token` + `refresh_token`
pub struct UpstoxOauthLogin {
    http: Arc<dyn AuthHttpClient>,
    cfg: UpstoxLoginConfig,
}

impl UpstoxOauthLogin {
    /// Flow with the official endpoints.
    pub fn new(http: Arc<dyn AuthHttpClient>) -> Self {
        Self {
            http,
            cfg: UpstoxLoginConfig::default(),
        }
    }

    /// Flow against custom endpoints (WireMock conformance).
    pub fn with_config(http: Arc<dyn AuthHttpClient>, cfg: UpstoxLoginConfig) -> Self {
        Self { http, cfg }
    }
}

#[async_trait]
impl LoginFlow for UpstoxOauthLogin {
    fn broker(&self) -> &str {
        "upstox"
    }

    async fn login(
        &self,
        creds: &BrokerCredentials,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError> {
        let broker = "upstox";
        if creds.api_key.is_empty() {
            return Err(missing_cred(broker, "api_key"));
        }
        if creds.api_secret.is_empty() {
            return Err(missing_cred(broker, "api_secret"));
        }
        let totp_code = creds.totp_code_at(now.timestamp() as u64)?;

        // Step 1: TOTP step yields an authorization-code redirect target.
        let reply = self
            .http
            .execute(AuthHttpRequest {
                method: "POST",
                url: format!("{}/service/login/authenticate/v3/totp", self.cfg.api_base),
                headers: vec![("Content-Type".into(), "application/json".into())],
                form: None,
                json: Some(serde_json::json!({
                    "client_id": creds.api_key,
                    "totp": totp_code,
                })),
            })
            .await?;
        ensure_success(broker, &reply, "totp authenticate")?;

        // Redirect target may be carried as a JSON field or embedded raw.
        let candidate = reply
            .json()
            .ok()
            .and_then(|v| optional_str(&v, &["redirect_uri", "redirectUrl", "url"]))
            .unwrap_or_else(|| reply.body.clone());
        let code = extract_code_from_redirect(&candidate)
            .or_else(|| extract_code_from_redirect(&reply.body))
            .ok_or_else(|| AuthError::Protocol {
                broker: broker.into(),
                detail: format!(
                    "no authorization 'code' in TOTP reply: {}",
                    reply.truncated()
                ),
            })?;

        // Step 2: standard OAuth2 authorized_code grant.
        let reply = self
            .http
            .execute(AuthHttpRequest {
                method: "POST",
                url: format!("{}/v2/login/authorized_code", self.cfg.api_base),
                headers: vec![("Content-Type".into(), "application/json".into())],
                form: None,
                json: Some(serde_json::json!({
                    "code": code,
                    "client_id": creds.api_key,
                    "client_secret": creds.api_secret,
                    "redirect_uri": creds.redirect_uri.clone(),
                    "grant_type": "authorization_code",
                })),
            })
            .await?;
        ensure_success(broker, &reply, "authorized_code exchange")?;
        let data = data_part(broker, &reply)?;
        let access_token = require_str(broker, &data, &["access_token"])?;
        let refresh_token = optional_str(&data, &["refresh_token"]);
        let ttl_secs = expires_secs(&data, &["expires_in"]).unwrap_or(86_400);

        Ok(SessionTokens {
            access_token,
            refresh_token,
            feed_token: None,
            issued_at: now,
            expires_at: now + Duration::seconds(ttl_secs as i64),
        })
    }
}

// ---------------------------------------------------------------------------
// Angel One (SmartAPI 2.0)
// ---------------------------------------------------------------------------

/// Angel One SmartAPI endpoints.
#[derive(Debug, Clone)]
pub struct AngelOneLoginConfig {
    /// SmartAPI base.
    pub api_base: String,
}

impl Default for AngelOneLoginConfig {
    fn default() -> Self {
        Self {
            api_base: "https://apiconnect.angelone.in".to_string(),
        }
    }
}

/// Angel One SmartAPI loginByPassword flow (SHA-256 password + TOTP).
///
/// Steps:
/// 1. `POST {api_base}/rest/auth/angelbroking/user/v1/loginByPassword` with
///    `clientcode`, `password = base64(sha256(password))`, and `totp` →
///    `data.jwtToken` + `data.refreshToken` + `data.feedToken`
pub struct AngelOneSmartLogin {
    http: Arc<dyn AuthHttpClient>,
    cfg: AngelOneLoginConfig,
}

impl AngelOneSmartLogin {
    /// Flow with the official endpoints.
    pub fn new(http: Arc<dyn AuthHttpClient>) -> Self {
        Self {
            http,
            cfg: AngelOneLoginConfig::default(),
        }
    }

    /// Flow against custom endpoints (WireMock conformance).
    pub fn with_config(http: Arc<dyn AuthHttpClient>, cfg: AngelOneLoginConfig) -> Self {
        Self { http, cfg }
    }

    fn headers(creds: &BrokerCredentials) -> Result<Vec<(String, String)>, AuthError> {
        Ok(vec![
            ("Content-Type".into(), "application/json".into()),
            ("Accept".into(), "application/json".into()),
            ("X-UserType".into(), "USER".into()),
            ("X-SourceID".into(), "WEB".into()),
            ("X-ClientLocalIP".into(), "127.0.0.1".into()),
            ("X-ClientPublicIP".into(), "127.0.0.1".into()),
            ("X-MACAddress".into(), "00:00:00:00:00:00".into()),
            ("X-PrivateKey".into(), creds.api_key.clone()),
        ])
    }
}

#[async_trait]
impl LoginFlow for AngelOneSmartLogin {
    fn broker(&self) -> &str {
        "angelone"
    }

    async fn login(
        &self,
        creds: &BrokerCredentials,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, AuthError> {
        let broker = "angelone";
        if creds.api_key.is_empty() {
            return Err(missing_cred(broker, "api_key"));
        }
        if creds.user_id.is_empty() {
            return Err(missing_cred(broker, "user_id"));
        }
        if creds.password.is_empty() {
            return Err(missing_cred(broker, "password"));
        }
        let totp_code = creds.totp_code_at(now.timestamp() as u64)?;
        let password_b64 = crypto::angelone_password_b64(&creds.password)?;

        let headers = Self::headers(creds)?;
        let reply = self
            .http
            .execute(AuthHttpRequest {
                method: "POST",
                url: format!(
                    "{}/rest/auth/angelbroking/user/v1/loginByPassword",
                    self.cfg.api_base
                ),
                headers,
                form: None,
                json: Some(serde_json::json!({
                    "clientcode": creds.user_id,
                    "password": password_b64,
                    "totp": totp_code,
                })),
            })
            .await?;

        let json = reply.json().map_err(|e| {
            let detail = format!("[status {}] {} ({e:?})", reply.status, reply.truncated());
            AuthError::Protocol {
                broker: broker.into(),
                detail,
            }
        })?;

        let status_ok = json
            .get("status")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !status_ok {
            let code = json
                .get("errorCode")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let truncated = reply.truncated();
            let detail = json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or(truncated.as_str())
                .to_string();
            return Err(AuthError::Protocol {
                broker: broker.into(),
                detail: format!("{code}: {detail}"),
            });
        }

        let data = json.get("data").cloned().unwrap_or(Value::Null);
        let access_token = require_str(broker, &data, &["jwtToken"])?;
        let refresh_token = optional_str(&data, &["refreshToken"]);
        let feed_token = optional_str(&data, &["feedToken"]);

        // SmartAPI sessions expire at the market close buffer (03:30 IST).
        let expiry = crate::tokens::next_ist_run(now, 3, 30);

        Ok(SessionTokens {
            access_token,
            refresh_token,
            feed_token,
            issued_at: now,
            expires_at: expiry,
        })
    }
}
