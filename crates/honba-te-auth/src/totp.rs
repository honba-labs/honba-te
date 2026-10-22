//! Zero-dependency RFC 6238 TOTP (time-based one-time password).
//!
//! Supports HMAC-SHA1 and HMAC-SHA256 over Base32-encoded secrets, the
//! standard 30-second step, and configurable digit counts. Broker authenticator
//! apps (Zerodha Kite, Upstox, Angel One) default to 6-digit SHA-1 TOTP.

use crate::crypto::{hmac_sha1, hmac_sha256};
use crate::error::AuthError;

/// Hash used within the HMAC of the TOTP chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TotpAlgorithm {
    /// HMAC-SHA1 (the industry default)
    Sha1,
    /// HMAC-SHA256 (RFC 6238 option)
    Sha256,
}

/// RFC 6238 TOTP configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TotpConfig {
    /// Hash algorithm
    pub algorithm: TotpAlgorithm,
    /// Time step seconds (standard: 30)
    pub step_secs: u64,
    /// OTP digit count (standard: 6)
    pub digits: u32,
}

impl TotpConfig {
    /// Standard broker configuration: SHA-1, 30s step, 6 digits.
    pub const STANDARD: Self = Self {
        algorithm: TotpAlgorithm::Sha1,
        step_secs: 30,
        digits: 6,
    };

    /// Custom configuration.
    pub fn new(algorithm: TotpAlgorithm, step_secs: u64, digits: u32) -> Self {
        Self {
            algorithm,
            step_secs,
            digits,
        }
    }
}

impl Default for TotpConfig {
    fn default() -> Self {
        Self::STANDARD
    }
}

/// A TOTP generator for one Base32-encoded secret.
#[derive(Debug, Clone)]
pub struct Totp {
    key: Vec<u8>,
    config: TotpConfig,
}

impl Totp {
    /// Construct from a Base32 secret (RFC 4648, case-insensitive, optional
    /// padding, spaces tolerated).
    pub fn from_base32(seed: &str) -> Result<Self, AuthError> {
        let key = base32_decode(seed)
            .map_err(|e| AuthError::Totp(format!("invalid Base32 TOTP seed: {e}")))?;
        if key.is_empty() {
            return Err(AuthError::Totp("empty TOTP seed".into()));
        }
        Ok(Self {
            key,
            config: TotpConfig::default(),
        })
    }

    /// Construct with explicit configuration.
    pub fn with_config(seed: &str, config: TotpConfig) -> Result<Self, AuthError> {
        let mut totp = Self::from_base32(seed)?;
        if config.digits == 0 || config.digits > 10 {
            return Err(AuthError::Totp("digits must be within 1..=10".into()));
        }
        if config.step_secs == 0 {
            return Err(AuthError::Totp("step must be positive".into()));
        }
        totp.config = config;
        Ok(totp)
    }

    /// RFC 6238 OTP at Unix-time `unix_seconds`.
    pub fn code_at(&self, unix_seconds: u64) -> String {
        let counter = unix_seconds / self.config.step_secs;
        let mac = match self.config.algorithm {
            TotpAlgorithm::Sha1 => hmac_sha1(&self.key, &counter.to_be_bytes()).to_vec(),
            TotpAlgorithm::Sha256 => hmac_sha256(&self.key, &counter.to_be_bytes()).to_vec(),
        };
        let last = mac.len() - 1;
        let offset = (mac[last] & 0x0f) as usize;
        let code = ((mac[offset] & 0x7f) as u32) << 24
            | (mac[offset + 1] as u32) << 16
            | (mac[offset + 2] as u32) << 8
            | mac[offset + 3] as u32;

        let modulus = 10u32.pow(self.config.digits);
        format!(
            "{:0width$}",
            code % modulus,
            width = self.config.digits as usize
        )
    }

    /// Verify `code` for a submitted `unix_seconds` allowing ±`window` steps
    /// of clock drift (±1 is what authenticators/brokers allow).
    pub fn verify(&self, code: &str, unix_seconds: u64, window: u32) -> bool {
        if code.len() != self.config.digits as usize || !code.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        let step = self.config.step_secs;
        for drift in 0..=window as u64 {
            for at in [
                unix_seconds.checked_add(step * drift),
                unix_seconds.checked_sub(step * drift),
            ]
            .into_iter()
            .flatten()
            {
                if self.code_at(at) == code {
                    return true;
                }
            }
        }
        false
    }
}

/// RFC 4648 Base32 alphabet.
const B32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Decode a Base32 string to raw bytes. Accepts lowercase input, ignores
/// whitespace and optional `=` padding; rejects invalid characters.
pub fn base32_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::with_capacity(input.len());
    for c in input.chars() {
        if c == '=' || c.is_whitespace() {
            continue;
        }
        let up = c.to_ascii_uppercase();
        let idx = B32.iter().position(|b| *b as char == up);
        match idx {
            Some(i) => buf.push(i as u8),
            None => return Err(format!("illegal Base32 character '{c}'")),
        }
    }

    let mut out = Vec::with_capacity(buf.len() * 5 / 8);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for symbol in buf {
        acc = (acc << 5) | symbol as u32;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    // bits < 8 remainder is discardable padding (RFC 4648 allows ignoring it).
    Ok(out)
}

/// Encode bytes as Base32 (padding-free, uppercase).
pub fn base32_encode(data: &[u8]) -> String {
    let mut out = String::new();
    let mut acc = 0u32;
    let mut bits = 0u32;
    for byte in data.iter().copied() {
        acc = (acc << 8) | byte as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(B32[((acc >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(B32[((acc << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}
