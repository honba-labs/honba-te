//! Hand-rolled cryptographic primitives for the headless login flows.
//!
//! The TOTP engine is deliberately zero-dependency (plan M1) and brokers
//! additionally require:
//! - SHA-256 hex (Zerodha login checksum)
//! - SHA-256 base64 (Angel One SmartAPI password)
//! - Base32 decoding (TOTP secrets)
//!
//! These are constant-time-independent reference implementations (this is 2FA
//! digest math, not bulk encryption) with RFC golden vectors tested in
//! `tests/crypto_tests.rs`.

use crate::error::AuthError;

// ---------------------------------------------------------------------------
// Hex / Base64 encoding
// ---------------------------------------------------------------------------

/// Lowercase hex encoding (the common wire form for digests).
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Uppercase hex encoding.
pub fn hex_encode_upper(bytes: &[u8]) -> String {
    hex_encode(bytes).to_uppercase()
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard Base64 encoding (with `=` padding).
pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let word = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64[(word >> 18) as usize & 0x3f] as char);
        out.push(B64[(word >> 12) as usize & 0x3f] as char);
        if chunk.len() > 1 {
            out.push(B64[(word >> 6) as usize & 0x3f] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64[word as usize & 0x3f] as char);
        } else {
            out.push('=');
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Bit-twiddling helpers
// ---------------------------------------------------------------------------

#[inline]
fn ch(x: u32, y: u32, z: u32) -> u32 {
    (x & y) | (!x & z)
}

#[inline]
fn maj(x: u32, y: u32, z: u32) -> u32 {
    (x & y) | (x & z) | (y & z)
}

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4)
// ---------------------------------------------------------------------------

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// SHA-256 digest of `data` (32 bytes).
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let mut padded = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for block in padded.chunks(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let b = &block[i * 4..i * 4 + 4];
            *word = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch(e, f, g))
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let t2 = s0.wrapping_add(maj(a, b, c));
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// SHA-256 rendered as lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    hex_encode(&sha256(data))
}

// ---------------------------------------------------------------------------
// SHA-1 (FIPS 180-4), required for RFC 6238 TOTP HMAC-SHA1
// ---------------------------------------------------------------------------

/// SHA-1 digest of `data` (20 bytes).
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476, 0xc3d2e1f0];

    let mut padded = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for block in padded.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let b = &block[i * 4..i * 4 + 4];
            *word = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);

        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5a827999u32),
                20..=39 => (b ^ c ^ d, 0x6ed9eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1bbcdc),
                _ => (b ^ c ^ d, 0xca62c1d6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// HMAC (RFC 2104)
// ---------------------------------------------------------------------------

const HMAC_BLOCK: usize = 64;

/// HMAC-SHA256 of `message` under `key`.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    hmac(key, message, sha256_vec, 32)
        .try_into()
        .expect("sha256 digest is 32 bytes")
}

/// HMAC-SHA1 of `message` under `key`.
pub fn hmac_sha1(key: &[u8], message: &[u8]) -> [u8; 20] {
    hmac(key, message, sha1_vec, 20)
        .try_into()
        .expect("sha1 digest is 20 bytes")
}

fn hmac(key: &[u8], message: &[u8], digest: fn(&[u8]) -> Vec<u8>, digest_len: usize) -> Vec<u8> {
    // Key normalization: hash if larger than block, pad to block otherwise.
    let mut k = if key.len() > HMAC_BLOCK {
        digest(key)
    } else {
        key.to_vec()
    };
    k.resize(HMAC_BLOCK, 0);

    let mut ipad = [0x36u8; HMAC_BLOCK];
    let mut opad = [0x5cu8; HMAC_BLOCK];
    for i in 0..HMAC_BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }

    let mut inner = ipad.to_vec();
    inner.extend_from_slice(message);
    let inner_digest = digest(&inner);

    let mut outer = opad.to_vec();
    outer.extend_from_slice(&inner_digest);
    digest(&outer)[..digest_len].to_vec()
}

// Convenience: vec-backed variants for the generic HMAC core. These delegate
// to the fixed-size functions so call sites can use either representation.
pub fn hmac_sha256_vec(key: &[u8], message: &[u8]) -> Vec<u8> {
    hmac_sha256(key, message).to_vec()
}

pub fn hmac_sha1_vec(key: &[u8], message: &[u8]) -> Vec<u8> {
    hmac_sha1(key, message).to_vec()
}

/// SHA-1 digest as bytes (byte-slice API parity).
pub fn sha1_vec(data: &[u8]) -> Vec<u8> {
    sha1(data).to_vec()
}

pub fn sha256_vec(data: &[u8]) -> Vec<u8> {
    sha256(data).to_vec()
}

// ---------------------------------------------------------------------------
// SHA-256 / SHA-1 convenience helpers used by login flows
// ---------------------------------------------------------------------------

/// Angel One SmartAPI password digest: SHA-256 of the password (Base64).
pub fn angelone_password_b64(password: &str) -> Result<String, AuthError> {
    Ok(base64_encode(&sha256(password.as_bytes())))
}

/// Zerodha login checksum: SHA-256 hex of api_key + request_token + api_secret.
pub fn zerodha_checksum(api_key: &str, request_token: &str, api_secret: &str) -> String {
    let mut joined = String::with_capacity(api_key.len() + request_token.len() + api_secret.len());
    joined.push_str(api_key);
    joined.push_str(request_token);
    joined.push_str(api_secret);
    sha256_hex(joined.as_bytes())
}
