//! RFC 6238 Appendix B conformance tests for the TOTP engine.

use honba_te_auth::totp::{Totp, TotpAlgorithm, TotpConfig, base32_decode, base32_encode};

/// RFC 6238 Appendix B vectors — HMAC-SHA1 over the ASCII secret
/// "12345678901234567890", 8-digit codes, 30-second steps.
const SHA1_SEED_B32: &str = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";

/// RFC 6238 Appendix B vectors — HMAC-SHA256 over the ASCII secret
/// "12345678901234567890123456789012" (32 bytes · 52 Base32 symbols), 8-digit
/// codes, 30-second steps.
const SHA256_SEED_B32: &str = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQGEZA";

const SHA1_CASES: &[(u64, &str)] = &[
    (59, "94287082"),
    (1_111_111_109, "07081804"),
    (1_111_111_111, "14050471"),
    (1_234_567_890, "89005924"),
    (2_000_000_000, "69279037"),
    (20_000_000_000, "65353130"),
];

const SHA256_CASES: &[(u64, &str)] = &[
    (59, "46119246"),
    (1_111_111_109, "68084774"),
    (1_111_111_111, "67062674"),
    (1_234_567_890, "91819424"),
    (2_000_000_000, "90698825"),
    (20_000_000_000, "77737706"),
];

#[test]
fn rfc6238_sha1_appendix_b_vectors() {
    let totp =
        Totp::with_config(SHA1_SEED_B32, TotpConfig::new(TotpAlgorithm::Sha1, 30, 8)).unwrap();
    for (t, expected) in SHA1_CASES {
        assert_eq!(&totp.code_at(*t), expected, "SHA1 T={t}");
    }
}

#[test]
fn rfc6238_sha256_appendix_b_vectors() {
    let totp = Totp::with_config(
        SHA256_SEED_B32,
        TotpConfig::new(TotpAlgorithm::Sha256, 30, 8),
    )
    .unwrap();
    for (t, expected) in SHA256_CASES {
        assert_eq!(&totp.code_at(*t), expected, "SHA256 T={t}");
    }
}

#[test]
fn six_digit_default_is_the_8_digit_suffix() {
    // A 6-digit broker code is the standard 10^6 truncation of the same OTP.
    let totp8 =
        Totp::with_config(SHA1_SEED_B32, TotpConfig::new(TotpAlgorithm::Sha1, 30, 8)).unwrap();
    let totp6 = Totp::from_base32(SHA1_SEED_B32).unwrap();
    assert_eq!(totp6.code_at(59), "287082");
    assert_eq!(&totp8.code_at(59)[2..], "287082");
}

// ---------------------------------------------------------------------------
// Base32
// ---------------------------------------------------------------------------

#[test]
fn base32_decode_rejects_invalid_characters() {
    assert!(base32_decode("AB!1").is_err());
    assert!(base32_decode("0189").is_err()); // digits outside the RFC alphabet
}

#[test]
fn base32_decode_tolerates_padding_and_whitespace_and_case() {
    assert_eq!(
        base32_decode("gEzD gNBV\nGY3T QOJQ ==").unwrap(),
        base32_decode("GEZDGNBVGY3TQOJQ").unwrap()
    );
}

#[test]
fn base32_decode_handles_partial_tail_bits() {
    // 2 symbols → 10 bits → 1 byte with 2 discarded bits: "GE" == 0x31 ('1').
    assert_eq!(base32_decode("GE").unwrap(), vec![0x31]);
    // 4 symbols → 20 bits → 2 bytes: "GEZD" == "12".
    assert_eq!(base32_decode("GEZD").unwrap(), vec![0x31, 0x32]);
    // Canonical RFC 4648 encoding of 2 bytes ("12"): 3 full symbols + a
    // zero-padded tail symbol.
    assert_eq!(base32_encode(&[0x31, 0x32]), "GEZA");
    // Zero-padding round-trips back to the same bytes.
    assert_eq!(base32_decode("GEZA").unwrap(), vec![0x31, 0x32]);
}

#[test]
fn base32_roundtrip_is_lossless() {
    for len in [0usize, 1, 2, 3, 4, 5, 6, 7, 8, 20, 32] {
        let data: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
        let encoded = base32_encode(&data);
        assert_eq!(base32_decode(&encoded).unwrap(), data, "len={len}");
        assert_eq!(base32_decode(&encoded.to_lowercase()).unwrap(), data);
    }
}

// ---------------------------------------------------------------------------
// Drift verification
// ---------------------------------------------------------------------------

#[test]
fn verify_accepts_codes_within_the_drift_window() {
    let totp = Totp::from_base32(SHA1_SEED_B32).unwrap();
    let t = 1_234_567_890u64;

    // Exact step.
    assert!(totp.verify(&totp.code_at(t), t, 1));
    // One step behind / ahead (±30s of clock drift).
    assert!(totp.verify(&totp.code_at(t - 30), t, 1));
    assert!(totp.verify(&totp.code_at(t + 30), t, 1));
    // Two steps outside a ±1 window is refused.
    assert!(!totp.verify(&totp.code_at(t + 60), t, 1));
}

#[test]
fn verify_rejects_malformed_codes() {
    let totp = Totp::from_base32(SHA1_SEED_B32).unwrap();
    let t = 1_234_567_890u64;
    let code = totp.code_at(t);

    // Wrong length / non-digits / empty are rejected before any comparison.
    assert!(!totp.verify("", t, 1));
    assert!(!totp.verify(&format!("{code}1"), t, 1));
    assert!(!totp.verify("abcdef", t, 1));
    let wrong: String = code
        .chars()
        .map(|c| if c == '0' { '1' } else { '0' })
        .collect();
    assert!(!totp.verify(&wrong, t, 1));
}

#[test]
fn configuration_validation_is_strict() {
    assert!(Totp::from_base32("").is_err());
    assert!(Totp::with_config(SHA1_SEED_B32, TotpConfig::new(TotpAlgorithm::Sha1, 30, 0)).is_err());
    assert!(
        Totp::with_config(SHA1_SEED_B32, TotpConfig::new(TotpAlgorithm::Sha1, 30, 11)).is_err()
    );
    assert!(Totp::with_config(SHA1_SEED_B32, TotpConfig::new(TotpAlgorithm::Sha1, 0, 6)).is_err());
}
