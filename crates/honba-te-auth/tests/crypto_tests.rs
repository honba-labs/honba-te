//! Golden-vector tests for the hand-rolled crypto primitives (M1).
//!
//! Vectors from FIPS 180-4, RFC 2104/2202/4231, and RFC 4648.

use honba_te_auth::crypto::{
    base64_encode, hex_encode, hex_encode_upper, hmac_sha1, hmac_sha256, sha1_vec, sha256_hex,
    sha256_vec, zerodha_checksum,
};

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4 §B.1 / §B.2)
// ---------------------------------------------------------------------------

#[test]
fn sha256_empty_string() {
    assert_eq!(
        hex_encode(&sha256_vec(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn sha256_abc() {
    assert_eq!(
        hex_encode(&sha256_vec(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn sha256_sixty_four_byte_message_pads_into_second_block() {
    // The canonical 56-byte FIPS message forces the padding boundary
    // (80 + 8 length bytes overflow into a second SHA block).
    let msg = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    assert_eq!(msg.len(), 56);
    assert_eq!(
        hex_encode(&sha256_vec(msg)),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
    );
}

#[test]
fn sha256_message_digest_larger_than_one_block() {
    let msg = b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";
    assert_eq!(
        hex_encode(&sha256_vec(msg)),
        "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1"
    );
}

// ---------------------------------------------------------------------------
// SHA-1
// ---------------------------------------------------------------------------

#[test]
fn sha1_empty_string() {
    assert_eq!(
        hex_encode(&sha1_vec(b"")),
        "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
}

#[test]
fn sha1_abc() {
    assert_eq!(
        hex_encode(&sha1_vec(b"abc")),
        "a9993e364706816aba3e25717850c26c9cd0d89d"
    );
}

#[test]
fn sha1_multi_block_message() {
    let msg = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    assert_eq!(
        hex_encode(&sha1_vec(msg)),
        "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
    );
}

// ---------------------------------------------------------------------------
// HMAC-SHA256 (RFC 4231)
// ---------------------------------------------------------------------------

#[test]
fn hmac_sha256_rfc4231_test_case_1() {
    let key = [0x0bu8; 20];
    let mac = hmac_sha256(&key, b"Hi There");
    assert_eq!(
        hex_encode(&mac),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[test]
fn hmac_sha256_rfc4231_test_case_2() {
    let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
    assert_eq!(
        hex_encode(&mac),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn hmac_sha256_larger_than_block_key_is_hashed_first() {
    // RFC 4231 test case 6: 131-byte key ( exceeds the 64-byte HMAC block) must
    // be digested before use.
    let key = [0xaau8; 131];
    let mac = hmac_sha256(
        &key,
        b"Test Using Larger Than Block-Size Key - Hash Key First",
    );
    assert_eq!(
        hex_encode(&mac),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

// ---------------------------------------------------------------------------
// HMAC-SHA1 (RFC 2202)
// ---------------------------------------------------------------------------

#[test]
fn hmac_sha1_rfc2202_test_case_1() {
    let key = [0x0bu8; 20];
    let mac = hmac_sha1(&key, b"Hi There");
    assert_eq!(hex_encode(&mac), "b617318655057264e28bc0b6fb378c8ef146be00");
}

#[test]
fn hmac_sha1_rfc2202_test_case_2() {
    let mac = hmac_sha1(b"Jefe", b"what do ya want for nothing?");
    assert_eq!(hex_encode(&mac), "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79");
}

#[test]
fn hmac_sha1_larger_than_block_key_is_hashed_first() {
    // RFC 2202 test case 6: 80-byte key.
    let key = [0xaau8; 80];
    let mac = hmac_sha1(
        &key,
        b"Test Using Larger Than Block-Size Key - Hash Key First",
    );
    assert_eq!(hex_encode(&mac), "aa4ae5e15272d00e95705637ce8a3b55ed402112");
}

// ---------------------------------------------------------------------------
// Hex / Base64 (RFC 4648 §10 vectors)
// ---------------------------------------------------------------------------

#[test]
fn hex_encoding_cases() {
    assert_eq!(hex_encode(&[]), "");
    assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    assert_eq!(hex_encode_upper(&[0xde, 0xad, 0xbe, 0xef]), "DEADBEEF");
}

#[test]
fn base64_encoding_cases() {
    assert_eq!(base64_encode(b""), "");
    assert_eq!(base64_encode(b"f"), "Zg==");
    assert_eq!(base64_encode(b"fo"), "Zm8=");
    assert_eq!(base64_encode(b"foo"), "Zm9v");
    assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
    assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
    assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
}

// ---------------------------------------------------------------------------
// Login-flow digest helpers
// ---------------------------------------------------------------------------

#[test]
fn zerodha_checksum_concatenation_order_is_exact() {
    // checksum = sha256hex(api_key + request_token + api_secret); the pieces
    // are concatenated with no separator.
    let expected = sha256_hex(b"AB12CD34request-token-9my-secret");
    assert_eq!(
        zerodha_checksum("AB12CD34", "request-token-9", "my-secret"),
        expected
    );
}

#[test]
fn sha256_fixed_size_api_matches_vec_api() {
    let msg = b"pivot parity";
    assert_eq!(
        hex_encode(&honba_te_auth::crypto::sha256(msg)),
        hex_encode(&sha256_vec(msg))
    );
}
