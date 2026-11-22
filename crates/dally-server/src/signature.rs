//! Signing push requests, and checking those signatures on the receiving end.
//!
//! When a push queue has a secret, every request carries a header
//!
//! ```text
//! Dally-Signature: t=1763550000,v1=5f0e...
//! ```
//!
//! where `t` is the time of signing in Unix seconds and `v1` is the hex
//! HMAC-SHA256 of `"<t>.<body>"` keyed with the secret. Including the
//! timestamp in the signed text lets receivers reject old requests that were
//! captured and replayed.
//!
//! ```
//! use std::time::Duration;
//!
//! use dally_server::signature;
//!
//! let body = br#"{"to":"ana@example.com"}"#;
//! let header = signature::sign(b"secret", 1_763_550_000, body);
//! assert!(signature::verify(b"secret", &header, body, 1_763_550_010, Duration::from_secs(300)).is_ok());
//! ```

use std::time::Duration;

use hmac::{KeyInit, Mac};

type HmacSha256 = hmac::Hmac<sha2::Sha256>;

/// The name of the header that carries the signature.
pub const HEADER: &str = "Dally-Signature";

/// Signs `body` as sent at `timestamp` (Unix seconds) and returns the value of
/// the [`HEADER`] header.
pub fn sign(secret: &[u8], timestamp: i64, body: &[u8]) -> String {
    let digest = mac(secret, timestamp, body).finalize().into_bytes();
    format!("t={timestamp},v1={}", hex::encode(digest))
}

/// Why a signature was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SignatureError {
    /// The header is not of the form `t=<seconds>,v1=<hex>`.
    #[error("malformed signature header")]
    Malformed,

    /// The timestamp is further from now than the allowed tolerance.
    #[error("signature timestamp is outside the allowed tolerance")]
    Expired,

    /// No signature in the header matches the body and secret.
    #[error("signature does not match")]
    Mismatch,
}

/// Checks a [`HEADER`] value against the raw request body.
///
/// `now` is the receiver's current time in Unix seconds. Requests signed more
/// than `tolerance` before or after it are rejected, which limits how long a
/// captured request can be replayed; five minutes is a good default. The
/// comparison runs in constant time.
///
/// The header may hold several `v1` entries; any one matching is enough. dally
/// sends one, but this leaves room to sign with two secrets while rotating.
pub fn verify(
    secret: &[u8],
    header: &str,
    body: &[u8],
    now: i64,
    tolerance: Duration,
) -> Result<(), SignatureError> {
    let parsed = Header::parse(header)?;

    let tolerance = i64::try_from(tolerance.as_secs()).unwrap_or(i64::MAX);
    if now.abs_diff(parsed.timestamp) > tolerance.unsigned_abs() {
        return Err(SignatureError::Expired);
    }

    let matches = parsed.signatures.iter().any(|signature| {
        hex::decode(signature).is_ok_and(|expected| {
            mac(secret, parsed.timestamp, body)
                .verify_slice(&expected)
                .is_ok()
        })
    });
    if matches {
        Ok(())
    } else {
        Err(SignatureError::Mismatch)
    }
}

/// A parsed `t=...,v1=...` header. Unknown keys are ignored so that a future
/// `v2` scheme can be added next to `v1` without breaking receivers.
struct Header<'a> {
    timestamp: i64,
    signatures: Vec<&'a str>,
}

impl<'a> Header<'a> {
    fn parse(header: &'a str) -> Result<Self, SignatureError> {
        let mut timestamp = None;
        let mut signatures = Vec::new();
        for item in header.split(',') {
            let (key, value) = item
                .trim()
                .split_once('=')
                .ok_or(SignatureError::Malformed)?;
            match key {
                "t" => {
                    let parsed = value.parse().map_err(|_| SignatureError::Malformed)?;
                    timestamp = Some(parsed);
                }
                "v1" => signatures.push(value),
                _ => {}
            }
        }
        match timestamp {
            Some(timestamp) if !signatures.is_empty() => Ok(Self {
                timestamp,
                signatures,
            }),
            _ => Err(SignatureError::Malformed),
        }
    }
}

fn mac(secret: &[u8], timestamp: i64, body: &[u8]) -> HmacSha256 {
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        unreachable!("HMAC accepts keys of any length");
    };
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOLERANCE: Duration = Duration::from_secs(300);

    #[test]
    fn known_vector() {
        // Computed independently with:
        //   printf '%s' '1700000000.{"hello":"world"}' \
        //     | openssl dgst -sha256 -hmac 'whsec_test'
        let header = sign(b"whsec_test", 1_700_000_000, br#"{"hello":"world"}"#);
        assert_eq!(
            header,
            "t=1700000000,v1=f592bbf3951cfc94e560eecfb5d9dd4da6b0fff2e626235f8ab4b54860925d0b"
        );
    }

    #[test]
    fn round_trip() {
        let header = sign(b"secret", 1_000, b"{}");
        assert_eq!(verify(b"secret", &header, b"{}", 1_000, TOLERANCE), Ok(()));
    }

    #[test]
    fn rejects_tampering() {
        let header = sign(b"secret", 1_000, b"{}");
        assert_eq!(
            verify(b"secret", &header, b"{ }", 1_000, TOLERANCE),
            Err(SignatureError::Mismatch)
        );
        assert_eq!(
            verify(b"other", &header, b"{}", 1_000, TOLERANCE),
            Err(SignatureError::Mismatch)
        );
        let moved = header.replace("t=1000", "t=1001");
        assert_eq!(
            verify(b"secret", &moved, b"{}", 1_000, TOLERANCE),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn rejects_old_and_future_timestamps() {
        let header = sign(b"secret", 1_000, b"{}");
        assert_eq!(verify(b"secret", &header, b"{}", 1_300, TOLERANCE), Ok(()));
        assert_eq!(
            verify(b"secret", &header, b"{}", 1_301, TOLERANCE),
            Err(SignatureError::Expired)
        );
        assert_eq!(
            verify(b"secret", &header, b"{}", 699, TOLERANCE),
            Err(SignatureError::Expired)
        );
    }

    #[test]
    fn accepts_any_matching_v1() {
        let header = sign(b"new", 1_000, b"{}");
        let rotated = header.replace("v1=", "v1=00ff,v1=");
        assert_eq!(verify(b"new", &rotated, b"{}", 1_000, TOLERANCE), Ok(()));
    }

    #[test]
    fn rejects_malformed_headers() {
        for header in ["", "t=1000", "v1=abcd", "t=soon,v1=abcd", "garbage"] {
            assert_eq!(
                verify(b"secret", header, b"{}", 1_000, TOLERANCE),
                Err(SignatureError::Malformed),
                "{header:?}"
            );
        }
    }
}
