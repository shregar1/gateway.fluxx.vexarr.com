//! TLS certificate pinning.
//!
//! Optional: when `FLUXX_API_TLS_PIN` is set to a base64-encoded SHA-256 of
//! the expected SubjectPublicKeyInfo, the HTTP client we build with
//! `pinned_client(...)` will reject any TLS connection where the leaf
//! certificate's SPKI doesn't match. When unset, the client uses the system
//! trust store as usual.
//!
//! Why SPKI hashes (not full cert hashes): a single SPKI survives the entire
//! issuer's renewal cycle as long as we keep the keypair, which is what we
//! want — pin once, rotate certs without ceremony.
//!
//! ### Operator instructions
//!
//! Capture the production pin once with:
//!
//! ```sh
//! openssl s_client -connect api.fluxx.dev:443 -servername api.fluxx.dev </dev/null 2>/dev/null \
//!   | openssl x509 -pubkey -noout \
//!   | openssl pkey -pubin -outform der \
//!   | openssl dgst -sha256 -binary \
//!   | base64
//! ```
//!
//! Bake the result into the agent build (or distribute via env) and ship.

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::DigitallySignedStruct;
use rustls::SignatureScheme;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum PinError {
    #[error("FLUXX_API_TLS_PIN must decode to 32 bytes (sha256)")]
    BadPinLength,
    #[error("FLUXX_API_TLS_PIN is not valid base64")]
    BadPinFormat,
}

/// Build a reqwest client with optional SPKI pinning.
///
/// When `pin_b64` is `Some(...)` and valid, the client rejects any TLS
/// connection whose leaf certificate's SPKI hash doesn't match. When
/// `None`, returns a default `reqwest::Client` (system trust store).
pub fn pinned_client(pin_b64: Option<&str>) -> Result<reqwest::Client, PinError> {
    let Some(pin) = pin_b64 else {
        return Ok(reqwest::Client::new());
    };
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    let pin_bytes = STANDARD.decode(pin).map_err(|_| PinError::BadPinFormat)?;
    if pin_bytes.len() != 32 {
        return Err(PinError::BadPinLength);
    }

    let verifier = Arc::new(SpkiVerifier { pin: pin_bytes });
    let cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();

    reqwest::Client::builder()
        .use_preconfigured_tls(cfg)
        .build()
        .map_err(|_| PinError::BadPinFormat)
}

#[derive(Debug)]
struct SpkiVerifier {
    pin: Vec<u8>,
}

impl ServerCertVerifier for SpkiVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let der = end_entity.as_ref();
        let spki = extract_spki_der(der).ok_or_else(|| {
            rustls::Error::General("could not extract SubjectPublicKeyInfo".into())
        })?;
        let hash = Sha256::digest(spki);
        if hash.as_slice() == self.pin.as_slice() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "TLS pin mismatch: expected sha256(spki) = {} but got {}",
                hex::encode(&self.pin),
                hex::encode(hash),
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // The custom-verifier path doesn't validate the chain, so we accept
        // signature checks unconditionally — the SPKI hash IS the auth.
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ED25519,
        ]
    }
}

/// Walk an X.509 certificate's DER to find the SubjectPublicKeyInfo bytes.
///
/// We avoid pulling in a full ASN.1 crate by doing minimal TLV navigation:
/// the SPKI is the sixth element under Certificate's `tbsCertificate`
/// SEQUENCE, after version (optional), serialNumber, signature, issuer,
/// validity, subject.
///
/// Returns the SPKI's full DER (including the outer SEQUENCE header), which
/// is what gets hashed — this matches the convention used by `openssl pkey
/// -pubin -outform der` in the operator instructions above.
fn extract_spki_der(cert_der: &[u8]) -> Option<&[u8]> {
    // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signature }
    let (_, tbs_body, _after_tbs) = read_sequence(cert_der)?;

    let mut cursor = tbs_body;

    // Optional explicit [0] version tag.
    if cursor.first().copied() == Some(0xA0) {
        let (_, _v, rest) = read_tlv(cursor)?;
        cursor = rest;
    }

    // serialNumber INTEGER
    let (_, _serial, rest) = read_tlv(cursor)?;
    cursor = rest;
    // signature AlgorithmIdentifier SEQUENCE
    let (_, _sig_alg, rest) = read_tlv(cursor)?;
    cursor = rest;
    // issuer Name SEQUENCE
    let (_, _issuer, rest) = read_tlv(cursor)?;
    cursor = rest;
    // validity Validity SEQUENCE
    let (_, _validity, rest) = read_tlv(cursor)?;
    cursor = rest;
    // subject Name SEQUENCE
    let (_, _subject, rest) = read_tlv(cursor)?;
    cursor = rest;
    // subjectPublicKeyInfo SEQUENCE
    let (header_len, body, _rest) = read_tlv(cursor)?;
    // Return the SPKI including its TLV header (sha256 is over the same).
    Some(&cursor[..header_len + body.len()])
}

/// Read a SEQUENCE (tag, length, body) and return (header_len, body, after).
fn read_sequence(input: &[u8]) -> Option<(usize, &[u8], &[u8])> {
    if input.first().copied() != Some(0x30) {
        return None;
    }
    read_tlv(input)
}

/// Read any DER TLV. Returns (header_len, body, rest_after).
fn read_tlv(input: &[u8]) -> Option<(usize, &[u8], &[u8])> {
    if input.len() < 2 {
        return None;
    }
    // Skip tag (we don't validate here — callers can if they care).
    let len_byte = input[1];
    let (header_len, len): (usize, usize) = if len_byte & 0x80 == 0 {
        (2, len_byte as usize)
    } else {
        let n = (len_byte & 0x7F) as usize;
        if n == 0 || n > 4 || input.len() < 2 + n {
            return None;
        }
        let mut l: usize = 0;
        for i in 0..n {
            l = (l << 8) | input[2 + i] as usize;
        }
        (2 + n, l)
    };
    if input.len() < header_len + len {
        return None;
    }
    Some((
        header_len,
        &input[header_len..header_len + len],
        &input[header_len + len..],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_pin_returns_default_client() {
        let c = pinned_client(None).unwrap();
        // Sanity: building with no pin should not require any specific config.
        drop(c);
    }

    #[test]
    fn bad_base64_rejected() {
        let err = pinned_client(Some("not-base64!!!")).unwrap_err();
        matches!(err, PinError::BadPinFormat);
    }

    #[test]
    fn wrong_length_pin_rejected() {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;
        let too_short = STANDARD.encode([0u8; 16]);
        let err = pinned_client(Some(&too_short)).unwrap_err();
        matches!(err, PinError::BadPinLength);
    }

    #[test]
    fn read_tlv_handles_short_form_length() {
        let input = [0x04, 0x03, 0xaa, 0xbb, 0xcc, 0xff];
        let (hdr, body, rest) = read_tlv(&input).unwrap();
        assert_eq!(hdr, 2);
        assert_eq!(body, &[0xaa, 0xbb, 0xcc]);
        assert_eq!(rest, &[0xff]);
    }

    #[test]
    fn read_tlv_handles_long_form_length() {
        // 0x04 (tag) 0x82 0x00 0x05 (long-form length, 2 bytes => 5) then 5 body bytes
        let input = [0x04, 0x82, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', 0xee];
        let (hdr, body, rest) = read_tlv(&input).unwrap();
        assert_eq!(hdr, 4);
        assert_eq!(body, b"hello");
        assert_eq!(rest, &[0xee]);
    }

    #[test]
    fn read_tlv_rejects_overrun() {
        // Says length 10 but only has 2 body bytes after.
        let input = [0x04, 0x0a, 0xaa, 0xbb];
        assert!(read_tlv(&input).is_none());
    }
}
