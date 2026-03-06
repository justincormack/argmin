/// AWS chunked transfer encoding decoder with optional per-chunk signature verification.
///
/// Wire format:
/// ```text
/// {size-hex}[;chunk-signature={hex}]\r\n
/// {chunk-data}\r\n
/// ...
/// 0[;chunk-signature={hex}]\r\n
/// [trailer-key:trailer-value\r\n]*
/// \r\n
/// ```
use auth::StreamingSigningContext;
use ring::hmac;

use crate::error::ServerError;

/// Result of decoding an aws-chunked body.
#[derive(Debug)]
pub struct DecodedBody {
    /// The decoded payload data (chunks concatenated).
    pub data: Vec<u8>,
    /// Trailing headers (e.g. x-amz-checksum-crc32).
    pub trailers: Vec<(String, String)>,
}

/// Decode an aws-chunked body, optionally verifying per-chunk signatures.
///
/// `streaming` must be `Some` for STREAMING-AWS4-HMAC-SHA256-* modes
/// and `None` for STREAMING-UNSIGNED-PAYLOAD-* modes.
/// Minimum chunk size for non-final chunks (matches AWS S3 behavior).
const MIN_CHUNK_SIZE: usize = 8192;

pub fn decode_chunked_body(
    wire: &[u8],
    streaming: Option<&StreamingSigningContext>,
    trailer_mode: bool,
) -> Result<DecodedBody, ServerError> {
    let mut pos = 0;
    let mut data = Vec::new();
    let mut prev_sig = streaming.map(|s| s.seed_signature.clone());
    let mut chunk_number: usize = 0;
    let mut prev_chunk_size: Option<usize> = None;

    loop {
        // Read the chunk header line (up to \r\n).
        let line_end = find_crlf(wire, pos).ok_or_else(|| ServerError::MalformedChunkedBody {
            reason: "missing CRLF after chunk size".to_string(),
        })?;
        let line = &wire[pos..line_end];
        pos = line_end + 2; // skip \r\n

        // Parse: {hex-size}[;chunk-signature={hex}]
        let (chunk_size, chunk_sig) = parse_chunk_header(line)?;

        // In signed mode, every chunk MUST include a chunk-signature.
        if streaming.is_some() && chunk_sig.is_none() {
            return Err(ServerError::MalformedChunkedBody {
                reason: "missing chunk-signature in signed chunked upload".to_string(),
            });
        }

        if chunk_size == 0 {
            // Terminal chunk. Verify its signature if signed.
            if let Some(ctx) = &streaming {
                let terminal_sig = chunk_sig.as_deref().unwrap();
                verify_chunk_signature(ctx, prev_sig.as_deref().unwrap(), b"", terminal_sig)?;
                prev_sig = Some(terminal_sig.to_string());
            }

            // Parse trailing headers until empty line.
            let trailers = parse_trailers(wire, &mut pos)?;

            // Verify trailer signature if in signed trailer mode.
            if trailer_mode {
                if let Some(ctx) = &streaming {
                    verify_trailer_signature(ctx, prev_sig.as_deref().unwrap(), &trailers)?;
                }
            }

            // Strip the trailer signature from returned trailers — it's a signing
            // mechanism, not a content trailer.
            let trailers: Vec<(String, String)> = trailers
                .into_iter()
                .filter(|(k, _)| k != "x-amz-trailer-signature")
                .collect();

            return Ok(DecodedBody { data, trailers });
        }

        // If this is a new data chunk after a previous one, the previous chunk
        // must have been >= MIN_CHUNK_SIZE. Only the last data chunk before the
        // terminal 0-chunk may be smaller.
        if let Some(prev_size) = prev_chunk_size {
            if prev_size < MIN_CHUNK_SIZE {
                return Err(ServerError::InvalidChunkSize {
                    chunk: chunk_number,
                    chunk_size: prev_size,
                    min_size: MIN_CHUNK_SIZE,
                });
            }
        }

        // Read chunk_size bytes of data.
        if pos + chunk_size > wire.len() {
            return Err(ServerError::MalformedChunkedBody {
                reason: "chunk data truncated".to_string(),
            });
        }
        let chunk_data = &wire[pos..pos + chunk_size];
        pos += chunk_size;

        // Expect \r\n after chunk data.
        if pos + 2 > wire.len() || wire[pos] != b'\r' || wire[pos + 1] != b'\n' {
            return Err(ServerError::MalformedChunkedBody {
                reason: "missing CRLF after chunk data".to_string(),
            });
        }
        pos += 2;

        // Verify chunk signature if signed.
        if let Some(ctx) = &streaming {
            let sig = chunk_sig.as_deref().unwrap();
            verify_chunk_signature(ctx, prev_sig.as_deref().unwrap(), chunk_data, sig)?;
            prev_sig = Some(sig.to_string());
        }

        data.extend_from_slice(chunk_data);
        chunk_number += 1;
        prev_chunk_size = Some(chunk_size);
    }
}

/// Find \r\n starting from `start` in `data`. Returns index of \r.
fn find_crlf(data: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < data.len() {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse a chunk header line into (size, optional signature hex).
fn parse_chunk_header(line: &[u8]) -> Result<(usize, Option<String>), ServerError> {
    let line_str = std::str::from_utf8(line).map_err(|_| ServerError::MalformedChunkedBody {
        reason: "non-UTF8 chunk header".to_string(),
    })?;

    let (size_part, rest) = match line_str.split_once(';') {
        Some((s, r)) => (s, Some(r)),
        None => (line_str, None),
    };

    let size = usize::from_str_radix(size_part.trim(), 16).map_err(|_| {
        ServerError::MalformedChunkedBody {
            reason: format!("invalid chunk size: {}", size_part),
        }
    })?;

    let sig = rest.and_then(|ext| {
        ext.strip_prefix("chunk-signature=")
            .map(|hex| hex.to_string())
    });

    Ok((size, sig))
}

/// Parse trailing headers after the terminal chunk.
fn parse_trailers(wire: &[u8], pos: &mut usize) -> Result<Vec<(String, String)>, ServerError> {
    let mut trailers = Vec::new();
    loop {
        if *pos >= wire.len() {
            // End of input without the required trailing CRLF terminator.
            return Err(ServerError::IncompleteBody);
        }

        // Empty line (\r\n) marks end of trailers.
        if *pos + 1 < wire.len() && wire[*pos] == b'\r' && wire[*pos + 1] == b'\n' {
            *pos += 2;
            break;
        }

        let line_end = find_crlf(wire, *pos).ok_or_else(|| ServerError::MalformedChunkedBody {
            reason: "unterminated trailer line".to_string(),
        })?;
        let line = &wire[*pos..line_end];
        *pos = line_end + 2;

        let line_str =
            std::str::from_utf8(line).map_err(|_| ServerError::MalformedChunkedBody {
                reason: "non-UTF8 trailer".to_string(),
            })?;

        if let Some((key, value)) = line_str.split_once(':') {
            trailers.push((key.trim().to_ascii_lowercase(), value.trim().to_string()));
        } else {
            return Err(ServerError::IncompleteBody);
        }
    }
    Ok(trailers)
}

/// Verify a single chunk's signature.
///
/// String-to-sign for chunk:
/// ```text
/// AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{prev_sig}\n{sha256("")}\n{sha256(chunk_data)}
/// ```
fn verify_chunk_signature(
    ctx: &StreamingSigningContext,
    prev_sig: &str,
    chunk_data: &[u8],
    claimed_sig: &str,
) -> Result<(), ServerError> {
    let empty_hash = auth::canonical::sha256_hex(b"");
    let chunk_hash = auth::canonical::sha256_hex(chunk_data);

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        ctx.timestamp, ctx.scope, prev_sig, empty_hash, chunk_hash
    );

    let key = hmac::Key::new(hmac::HMAC_SHA256, &ctx.signing_key);
    let expected = hmac::sign(&key, string_to_sign.as_bytes());
    let expected_hex = hex_encode(expected.as_ref());

    if expected_hex != claimed_sig {
        return Err(ServerError::Auth(auth::AuthError::SignatureMismatch));
    }
    Ok(())
}

/// Verify the trailer signature.
///
/// String-to-sign for trailer:
/// ```text
/// AWS4-HMAC-SHA256-TRAILER\n{timestamp}\n{scope}\n{prev_sig}\n{sha256(canonical_trailers)}
/// ```
/// where canonical_trailers is the sorted non-signature trailer headers, each as
/// `key:value\n`.
fn verify_trailer_signature(
    ctx: &StreamingSigningContext,
    prev_sig: &str,
    trailers: &[(String, String)],
) -> Result<(), ServerError> {
    // Separate the trailer signature from the other trailers.
    let claimed_sig = trailers
        .iter()
        .find(|(k, _)| k == "x-amz-trailer-signature")
        .map(|(_, v)| v.as_str());

    // Collect non-signature trailers.
    let content_trailers: Vec<&(String, String)> = trailers
        .iter()
        .filter(|(k, _)| k != "x-amz-trailer-signature")
        .collect();

    // If there are no content trailers and no trailer signature, nothing to verify.
    if content_trailers.is_empty() && claimed_sig.is_none() {
        return Ok(());
    }

    // If there are content trailers but no signature, reject.
    let claimed_sig =
        claimed_sig.ok_or_else(|| ServerError::Auth(auth::AuthError::SignatureMismatch))?;

    // Build canonical trailer string: sorted by key, each line as "key:value\n".
    let mut sorted_trailers = content_trailers;
    sorted_trailers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical: String = sorted_trailers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v))
        .collect();

    let trailer_hash = auth::canonical::sha256_hex(canonical.as_bytes());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        ctx.timestamp, ctx.scope, prev_sig, trailer_hash
    );

    let key = hmac::Key::new(hmac::HMAC_SHA256, &ctx.signing_key);
    let expected = hmac::sign(&key, string_to_sign.as_bytes());
    let expected_hex = hex_encode(expected.as_ref());

    if expected_hex != claimed_sig {
        return Err(ServerError::Auth(auth::AuthError::SignatureMismatch));
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_unsigned_single_chunk() {
        let wire = b"5\r\nhello\r\n0\r\n\r\n";
        let result = decode_chunked_body(wire, None, false).unwrap();
        assert_eq!(result.data, b"hello");
        assert!(result.trailers.is_empty());
    }

    #[test]
    fn decode_unsigned_multiple_chunks() {
        // Non-final chunks must be >= 8192 bytes.
        let chunk1 = vec![b'A'; 8192];
        let chunk2 = b"tail";
        let mut wire = Vec::new();
        wire.extend_from_slice(format!("{:x}\r\n", chunk1.len()).as_bytes());
        wire.extend_from_slice(&chunk1);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("{:x}\r\n", chunk2.len()).as_bytes());
        wire.extend_from_slice(chunk2);
        wire.extend_from_slice(b"\r\n0\r\n\r\n");
        let result = decode_chunked_body(&wire, None, false).unwrap();
        let mut expected = chunk1;
        expected.extend_from_slice(chunk2);
        assert_eq!(result.data, expected);
    }

    #[test]
    fn decode_small_non_final_chunk_rejected() {
        // Two small chunks — the first (5 bytes) should be rejected since a second follows.
        let wire = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let err = decode_chunked_body(wire, None, false).unwrap_err();
        assert!(matches!(err, ServerError::InvalidChunkSize { .. }));
    }

    #[test]
    fn decode_unsigned_with_trailers() {
        let wire = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:abcd1234\r\n\r\n";
        let result = decode_chunked_body(wire, None, false).unwrap();
        assert_eq!(result.data, b"hello");
        assert_eq!(result.trailers.len(), 1);
        assert_eq!(result.trailers[0].0, "x-amz-checksum-crc32");
        assert_eq!(result.trailers[0].1, "abcd1234");
    }

    #[test]
    fn decode_unsigned_empty_body() {
        let wire = b"0\r\n\r\n";
        let result = decode_chunked_body(wire, None, false).unwrap();
        assert!(result.data.is_empty());
        assert!(result.trailers.is_empty());
    }

    #[test]
    fn decode_malformed_no_crlf() {
        let wire = b"5\r\nhello";
        assert!(decode_chunked_body(wire, None, false).is_err());
    }

    #[test]
    fn decode_malformed_bad_hex() {
        let wire = b"zz\r\n\r\n";
        assert!(decode_chunked_body(wire, None, false).is_err());
    }

    #[test]
    fn decode_signed_chunk_verification() {
        // Build a properly signed chunked body and verify it decodes.
        let chunk_data = b"Hello";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = "20130524";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20130524T000000Z";
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        // Derive signing key.
        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey(secret.to_string()),
            date,
            region,
            service,
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(signing_key.as_ref());

        // Compute seed signature (normally from the Authorization header).
        let seed_sig = "seed0000000000000000000000000000000000000000000000000000000000ab";

        let ctx = StreamingSigningContext {
            signing_key: key_bytes,
            seed_signature: seed_sig.to_string(),
            scope: scope.clone(),
            timestamp: timestamp.to_string(),
        };

        // Compute chunk signature.
        let empty_hash = auth::canonical::sha256_hex(b"");
        let chunk_hash = auth::canonical::sha256_hex(chunk_data);
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            timestamp, scope, seed_sig, empty_hash, chunk_hash
        );
        let key = hmac::Key::new(hmac::HMAC_SHA256, &key_bytes);
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        // Compute terminal chunk signature (size=0, data=empty).
        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            timestamp, scope, chunk_sig, empty_hash, terminal_hash
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        let wire = format!(
            "5;chunk-signature={}\r\nHello\r\n0;chunk-signature={}\r\n\r\n",
            chunk_sig, terminal_sig
        );

        let result = decode_chunked_body(wire.as_bytes(), Some(&ctx), false).unwrap();
        assert_eq!(result.data, b"Hello");
    }

    #[test]
    fn decode_signed_bad_signature_rejected() {
        let ctx = StreamingSigningContext {
            signing_key: [0u8; 32],
            seed_signature: "0".repeat(64),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            timestamp: "20130524T000000Z".to_string(),
        };

        let wire = format!(
            "5;chunk-signature={}\r\nHello\r\n0;chunk-signature={}\r\n\r\n",
            "bad".repeat(21) + "b",
            "bad".repeat(21) + "b",
        );

        let err = decode_chunked_body(wire.as_bytes(), Some(&ctx), false).unwrap_err();
        assert!(matches!(
            err,
            ServerError::Auth(auth::AuthError::SignatureMismatch)
        ));
    }

    #[test]
    fn decode_signed_missing_chunk_signature_rejected() {
        // In signed mode, chunks without ;chunk-signature=... must be rejected.
        let ctx = StreamingSigningContext {
            signing_key: [0u8; 32],
            seed_signature: "0".repeat(64),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            timestamp: "20130524T000000Z".to_string(),
        };

        // No chunk-signature on data chunk.
        let wire = b"5\r\nHello\r\n0\r\n\r\n";
        let err = decode_chunked_body(wire, Some(&ctx), false).unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn decode_signed_with_valid_trailer_signature() {
        let chunk_data = b"Hello";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = "20130524";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20130524T000000Z";
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey(secret.to_string()),
            date,
            region,
            service,
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(signing_key.as_ref());

        let seed_sig = "seed0000000000000000000000000000000000000000000000000000000000ab";

        let ctx = StreamingSigningContext {
            signing_key: key_bytes,
            seed_signature: seed_sig.to_string(),
            scope: scope.clone(),
            timestamp: timestamp.to_string(),
        };

        let empty_hash = auth::canonical::sha256_hex(b"");
        let chunk_hash = auth::canonical::sha256_hex(chunk_data);
        let key = hmac::Key::new(hmac::HMAC_SHA256, &key_bytes);

        // Chunk signature.
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            timestamp, scope, seed_sig, empty_hash, chunk_hash
        );
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        // Terminal chunk signature.
        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            timestamp, scope, chunk_sig, empty_hash, terminal_hash
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        // Trailer signature: AWS4-HMAC-SHA256-TRAILER\n{ts}\n{scope}\n{terminal_sig}\n{sha256(canonical_trailers)}
        let canonical_trailers = "x-amz-checksum-crc32:abcd1234\n";
        let trailer_hash = auth::canonical::sha256_hex(canonical_trailers.as_bytes());
        let sts_trailer = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
            timestamp, scope, terminal_sig, trailer_hash
        );
        let trailer_sig = hex_encode(hmac::sign(&key, sts_trailer.as_bytes()).as_ref());

        let wire = format!(
            "5;chunk-signature={}\r\nHello\r\n0;chunk-signature={}\r\nx-amz-checksum-crc32:abcd1234\r\nx-amz-trailer-signature:{}\r\n\r\n",
            chunk_sig, terminal_sig, trailer_sig
        );

        let result = decode_chunked_body(wire.as_bytes(), Some(&ctx), true).unwrap();
        assert_eq!(result.data, b"Hello");
        assert_eq!(result.trailers.len(), 1);
        assert_eq!(result.trailers[0].0, "x-amz-checksum-crc32");
        assert_eq!(result.trailers[0].1, "abcd1234");
    }

    #[test]
    fn decode_signed_with_bad_trailer_signature_rejected() {
        let chunk_data = b"Hello";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = "20130524";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20130524T000000Z";
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey(secret.to_string()),
            date,
            region,
            service,
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(signing_key.as_ref());

        let seed_sig = "seed0000000000000000000000000000000000000000000000000000000000ab";

        let ctx = StreamingSigningContext {
            signing_key: key_bytes,
            seed_signature: seed_sig.to_string(),
            scope: scope.clone(),
            timestamp: timestamp.to_string(),
        };

        let empty_hash = auth::canonical::sha256_hex(b"");
        let chunk_hash = auth::canonical::sha256_hex(chunk_data);
        let key = hmac::Key::new(hmac::HMAC_SHA256, &key_bytes);

        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            timestamp, scope, seed_sig, empty_hash, chunk_hash
        );
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            timestamp, scope, chunk_sig, empty_hash, terminal_hash
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        // Bad trailer signature.
        let bad_trailer_sig = "0".repeat(64);

        let wire = format!(
            "5;chunk-signature={}\r\nHello\r\n0;chunk-signature={}\r\nx-amz-checksum-crc32:abcd1234\r\nx-amz-trailer-signature:{}\r\n\r\n",
            chunk_sig, terminal_sig, bad_trailer_sig
        );

        let err = decode_chunked_body(wire.as_bytes(), Some(&ctx), true).unwrap_err();
        assert!(matches!(
            err,
            ServerError::Auth(auth::AuthError::SignatureMismatch)
        ));
    }

    #[test]
    fn decode_malformed_trailer_line_rejected() {
        // A trailer line without ':' should be rejected.
        let wire = b"5\r\nhello\r\n0\r\nno-colon-here\r\n\r\n";
        let err = decode_chunked_body(wire, None, false).unwrap_err();
        assert!(matches!(err, ServerError::IncompleteBody));
    }

    #[test]
    fn decode_missing_terminal_crlf_rejected() {
        // Terminal chunk without the final \r\n terminator should be rejected.
        let wire = b"5\r\nhello\r\n0\r\n";
        let err = decode_chunked_body(wire, None, false).unwrap_err();
        assert!(matches!(err, ServerError::IncompleteBody));
    }
}
