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
use ring::{digest, hmac};

use crate::error::ServerError;

/// Result of decoding an aws-chunked body.
#[derive(Debug)]
pub struct DecodedBody {
    /// The decoded payload data (chunks concatenated).
    pub data: Vec<u8>,
    /// Trailing headers (e.g. x-amz-checksum-crc32).
    pub trailers: Vec<(String, String)>,
}

/// Minimum chunk size for non-final chunks (matches AWS S3 behavior).
const MIN_CHUNK_SIZE: usize = 8192;
const MAX_CHUNKED_LINE_BYTES: usize = 16 * 1024;
const MAX_CHUNKED_TRAILER_BYTES: usize = 64 * 1024;
const MAX_CHUNKED_TRAILER_COUNT: usize = 128;

/// Decode an aws-chunked body, optionally verifying per-chunk signatures.
///
/// `streaming` must be `Some` for STREAMING-AWS4-HMAC-SHA256-* modes
/// and `None` for STREAMING-UNSIGNED-PAYLOAD-* modes.
///
/// Used only in unit tests — production uses `IncrementalChunkedDecoder`
/// via the streaming path in serve.rs.
#[cfg(test)]
pub(super) fn decode_chunked_body(
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

fn chunked_line_too_long() -> ServerError {
    ServerError::MalformedChunkedBody {
        reason: format!("chunked line exceeds {MAX_CHUNKED_LINE_BYTES} bytes"),
    }
}

fn chunked_trailers_too_large() -> ServerError {
    ServerError::MalformedChunkedBody {
        reason: "chunked trailer section exceeds limit".to_string(),
    }
}

/// Read one CRLF-terminated line from `pending` plus `wire`.
///
/// Returns `None` when more wire bytes are needed. `pending` is only for
/// header/trailer line fragments; chunk payload bytes are not stored there.
fn read_crlf_line(pending: &mut Vec<u8>, wire: &mut &[u8]) -> Result<Option<Vec<u8>>, ServerError> {
    if pending.is_empty() {
        if let Some(crlf_pos) = find_crlf(wire, 0) {
            if crlf_pos > MAX_CHUNKED_LINE_BYTES {
                return Err(chunked_line_too_long());
            }
            let line = wire[..crlf_pos].to_vec();
            *wire = &wire[crlf_pos + 2..];
            return Ok(Some(line));
        }
        if wire.len() > MAX_CHUNKED_LINE_BYTES {
            return Err(chunked_line_too_long());
        }
        pending.extend_from_slice(wire);
        *wire = &[];
        return Ok(None);
    }

    if pending.last() == Some(&b'\r') && wire.first() == Some(&b'\n') {
        let line = pending[..pending.len() - 1].to_vec();
        if line.len() > MAX_CHUNKED_LINE_BYTES {
            return Err(chunked_line_too_long());
        }
        pending.clear();
        *wire = &wire[1..];
        return Ok(Some(line));
    }

    if let Some(crlf_pos) = find_crlf(wire, 0) {
        if pending.len() + crlf_pos > MAX_CHUNKED_LINE_BYTES {
            return Err(chunked_line_too_long());
        }
        pending.extend_from_slice(&wire[..crlf_pos + 2]);
        *wire = &wire[crlf_pos + 2..];
    } else {
        if pending.len() + wire.len() > MAX_CHUNKED_LINE_BYTES {
            return Err(chunked_line_too_long());
        }
        pending.extend_from_slice(wire);
        *wire = &[];
    }

    if let Some(crlf_pos) = find_crlf(pending, 0) {
        let line = pending[..crlf_pos].to_vec();
        pending.drain(..crlf_pos + 2);
        Ok(Some(line))
    } else {
        Ok(None)
    }
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
            reason: format!("invalid chunk size: {size_part}"),
        }
    })?;

    let sig = rest.and_then(|ext| {
        ext.strip_prefix("chunk-signature=")
            .map(std::string::ToString::to_string)
    });

    Ok((size, sig))
}

/// Parse trailing headers after the terminal chunk.
///
/// Used only by `decode_chunked_body` (test-only batch decoder).
/// The incremental decoder handles trailers inline in its state machine.
#[cfg(test)]
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
    let chunk_hash = auth::canonical::sha256_hex(chunk_data);
    verify_chunk_signature_hash(ctx, prev_sig, &chunk_hash, claimed_sig)
}

fn verify_chunk_signature_hash(
    ctx: &StreamingSigningContext,
    prev_sig: &str,
    chunk_hash: &str,
    claimed_sig: &str,
) -> Result<(), ServerError> {
    let empty_hash = auth::canonical::sha256_hex(b"");
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
/// where `canonical_trailers` is the sorted non-signature trailer headers, each as
/// `key:value\n`.
#[allow(clippy::format_collect)]
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
    // AWS returns IncompleteBody for a missing trailer signature.
    let claimed_sig = claimed_sig.ok_or(ServerError::IncompleteBody)?;

    // Build canonical trailer string: sorted by key, each line as "key:value\n".
    let mut sorted_trailers = content_trailers;
    sorted_trailers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical: String = sorted_trailers
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
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

/// Incremental aws-chunked decoder for streaming write paths.
///
/// Accepts wire bytes incrementally via `feed()` and yields decoded payload
/// bytes. Handles per-chunk signature verification and trailer parsing.
pub struct IncrementalChunkedDecoder {
    streaming: Option<StreamingSigningContext>,
    trailer_mode: bool,
    prev_sig: Option<String>,
    chunk_number: usize,
    prev_chunk_size: Option<usize>,
    expected_decoded_remaining: Option<u64>,
    current_chunk_hash: Option<digest::Context>,
    buf: Vec<u8>,
    state: ChunkedDecoderState,
    trailers: Vec<(String, String)>,
    trailer_bytes: usize,
    trailer_count: usize,
    done: bool,
}

#[derive(Debug)]
enum ChunkedDecoderState {
    /// Waiting for chunk header line.
    ReadingHeader,
    /// Reading chunk data bytes. `remaining` bytes left to read in current chunk.
    ReadingData {
        chunk_size: usize,
        remaining: usize,
        sig: Option<String>,
    },
    /// Consumed chunk data, expecting \r\n after it.
    ExpectingDataCrlf { seen_cr: bool },
    /// Reading trailers after terminal chunk.
    ReadingTrailers,
    /// Decoding complete.
    Done,
}

impl IncrementalChunkedDecoder {
    #[must_use]
    pub fn new(streaming: Option<StreamingSigningContext>, trailer_mode: bool) -> Self {
        Self::new_with_expected_len(streaming, trailer_mode, None)
    }

    #[must_use]
    pub fn new_with_expected_len(
        streaming: Option<StreamingSigningContext>,
        trailer_mode: bool,
        expected_decoded_len: Option<u64>,
    ) -> Self {
        let prev_sig = streaming.as_ref().map(|s| s.seed_signature.clone());
        Self {
            streaming,
            trailer_mode,
            prev_sig,
            chunk_number: 0,
            prev_chunk_size: None,
            expected_decoded_remaining: expected_decoded_len,
            current_chunk_hash: None,
            buf: Vec::new(),
            state: ChunkedDecoderState::ReadingHeader,
            trailers: Vec::new(),
            trailer_bytes: 0,
            trailer_count: 0,
            done: false,
        }
    }

    /// Feed wire bytes and extract decoded payload.
    ///
    /// Returns decoded payload bytes extracted from this batch. May return
    /// an empty vec if the wire data doesn't complete any payload chunks.
    ///
    /// Production callers should feed bounded wire slices. The decoder does not
    /// retain chunk payload bytes between calls, but the returned payload is
    /// owned so async callers never hold a decoder borrow across `.await`.
    pub fn feed(&mut self, mut wire: &[u8]) -> Result<Vec<u8>, ServerError> {
        if self.done {
            return Err(ServerError::MalformedChunkedBody {
                reason: "data after chunked body complete".to_string(),
            });
        }
        let mut payload = Vec::new();

        loop {
            match self.state {
                ChunkedDecoderState::ReadingHeader => {
                    let Some(line) = read_crlf_line(&mut self.buf, &mut wire)? else {
                        break;
                    };
                    let (chunk_size, chunk_sig) = parse_chunk_header(&line)?;

                    // Signed mode requires chunk-signature on every chunk.
                    if self.streaming.is_some() && chunk_sig.is_none() {
                        return Err(ServerError::MalformedChunkedBody {
                            reason: "missing chunk-signature in signed chunked upload".to_string(),
                        });
                    }

                    if chunk_size == 0 {
                        if let Some(remaining) = self.expected_decoded_remaining {
                            if remaining != 0 {
                                return Err(ServerError::MalformedChunkedBody {
                                    reason: format!(
                                        "decoded content length mismatch: {remaining} bytes remaining"
                                    ),
                                });
                            }
                        }
                        // Terminal chunk.
                        if let Some(ref ctx) = self.streaming {
                            let sig = chunk_sig.as_deref().unwrap();
                            verify_chunk_signature(
                                ctx,
                                self.prev_sig.as_deref().unwrap(),
                                b"",
                                sig,
                            )?;
                            self.prev_sig = Some(sig.to_string());
                        }
                        self.state = ChunkedDecoderState::ReadingTrailers;
                        continue;
                    }

                    // Validate minimum chunk size for non-final data chunks.
                    if let Some(prev_size) = self.prev_chunk_size {
                        if prev_size < MIN_CHUNK_SIZE {
                            return Err(ServerError::InvalidChunkSize {
                                chunk: self.chunk_number,
                                chunk_size: prev_size,
                                min_size: MIN_CHUNK_SIZE,
                            });
                        }
                    }

                    if let Some(remaining) = self.expected_decoded_remaining {
                        let chunk_size_u64 =
                            u64::try_from(chunk_size).map_err(|_| ServerError::ObjectTooLarge {
                                size: u64::MAX,
                                max: remaining,
                            })?;
                        if chunk_size_u64 > remaining {
                            return Err(ServerError::MalformedChunkedBody {
                                reason: format!(
                                    "chunk size {chunk_size_u64} exceeds remaining decoded content length {remaining}"
                                ),
                            });
                        }
                    }

                    self.current_chunk_hash = self
                        .streaming
                        .as_ref()
                        .map(|_| digest::Context::new(&digest::SHA256));
                    self.state = ChunkedDecoderState::ReadingData {
                        chunk_size,
                        remaining: chunk_size,
                        sig: chunk_sig,
                    };
                }
                ChunkedDecoderState::ReadingData {
                    chunk_size,
                    ref mut remaining,
                    ref sig,
                } => {
                    if wire.is_empty() {
                        break;
                    }
                    let take = (*remaining).min(wire.len());
                    let chunk_data = &wire[..take];
                    if let Some(ref mut ctx) = self.current_chunk_hash {
                        ctx.update(chunk_data);
                    }
                    if let Some(ref mut decoded_remaining) = self.expected_decoded_remaining {
                        *decoded_remaining -= u64::try_from(take).expect("usize fits in u64");
                    }
                    payload.extend_from_slice(chunk_data);
                    wire = &wire[take..];
                    *remaining -= take;
                    if *remaining != 0 {
                        break;
                    }

                    // Verify chunk signature.
                    if let Some(ref ctx) = self.streaming {
                        let s = sig.as_deref().unwrap();
                        let chunk_hash = self
                            .current_chunk_hash
                            .take()
                            .expect("signed chunks must have hash context")
                            .finish();
                        let chunk_hash_hex = hex_encode(chunk_hash.as_ref());
                        verify_chunk_signature_hash(
                            ctx,
                            self.prev_sig.as_deref().unwrap(),
                            &chunk_hash_hex,
                            s,
                        )?;
                        self.prev_sig = Some(s.to_string());
                    }

                    self.prev_chunk_size = Some(chunk_size);
                    self.chunk_number += 1;

                    self.state = ChunkedDecoderState::ExpectingDataCrlf { seen_cr: false };
                }
                ChunkedDecoderState::ExpectingDataCrlf { ref mut seen_cr } => {
                    if !*seen_cr {
                        let Some((&b, rest)) = wire.split_first() else {
                            break;
                        };
                        if b != b'\r' {
                            return Err(ServerError::MalformedChunkedBody {
                                reason: "missing CRLF after chunk data".to_string(),
                            });
                        }
                        wire = rest;
                        *seen_cr = true;
                    }
                    let Some((&b, rest)) = wire.split_first() else {
                        break;
                    };
                    if b != b'\n' {
                        return Err(ServerError::MalformedChunkedBody {
                            reason: "missing CRLF after chunk data".to_string(),
                        });
                    }
                    wire = rest;
                    self.state = ChunkedDecoderState::ReadingHeader;
                }
                ChunkedDecoderState::ReadingTrailers => {
                    // Try to parse trailer lines until empty line.
                    loop {
                        let Some(line) = read_crlf_line(&mut self.buf, &mut wire)? else {
                            return Ok(payload); // Need more data
                        };
                        if line.is_empty() {
                            // Verify trailer signature if needed.
                            if self.trailer_mode {
                                if let Some(ref ctx) = self.streaming {
                                    verify_trailer_signature(
                                        ctx,
                                        self.prev_sig.as_deref().unwrap(),
                                        &self.trailers,
                                    )?;
                                }
                            }

                            // Strip trailer signature from returned trailers.
                            self.trailers
                                .retain(|(k, _)| k != "x-amz-trailer-signature");

                            self.state = ChunkedDecoderState::Done;
                            self.done = true;
                            break;
                        }

                        self.trailer_count += 1;
                        self.trailer_bytes += line.len() + 2;
                        if self.trailer_count > MAX_CHUNKED_TRAILER_COUNT
                            || self.trailer_bytes > MAX_CHUNKED_TRAILER_BYTES
                        {
                            return Err(chunked_trailers_too_large());
                        }

                        let line_str = std::str::from_utf8(&line).map_err(|_| {
                            ServerError::MalformedChunkedBody {
                                reason: "non-UTF8 trailer".to_string(),
                            }
                        })?;

                        if let Some((key, value)) = line_str.split_once(':') {
                            self.trailers
                                .push((key.trim().to_ascii_lowercase(), value.trim().to_string()));
                        } else {
                            return Err(ServerError::IncompleteBody);
                        }
                    }
                    break;
                }
                ChunkedDecoderState::Done => break,
            }
        }

        Ok(payload)
    }

    /// Check if decoding is complete (terminal chunk + trailers processed).
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Consume the decoder and return trailers. Panics if not done.
    #[must_use]
    pub fn into_trailers(self) -> Vec<(String, String)> {
        self.trailers
    }
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
        let scope = format!("{date}/{region}/{service}/aws4_request");

        // Derive signing key.
        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey::new(secret.to_string()),
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
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{seed_sig}\n{empty_hash}\n{chunk_hash}"
        );
        let key = hmac::Key::new(hmac::HMAC_SHA256, &key_bytes);
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        // Compute terminal chunk signature (size=0, data=empty).
        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{chunk_sig}\n{empty_hash}\n{terminal_hash}"
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        let wire = format!(
            "5;chunk-signature={chunk_sig}\r\nHello\r\n0;chunk-signature={terminal_sig}\r\n\r\n"
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
        let scope = format!("{date}/{region}/{service}/aws4_request");

        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey::new(secret.to_string()),
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
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{seed_sig}\n{empty_hash}\n{chunk_hash}"
        );
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        // Terminal chunk signature.
        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{chunk_sig}\n{empty_hash}\n{terminal_hash}"
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        // Trailer signature: AWS4-HMAC-SHA256-TRAILER\n{ts}\n{scope}\n{terminal_sig}\n{sha256(canonical_trailers)}
        let canonical_trailers = "x-amz-checksum-crc32:abcd1234\n";
        let trailer_hash = auth::canonical::sha256_hex(canonical_trailers.as_bytes());
        let sts_trailer = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{timestamp}\n{scope}\n{terminal_sig}\n{trailer_hash}"
        );
        let trailer_sig = hex_encode(hmac::sign(&key, sts_trailer.as_bytes()).as_ref());

        let wire = format!(
            "5;chunk-signature={chunk_sig}\r\nHello\r\n0;chunk-signature={terminal_sig}\r\nx-amz-checksum-crc32:abcd1234\r\nx-amz-trailer-signature:{trailer_sig}\r\n\r\n"
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
        let scope = format!("{date}/{region}/{service}/aws4_request");

        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey::new(secret.to_string()),
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
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{seed_sig}\n{empty_hash}\n{chunk_hash}"
        );
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{chunk_sig}\n{empty_hash}\n{terminal_hash}"
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        // Bad trailer signature.
        let bad_trailer_sig = "0".repeat(64);

        let wire = format!(
            "5;chunk-signature={chunk_sig}\r\nHello\r\n0;chunk-signature={terminal_sig}\r\nx-amz-checksum-crc32:abcd1234\r\nx-amz-trailer-signature:{bad_trailer_sig}\r\n\r\n"
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

    // ── IncrementalChunkedDecoder tests ──────────────────────────────

    #[test]
    fn incremental_single_feed() {
        let mut dec = IncrementalChunkedDecoder::new(None, false);
        let wire = b"5\r\nhello\r\n0\r\n\r\n";
        let payload = dec.feed(wire).unwrap();
        assert_eq!(payload, b"hello");
        assert!(dec.is_done());
        assert!(dec.into_trailers().is_empty());
    }

    #[test]
    fn incremental_byte_at_a_time() {
        let mut dec = IncrementalChunkedDecoder::new(None, false);
        let wire = b"5\r\nhello\r\n0\r\n\r\n";
        let mut all_payload = Vec::new();
        for &b in wire {
            let chunk = dec.feed(&[b]).unwrap();
            all_payload.extend_from_slice(&chunk);
        }
        assert_eq!(all_payload, b"hello");
        assert!(dec.is_done());
    }

    #[test]
    fn incremental_with_trailers() {
        let mut dec = IncrementalChunkedDecoder::new(None, true);
        let wire = b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:abcd1234\r\n\r\n";
        let payload = dec.feed(wire).unwrap();
        assert_eq!(payload, b"hello");
        assert!(dec.is_done());
        let trailers = dec.into_trailers();
        assert_eq!(trailers.len(), 1);
        assert_eq!(trailers[0].0, "x-amz-checksum-crc32");
        assert_eq!(trailers[0].1, "abcd1234");
    }

    #[test]
    fn incremental_split_across_chunks() {
        let mut dec = IncrementalChunkedDecoder::new(None, false);
        // Feed header + partial data
        let payload1 = dec.feed(b"5\r\nhel").unwrap();
        assert_eq!(payload1, b"hel");
        // Feed rest of data + terminal
        let payload2 = dec.feed(b"lo\r\n0\r\n\r\n").unwrap();
        assert_eq!(payload2, b"lo");
        assert!(dec.is_done());
    }

    #[test]
    fn incremental_multiple_chunks() {
        let mut dec = IncrementalChunkedDecoder::new(None, false);
        // Two chunks: 8192-byte chunk + 4-byte chunk
        let chunk1 = vec![b'A'; 8192];
        let mut wire = Vec::new();
        wire.extend_from_slice(format!("{:x}\r\n", chunk1.len()).as_bytes());
        wire.extend_from_slice(&chunk1);
        wire.extend_from_slice(b"\r\n4\r\ntail\r\n0\r\n\r\n");

        let payload = dec.feed(&wire).unwrap();
        let mut expected = chunk1;
        expected.extend_from_slice(b"tail");
        assert_eq!(payload, expected);
        assert!(dec.is_done());
    }

    #[test]
    fn incremental_small_non_final_chunk_rejected() {
        let mut dec = IncrementalChunkedDecoder::new(None, false);
        let wire = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let err = dec.feed(wire).unwrap_err();
        assert!(matches!(err, ServerError::InvalidChunkSize { .. }));
    }

    #[test]
    fn incremental_chunk_larger_than_remaining_decoded_length_rejected() {
        let mut dec = IncrementalChunkedDecoder::new_with_expected_len(None, false, Some(4));
        let err = dec.feed(b"5\r\n").unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn incremental_chunk_header_line_too_long_rejected() {
        let mut dec = IncrementalChunkedDecoder::new(None, false);
        let line = vec![b'a'; MAX_CHUNKED_LINE_BYTES + 1];
        let err = dec.feed(&line).unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn incremental_trailer_line_too_long_rejected() {
        let mut dec = IncrementalChunkedDecoder::new(None, true);
        assert!(dec.feed(b"0\r\n").unwrap().is_empty());
        let line = vec![b'a'; MAX_CHUNKED_LINE_BYTES + 1];
        let err = dec.feed(&line).unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn incremental_too_many_complete_trailers_rejected() {
        let mut dec = IncrementalChunkedDecoder::new(None, true);
        assert!(dec.feed(b"0\r\n").unwrap().is_empty());

        let mut trailers = Vec::new();
        for index in 0..=MAX_CHUNKED_TRAILER_COUNT {
            trailers.extend_from_slice(format!("x-test-{index}: value\r\n").as_bytes());
        }
        let err = dec.feed(&trailers).unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn incremental_total_trailer_bytes_too_large_rejected() {
        let mut dec = IncrementalChunkedDecoder::new(None, true);
        assert!(dec.feed(b"0\r\n").unwrap().is_empty());

        let value = "a".repeat(1024);
        let mut trailers = Vec::new();
        for index in 0..MAX_CHUNKED_TRAILER_COUNT {
            trailers.extend_from_slice(format!("x-test-{index}: {value}\r\n").as_bytes());
            if trailers.len() > MAX_CHUNKED_TRAILER_BYTES {
                break;
            }
        }
        let err = dec.feed(&trailers).unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn incremental_terminal_before_expected_decoded_length_rejected() {
        let mut dec = IncrementalChunkedDecoder::new_with_expected_len(None, false, Some(5));
        let err = dec.feed(b"0\r\n\r\n").unwrap_err();
        assert!(matches!(err, ServerError::MalformedChunkedBody { .. }));
    }

    #[test]
    fn incremental_expected_decoded_length_allows_exact_body() {
        let mut dec = IncrementalChunkedDecoder::new_with_expected_len(None, false, Some(5));
        let payload = dec.feed(b"5\r\nhello\r\n0\r\n\r\n").unwrap();
        assert_eq!(payload, b"hello");
        assert!(dec.is_done());
    }

    #[test]
    fn incremental_signed_verification() {
        // Reuse the same signing setup as the batch decoder test.
        let chunk_data = b"Hello";
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = "20130524";
        let region = "us-east-1";
        let service = "s3";
        let timestamp = "20130524T000000Z";
        let scope = format!("{date}/{region}/{service}/aws4_request");

        let signing_key = auth::sigv4::derive_signing_key(
            &auth::SecretKey::new(secret.to_string()),
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
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{seed_sig}\n{empty_hash}\n{chunk_hash}"
        );
        let chunk_sig = hex_encode(hmac::sign(&key, sts.as_bytes()).as_ref());

        let terminal_hash = auth::canonical::sha256_hex(b"");
        let sts_terminal = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{chunk_sig}\n{empty_hash}\n{terminal_hash}"
        );
        let terminal_sig = hex_encode(hmac::sign(&key, sts_terminal.as_bytes()).as_ref());

        let wire = format!(
            "5;chunk-signature={chunk_sig}\r\nHello\r\n0;chunk-signature={terminal_sig}\r\n\r\n"
        );

        let mut dec = IncrementalChunkedDecoder::new(Some(ctx), false);
        let payload = dec.feed(wire.as_bytes()).unwrap();
        assert_eq!(payload, b"Hello");
        assert!(dec.is_done());
    }
}
