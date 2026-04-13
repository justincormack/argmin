#![no_main]

use auth::StreamingSigningContext;
use libfuzzer_sys::fuzz_target;
use server_http::http::chunked::IncrementalChunkedDecoder;

fn seeded_context(seed: &[u8], trailer_seed: &[u8]) -> StreamingSigningContext {
    let mut signing_key = [0u8; 32];
    for (index, byte) in seed.iter().copied().enumerate() {
        signing_key[index % signing_key.len()] ^= byte;
    }

    let mut seed_signature = String::with_capacity(64);
    for byte in seed.iter().copied().cycle().take(32) {
        use std::fmt::Write;
        let _ = write!(&mut seed_signature, "{byte:02x}");
    }

    StreamingSigningContext {
        signing_key,
        seed_signature,
        scope: argmin_fuzz::lossy(seed),
        timestamp: argmin_fuzz::lossy(trailer_seed),
    }
}

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 4);
    let mode = chunks[0].first().copied().unwrap_or_default() % 4;
    let streaming = match mode {
        1 | 2 => Some(seeded_context(chunks[0], chunks[1])),
        _ => None,
    };
    let trailer_mode = matches!(mode, 2 | 3);

    let mut decoder = IncrementalChunkedDecoder::new(streaming, trailer_mode);
    for chunk in argmin_fuzz::chunk_by_controls(chunks[3], chunks[2]) {
        let _ = decoder.feed(chunk);
        if decoder.is_done() {
            let _ = decoder.into_trailers();
            return;
        }
    }

    if decoder.is_done() {
        let _ = decoder.into_trailers();
    }
});
