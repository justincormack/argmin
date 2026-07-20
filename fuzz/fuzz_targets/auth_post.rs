#![no_main]

use auth::{
    authenticate_post_sigv4, ExpectedCredentialScope, ExpectedSigningRegion, PostSigV4Request,
};
use base64::Engine;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 8);
    let algorithm = argmin_fuzz::lossy(chunks[0]);
    let credential = argmin_fuzz::lossy(chunks[1]);
    let date = argmin_fuzz::lossy(chunks[2]);
    let signature = argmin_fuzz::lossy(chunks[3]);
    let expiration = argmin_fuzz::lossy(chunks[4]);
    let key_prefix = argmin_fuzz::lossy(chunks[5]);
    let token_one = argmin_fuzz::lossy(chunks[6]);
    let token_two = argmin_fuzz::lossy(chunks[7]);
    let security_tokens = match data.first().copied().unwrap_or_default() % 4 {
        0 => Vec::new(),
        1 => vec![token_one.as_str()],
        2 => vec![token_one.as_str(), token_one.as_str()],
        _ => vec![token_one.as_str(), token_two.as_str()],
    };

    let store = argmin_fuzz::seeded_store();

    let raw_policy_b64 = base64::engine::general_purpose::STANDARD.encode(data);
    let _ = authenticate_post_sigv4(
        PostSigV4Request {
            algorithm: &algorithm,
            credential: &credential,
            date: &date,
            policy_b64: &raw_policy_b64,
            signature_hex: &signature,
            security_tokens: &security_tokens,
        },
        &store,
        ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        1_700_000_000,
    );

    let generated_policy = serde_json::json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": "bucket"},
            ["starts-with", "$key", key_prefix],
            {"x-amz-algorithm": "AWS4-HMAC-SHA256"}
        ]
    });
    let generated_policy_b64 =
        base64::engine::general_purpose::STANDARD.encode(generated_policy.to_string());
    let fields = vec![
        ("key", "key"),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", credential.as_str()),
        ("x-amz-date", date.as_str()),
    ];
    let _ = auth::validate_post_policy(
        &generated_policy_b64,
        &fields,
        data.len(),
        "bucket",
        1_700_000_000,
    );
});
