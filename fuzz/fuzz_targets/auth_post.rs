#![no_main]

use auth::authenticate_post_sigv4;
use base64::Engine;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 6);
    let algorithm = argmin_fuzz::lossy(chunks[0]);
    let credential = argmin_fuzz::lossy(chunks[1]);
    let date = argmin_fuzz::lossy(chunks[2]);
    let signature = argmin_fuzz::lossy(chunks[3]);
    let expiration = argmin_fuzz::lossy(chunks[4]);
    let key_prefix = argmin_fuzz::lossy(chunks[5]);

    let store = argmin_fuzz::seeded_store();

    let raw_policy_b64 = base64::engine::general_purpose::STANDARD.encode(data);
    let _ = authenticate_post_sigv4(
        &algorithm,
        &credential,
        &date,
        &raw_policy_b64,
        &signature,
        &store,
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
