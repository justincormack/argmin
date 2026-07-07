use aws_sdk_s3::types::EncodingType;
use s3_tests::{
    assert_s3_err_code, create_objects, create_objects_with_keys, delete_all_and_bucket,
    err_status, raw_bucket, send_signed_request,
    shape::{assert_shape, error_response_headers, expected_error, shape, xml_response_headers},
    unique_bucket, SendRetryingOperationAborted, CTX,
};
use std::time::Duration;

fn assert_canonical_owner_id(id: &str) {
    assert_eq!(
        id.len(),
        64,
        "expected 64-char canonical owner ID, got {id}"
    );
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "expected lowercase hex canonical owner ID, got {id}"
    );
}

// ── Test data sets ──────────────────────────────────────────────────

/// Set A — 6 keys for delimiter tests.
const SET_A: &[&str] = &["asdf", "boo/bar", "boo/baz/xyzzy", "cquux", "thud", "zoo"];

/// Set B — 7 keys for prefix/general tests.
const SET_B: &[&str] = &["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"];

const CONTROL_KEY_CASES: &[(&str, &str)] = &[
    ("bad\u{0001}key", "bad%01key"),
    ("bad\u{001F}key", "bad%1Fkey"),
    ("bad\u{007F}key", "bad%7Fkey"),
    ("bad\u{0080}key", "bad%C2%80key"),
];

const XML_SPECIAL_KEY: &str = "xml<>&\"key";
const XML_SPECIAL_KEY_ENCODED: &str = "xml%3C%3E%26%22key";
const ECHO_FIELD_VALUE: &str = "echo\u{0001}<>&\"+";

// ── Local helpers ───────────────────────────────────────────────────

fn get_keys(objects: &[aws_sdk_s3::types::Object]) -> Vec<String> {
    objects
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}

fn get_prefixes(prefixes: &[aws_sdk_s3::types::CommonPrefix]) -> Vec<String> {
    prefixes
        .iter()
        .filter_map(|p| p.prefix().map(str::to_string))
        .collect()
}

fn expected_raw_list_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\u{0001}'..='\u{0008}' | '\u{000B}' | '\u{000C}' | '\u{000E}'..='\u{001F}' => {
                escaped.push_str(&format!("&#x{:x};", ch as u32));
            }
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn expected_raw_list_key(decoded_key: &str) -> String {
    format!("<Key>{}</Key>", expected_raw_list_value(decoded_key))
}

fn expected_url_list_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn query_encode_value(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn put_raw_object(bucket: &str, encoded_key: &str) {
    let url = format!("{}/{bucket}/{encoded_key}", CTX.endpoint());
    let response = send_signed_request("PUT", &url, b"data", std::iter::empty::<(&str, &str)>());
    assert_eq!(response.status, 200, "unexpected body: {}", response.body);
}

fn delete_raw_object(bucket: &str, encoded_key: &str) {
    let url = format!("{}/{bucket}/{encoded_key}", CTX.endpoint());
    let response = send_signed_request("DELETE", &url, b"", std::iter::empty::<(&str, &str)>());
    assert_eq!(response.status, 204, "unexpected body: {}", response.body);
}

async fn head_object_eventually_after_versioning_enable(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::head_object::HeadObjectOutput {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("head object after enabling versioning")
            .await;
        match result {
            Ok(head) => return head,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => panic!("head_object after enabling versioning: {err:?}"),
        }
    }

    unreachable!("loop should return or panic")
}

// ── Empty / basic ───────────────────────────────────────────────────

#[test]
fn test_list_objects_v2_no_such_bucket_error_shape() {
    s3_tests::run(async {
        let missing_bucket = unique_bucket();
        let response = raw_bucket("GET", &missing_bucket, Some("list-type=2"));
        assert_shape(
            "ListObjectsV2 missing bucket",
            &response,
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body(expected_error::no_such_bucket(&missing_bucket)),
        );
    });
}

#[test]
fn test_bucket_list_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_v2_expected_bucket_owner() {
    s3_tests::run(async {
        let account_id = CTX.account_id().to_string();
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["foo", "bar"]).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .customize()
            .mutate_request({
                let account_id = account_id.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-expected-bucket-owner", account_id.clone());
                }
            })
            .send()
            .await
            .unwrap();
        let result_keys = get_keys(resp.contents());
        assert_eq!(result_keys, vec!["bar", "foo"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_v2_wrong_expected_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["foo", "bar"]).await;

        let result = client
            .list_objects_v2()
            .bucket(&bucket)
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-expected-bucket-owner", "000000000000");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_distinct() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket_a, keys_a) = create_objects_with_keys(client, &["foo", "bar"]).await;
        let bucket_b = unique_bucket();
        s3_tests::create_bucket(client, &bucket_b).await.unwrap();

        let resp = client
            .list_objects()
            .bucket(&bucket_b)
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket_b).await;
        delete_all_and_bucket(client, &bucket_a, &keys_a).await;
    });
}

#[test]
fn test_bucket_list_unordered() {
    s3_tests::run(async {
        let client = CTX.client();
        // Create in non-sorted order
        let (bucket, keys) =
            create_objects_with_keys(client, &["zoo", "asdf", "mango", "bar"]).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        let result_keys = get_keys(resp.contents());
        assert_eq!(result_keys, vec!["asdf", "bar", "mango", "zoo"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V1 max-keys / pagination ───────────────────────────────────────

#[test]
fn test_bucket_list_many() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 35).await;

        let mut collected = Vec::new();
        let mut marker = String::new();
        loop {
            let mut req = client.list_objects().bucket(&bucket).max_keys(2);
            if !marker.is_empty() {
                req = req.marker(&marker);
            }
            let resp = req
                .send_retrying_operation_aborted("list bucket objects")
                .await
                .unwrap();
            let page_keys = get_keys(resp.contents());
            if let Some(last) = page_keys.last() {
                marker = last.clone();
            }
            collected.extend(page_keys);
            if resp.is_truncated() != Some(true) {
                break;
            }
        }

        assert_eq!(collected.len(), 35);
        let mut sorted = collected.clone();
        sorted.sort();
        assert_eq!(collected, sorted);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_maxkeys_one() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .max_keys(1)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 1);
        assert_eq!(resp.is_truncated(), Some(true));

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_maxkeys_zero() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .max_keys(0)
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_maxkeys_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"]
        );
        assert_eq!(resp.is_truncated(), Some(false));

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V2 max-keys / pagination ───────────────────────────────────────

#[test]
fn test_bucket_listv2_many() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 35).await;

        let mut collected = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = client.list_objects_v2().bucket(&bucket).max_keys(2);
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req
                .send_retrying_operation_aborted("list bucket objects")
                .await
                .unwrap();
            collected.extend(get_keys(resp.contents()));
            if resp.is_truncated() != Some(true) {
                break;
            }
            continuation_token = resp.next_continuation_token().map(String::from);
        }

        assert_eq!(collected.len(), 35);
        let mut sorted = collected.clone();
        sorted.sort();
        assert_eq!(collected, sorted);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_maxkeys_one() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(1)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 1);
        assert_eq!(resp.is_truncated(), Some(true));

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_maxkeys_zero() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(0)
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_maxkeys_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"]
        );
        assert_eq!(resp.is_truncated(), Some(false));
        assert_eq!(resp.key_count(), Some(7));

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V2 ordering ─────────────────────────────────────────────────────

#[test]
fn test_bucket_listv2_unordered() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) =
            create_objects_with_keys(client, &["zoo", "asdf", "mango", "bar"]).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let result_keys = get_keys(resp.contents());
        assert_eq!(result_keys, vec!["asdf", "bar", "mango", "zoo"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V1 delimiter ────────────────────────────────────────────────────

#[test]
fn test_bucket_list_delimiter_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["asdf", "cquux", "thud", "zoo"]
        );
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["boo/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_alt() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("a")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["cquux", "thud", "zoo"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["a", "boo/ba"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_percentage() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("%")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_whitespace() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter(" ")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_dot() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter(".")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("\x0a")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_prefix_ends_with_delimiter() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("boo/")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["boo/bar"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["boo/baz/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        // Keys that don't contain the delimiter "/"
        let (bucket, keys) =
            create_objects_with_keys(client, &["bar", "baz", "cab", "dog", "foo", "quux"]).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["bar", "baz", "cab", "dog", "foo", "quux"]
        );
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_not_skip_special() {
    s3_tests::run(async {
        let client = CTX.client();
        // Keys under "0/" prefix plus some keys that sort after the prefix
        let mut key_strs: Vec<String> = vec!["0/".to_string()];
        for i in 0..10 {
            key_strs.push(format!("0/{}", i));
        }
        key_strs.extend(["1", "2", "3"].iter().map(|s| s.to_string()));
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let (bucket, keys) = create_objects_with_keys(client, &key_refs).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();
        // Keys after the "0/" prefix should not be skipped
        assert_eq!(get_keys(resp.contents()), vec!["1", "2", "3"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["0/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_prefix_underscore() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(
            client,
            &[
                "Obj1_",
                "Under1/bar",
                "Under1/baz/xyzzy",
                "Under2/thud",
                "Under2/bla",
            ],
        )
        .await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("Under1/")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["Under1/bar"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["Under1/baz/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_delimiter_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        // Paginate with max_keys=1 through delimiter="/" results
        let mut all_keys = Vec::new();
        let mut all_prefixes = Vec::new();
        let mut marker = String::new();
        loop {
            let mut req = client
                .list_objects()
                .bucket(&bucket)
                .delimiter("/")
                .max_keys(1);
            if !marker.is_empty() {
                req = req.marker(&marker);
            }
            let resp = req
                .send_retrying_operation_aborted("list bucket objects")
                .await
                .unwrap();

            let page_keys = get_keys(resp.contents());
            let page_prefixes = get_prefixes(resp.common_prefixes());

            if resp.is_truncated() != Some(true) {
                all_keys.extend(page_keys);
                all_prefixes.extend(page_prefixes);
                break;
            }

            // Advance marker: use next_marker, or last key/prefix
            marker = resp
                .next_marker()
                .map(String::from)
                .or_else(|| page_keys.last().cloned())
                .or_else(|| page_prefixes.last().cloned())
                .unwrap();

            all_keys.extend(page_keys);
            all_prefixes.extend(page_prefixes);
        }

        assert_eq!(all_keys, vec!["asdf", "cquux", "thud", "zoo"]);
        assert_eq!(all_prefixes, vec!["boo/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_delimiter_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .delimiter("/")
            .prefix("boo/")
            .send()
            .await
            .unwrap();

        let result_keys = get_keys(resp.contents());
        let result_prefixes = get_prefixes(resp.common_prefixes());
        assert_eq!(result_keys, vec!["boo/bar"]);
        assert_eq!(result_prefixes, vec!["boo/baz/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_delimiter_alt() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        // Prefix + alternative (non-"/") delimiter
        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("boo/ba")
            .delimiter("r")
            .send()
            .await
            .unwrap();
        // "boo/bar" → remaining "r", "r" at 0 → prefix "boo/bar"
        // "boo/baz/xyzzy" → remaining "z/xyzzy", no "r" → content
        assert_eq!(get_keys(resp.contents()), vec!["boo/baz/xyzzy"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["boo/bar"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V2 delimiter ────────────────────────────────────────────────────

#[test]
fn test_bucket_listv2_delimiter_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["asdf", "cquux", "thud", "zoo"]
        );
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["boo/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_alt() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("a")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["cquux", "thud", "zoo"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["a", "boo/ba"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_percentage() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("%")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_whitespace() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter(" ")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_dot() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter(".")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("\x0a")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 6);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_prefix_ends_with_delimiter() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("boo/")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["boo/bar"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["boo/baz/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) =
            create_objects_with_keys(client, &["bar", "baz", "cab", "dog", "foo", "quux"]).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["bar", "baz", "cab", "dog", "foo", "quux"]
        );
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_not_skip_special() {
    s3_tests::run(async {
        let client = CTX.client();
        let mut key_strs: Vec<String> = vec!["0/".to_string()];
        for i in 0..10 {
            key_strs.push(format!("0/{}", i));
        }
        key_strs.extend(["1", "2", "3"].iter().map(|s| s.to_string()));
        let key_refs: Vec<&str> = key_strs.iter().map(|s| s.as_str()).collect();
        let (bucket, keys) = create_objects_with_keys(client, &key_refs).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["1", "2", "3"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["0/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_dedupes_folder_object_common_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) =
            create_objects_with_keys(client, &["folder/file.txt", "folder/"]).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .send()
            .await
            .unwrap();

        assert!(
            get_keys(resp.contents()).is_empty(),
            "expected folder marker to be rolled into CommonPrefixes, got contents={:?}",
            get_keys(resp.contents())
        );
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["folder/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_prefix_underscore() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(
            client,
            &[
                "Obj1_",
                "Under1/bar",
                "Under1/baz/xyzzy",
                "Under2/thud",
                "Under2/bla",
            ],
        )
        .await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("Under1/")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["Under1/bar"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["Under1/baz/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_delimiter_alt() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("boo/ba")
            .delimiter("r")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["boo/baz/xyzzy"]);
        assert_eq!(get_prefixes(resp.common_prefixes()), vec!["boo/bar"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V1 prefix ───────────────────────────────────────────────────────

#[test]
fn test_bucket_list_prefix_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("foo/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["foo/bar", "foo/baz"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_alt() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("ba")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["bar", "baz"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("doesnotexist")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("\x0a")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_delimiter_prefix_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("doesnotexist")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_delimiter_delimiter_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // Prefix "b" matches "bar" and "baz"; delimiter "/" not in remaining
        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("b")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["bar", "baz"]);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("doesnotexist")
            .delimiter("!")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V2 prefix ───────────────────────────────────────────────────────

#[test]
fn test_bucket_listv2_prefix_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("foo/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["foo/bar", "foo/baz"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_alt() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("ba")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["bar", "baz"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("doesnotexist")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("\x0a")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_delimiter_prefix_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("doesnotexist")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_delimiter_delimiter_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("b")
            .delimiter("/")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()), vec!["bar", "baz"]);
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("doesnotexist")
            .delimiter("!")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());
        assert!(resp.common_prefixes().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V1 marker ───────────────────────────────────────────────────────

#[test]
fn test_bucket_list_marker_none() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"]
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_marker_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .marker("")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"]
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_marker_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // \x0a sorts before all Set B keys
        let resp = client
            .list_objects()
            .bucket(&bucket)
            .marker("\x0a")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_marker_not_in_list() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // "bzz" is between "baz" and "cab"
        let resp = client
            .list_objects()
            .bucket(&bucket)
            .marker("bzz")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["cab", "dog", "foo/bar", "foo/baz", "quux"]
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_marker_after_list() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .marker("zzz")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V2 continuation / start-after ───────────────────────────────────

#[test]
fn test_bucket_listv2_continuationtoken() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // First page: max_keys=1
        let resp1 = client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.is_truncated(), Some(true));
        let first_keys = get_keys(resp1.contents());
        assert_eq!(first_keys, vec!["bar"]);
        let token = resp1.next_continuation_token().unwrap();

        // Second page using continuation token
        let resp2 = client
            .list_objects_v2()
            .bucket(&bucket)
            .continuation_token(token)
            .send()
            .await
            .unwrap();
        let remaining_keys = get_keys(resp2.contents());
        assert_eq!(
            remaining_keys,
            vec!["baz", "cab", "dog", "foo/bar", "foo/baz", "quux"]
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_continuationtoken_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // AWS rejects empty continuation token with 400 InvalidArgument
        let result = client
            .list_objects_v2()
            .bucket(&bucket)
            .continuation_token("")
            .send()
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 400);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_both_continuationtoken_startafter() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // Get a continuation token from the first page
        let resp1 = client
            .list_objects_v2()
            .bucket(&bucket)
            .max_keys(1)
            .send()
            .await
            .unwrap();
        let token = resp1.next_continuation_token().unwrap().to_string();

        // Use both continuation_token and start_after
        // continuation_token takes precedence; start_after="zzz" is ignored
        let resp2 = client
            .list_objects_v2()
            .bucket(&bucket)
            .continuation_token(&token)
            .start_after("zzz")
            .send()
            .await
            .unwrap();
        let result_keys = get_keys(resp2.contents());
        assert_eq!(
            result_keys,
            vec!["baz", "cab", "dog", "foo/bar", "foo/baz", "quux"]
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_startafter_unreadable() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .start_after("\x0a")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_startafter_not_in_list() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .start_after("bzz")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get_keys(resp.contents()),
            vec!["cab", "dog", "foo/bar", "foo/baz", "quux"]
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_startafter_after_list() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .start_after("zzz")
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── V2 fetch-owner ──────────────────────────────────────────────────

#[test]
fn test_bucket_listv2_fetchowner_notempty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .fetch_owner(true)
            .send()
            .await
            .unwrap();
        for obj in resp.contents() {
            let owner = obj
                .owner()
                .unwrap_or_else(|| panic!("expected owner for key {:?}", obj.key()));
            let owner_id = owner
                .id()
                .unwrap_or_else(|| panic!("expected owner ID for key {:?}", obj.key()));
            assert_canonical_owner_id(owner_id);
        }

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_fetchowner_defaultempty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // Default (no fetch_owner) — owner should be absent
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        for obj in resp.contents() {
            assert!(
                obj.owner().is_none(),
                "expected no owner for key {:?}",
                obj.key()
            );
        }

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_fetchowner_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_B).await;

        // Explicit fetch_owner=false
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .fetch_owner(false)
            .send()
            .await
            .unwrap();
        for obj in resp.contents() {
            assert!(
                obj.owner().is_none(),
                "expected no owner for key {:?}",
                obj.key()
            );
        }

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── Encoding ────────────────────────────────────────────────────────

#[test]
fn test_bucket_list_encoding_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) =
            create_objects_with_keys(client, &["foo+1", "foo/bar", "foo 3", "foo&4"]).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 4);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_encoding_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) =
            create_objects_with_keys(client, &["foo+1", "foo/bar", "foo 3", "foo&4"]).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 4);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_encoding_url_uses_plus_for_spaces() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["foo 3"]).await;

        let url = format!("{}/{bucket}?encoding-type=url", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<Key>foo+3</Key>"),
            "unexpected body: {}",
            response.body
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_without_encoding_type_keeps_spaces_literal() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["foo 3"]).await;

        let url = format!("{}/{bucket}", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<Key>foo 3</Key>"),
            "unexpected body: {}",
            response.body
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_encoding_url_uses_plus_for_spaces() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["foo 3"]).await;

        let url = format!("{}/{bucket}?list-type=2&encoding-type=url", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<Key>foo+3</Key>"),
            "unexpected body: {}",
            response.body
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_without_encoding_type_keeps_spaces_literal() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["foo 3"]).await;

        let url = format!("{}/{bucket}?list-type=2", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<Key>foo 3</Key>"),
            "unexpected body: {}",
            response.body
        );

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_without_encoding_type_keeps_control_characters_literal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            put_raw_object(&bucket, encoded_key);
        }

        let url = format!("{}/{bucket}", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        for &(decoded_key, _) in CONTROL_KEY_CASES {
            let needle = expected_raw_list_key(decoded_key);
            assert!(
                response.body.contains(&needle),
                "missing raw key {:?} (bytes {:?}) in body bytes {:?}",
                decoded_key,
                needle.as_bytes(),
                response.body.as_bytes()
            );
        }

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            delete_raw_object(&bucket, encoded_key);
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_encoding_type_url_encodes_control_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            put_raw_object(&bucket, encoded_key);
        }

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .unwrap();
        let mut keys = get_keys(resp.contents());
        let mut expected: Vec<String> = CONTROL_KEY_CASES
            .iter()
            .map(|(_, encoded_key)| (*encoded_key).to_string())
            .collect();
        keys.sort();
        expected.sort();
        assert_eq!(keys, expected);

        let url = format!("{}/{bucket}?encoding-type=url", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        for &(_, encoded_key) in CONTROL_KEY_CASES {
            assert!(
                response.body.contains(&format!("<Key>{encoded_key}</Key>")),
                "unexpected body: {}",
                response.body
            );
        }

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            delete_raw_object(&bucket, encoded_key);
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_without_encoding_type_keeps_control_characters_literal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            put_raw_object(&bucket, encoded_key);
        }

        let url = format!("{}/{bucket}?list-type=2", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        for &(decoded_key, _) in CONTROL_KEY_CASES {
            let needle = expected_raw_list_key(decoded_key);
            assert!(
                response.body.contains(&needle),
                "missing raw key {:?} (bytes {:?}) in body bytes {:?}",
                decoded_key,
                needle.as_bytes(),
                response.body.as_bytes()
            );
        }

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            delete_raw_object(&bucket, encoded_key);
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_without_encoding_type_escapes_xml_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);

        let url = format!("{}/{bucket}", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response
                .body
                .contains(&expected_raw_list_key(XML_SPECIAL_KEY)),
            "unexpected body: {}",
            response.body
        );

        delete_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_encoding_type_url_encodes_xml_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);

        let url = format!("{}/{bucket}?encoding-type=url", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{XML_SPECIAL_KEY_ENCODED}</Key>")),
            "unexpected body: {}",
            response.body
        );

        delete_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_without_encoding_type_escapes_xml_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);

        let url = format!("{}/{bucket}?list-type=2", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response
                .body
                .contains(&expected_raw_list_key(XML_SPECIAL_KEY)),
            "unexpected body: {}",
            response.body
        );

        delete_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_encoding_type_url_encodes_xml_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);

        let url = format!("{}/{bucket}?list-type=2&encoding-type=url", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{XML_SPECIAL_KEY_ENCODED}</Key>")),
            "unexpected body: {}",
            response.body
        );

        delete_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_encoding_type_url_encodes_control_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            put_raw_object(&bucket, encoded_key);
        }

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .unwrap();
        let mut keys = get_keys(resp.contents());
        let mut expected: Vec<String> = CONTROL_KEY_CASES
            .iter()
            .map(|(_, encoded_key)| (*encoded_key).to_string())
            .collect();
        keys.sort();
        expected.sort();
        assert_eq!(keys, expected);

        let url = format!("{}/{bucket}?list-type=2&encoding-type=url", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        for &(_, encoded_key) in CONTROL_KEY_CASES {
            assert!(
                response.body.contains(&format!("<Key>{encoded_key}</Key>")),
                "unexpected body: {}",
                response.body
            );
        }

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            delete_raw_object(&bucket, encoded_key);
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_echoed_fields_without_encoding_type_escape_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let url = format!(
            "{}/{bucket}?list-type=2&prefix={}&delimiter={}&start-after={}",
            CTX.endpoint(),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_raw_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Delimiter>{}</Delimiter>",
                expected_raw_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<StartAfter>{}</StartAfter>",
                expected_raw_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_echoed_fields_with_encoding_type_url_encode_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let url = format!(
            "{}/{bucket}?list-type=2&encoding-type=url&prefix={}&delimiter={}&start-after={}",
            CTX.endpoint(),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_url_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Delimiter>{}</Delimiter>",
                expected_url_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<StartAfter>{}</StartAfter>",
                expected_url_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv1_echoed_fields_without_encoding_type_escape_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let url = format!(
            "{}/{bucket}?prefix={}&marker={}&delimiter={}",
            CTX.endpoint(),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_raw_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Marker>{}</Marker>",
                expected_raw_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Delimiter>{}</Delimiter>",
                expected_raw_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv1_echoed_fields_with_encoding_type_url_encode_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let url = format!(
            "{}/{bucket}?encoding-type=url&prefix={}&marker={}&delimiter={}",
            CTX.endpoint(),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE),
            query_encode_value(ECHO_FIELD_VALUE)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_url_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Marker>{}</Marker>",
                expected_url_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Delimiter>{}</Delimiter>",
                expected_url_list_value(ECHO_FIELD_VALUE)
            )),
            "unexpected body: {}",
            response.body
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv1_next_marker_without_encoding_type_escapes_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let next_marker = "aaa\u{0001}<>&\"+";
        let encoded_next_marker = query_encode_value(next_marker);
        put_raw_object(&bucket, &format!("{encoded_next_marker}%2Bobj"));
        client
            .put_object()
            .bucket(&bucket)
            .key("zzz+obj")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{bucket}?delimiter=%2B&max-keys=1", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<IsTruncated>true</IsTruncated>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<NextMarker>{}</NextMarker>",
                expected_raw_list_value(next_marker)
            )),
            "unexpected body: {}",
            response.body
        );

        delete_raw_object(&bucket, &format!("{encoded_next_marker}%2Bobj"));
        let keys = vec!["zzz+obj".to_string()];
        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv1_next_marker_with_encoding_type_url_encodes_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let next_marker = "aaa\u{0001}<>&\"+";
        let encoded_next_marker = query_encode_value(next_marker);
        put_raw_object(&bucket, &format!("{encoded_next_marker}%2Bobj"));
        client
            .put_object()
            .bucket(&bucket)
            .key("zzz+obj")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!(
            "{}/{bucket}?encoding-type=url&delimiter=%2B&max-keys=1",
            CTX.endpoint()
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<IsTruncated>true</IsTruncated>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<NextMarker>{}</NextMarker>",
                expected_url_list_value(next_marker)
            )),
            "unexpected body: {}",
            response.body
        );

        delete_raw_object(&bucket, &format!("{encoded_next_marker}%2Bobj"));
        let keys = vec!["zzz+obj".to_string()];
        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── Return data ─────────────────────────────────────────────────────

#[test]
fn test_bucket_list_return_data() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["bar", "baz"]).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        let objects = resp.contents();
        assert_eq!(objects.len(), 2);

        for obj in objects {
            assert!(obj.key().is_some());
            assert!(obj.e_tag().is_some());
            assert!(obj.size().is_some());
            let size = obj.size().unwrap();
            assert_eq!(size, 7); // "content" is 7 bytes
            assert!(obj.last_modified().is_some());
        }

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

// ── Anonymous access ────────────────────────────────────────────────

fn anon_agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

#[test]
fn test_bucket_list_objects_anonymous_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        // Private bucket (default ACL)
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for anon list on private bucket, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_listv2_objects_anonymous_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        // Private bucket (default ACL)
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for anon listv2 on private bucket, got {}",
            status
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Additional Ceph tests ──────────────────────────────────────────────

#[test]
fn test_bucket_list_long_name() {
    s3_tests::run(async {
        let client = CTX.client();
        // Use a 63-char bucket name (max allowed), unique via unique_bucket() padding
        let base = s3_tests::unique_bucket();
        let bucket = if base.len() >= 63 {
            base[..63].to_string()
        } else {
            format!("{}{}", base, "a".repeat(63 - base.len()))
        };
        assert_eq!(bucket.len(), 63);
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket objects")
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_maxkeys_invalid() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects(client, "", 3).await;

        // max-keys = -1 should be treated as error or ignored
        let result = client
            .list_objects()
            .bucket(&bucket)
            .max_keys(-1)
            .send()
            .await;
        // AWS returns 400 for invalid max-keys
        assert_eq!(err_status(&result), 400);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_special_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) =
            create_objects_with_keys(client, &["_bla/1", "_bla/2", "_bla/3", "_bla/foo"]).await;

        let resp = client
            .list_objects()
            .bucket(&bucket)
            .prefix("_bla/")
            .send()
            .await
            .unwrap();
        let result_keys = get_keys(resp.contents());
        assert_eq!(result_keys.len(), 4);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_delimiter_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        // Paginate V2 with max_keys=1 through delimiter="/" results
        let mut all_keys = Vec::new();
        let mut all_prefixes = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(&bucket)
                .delimiter("/")
                .max_keys(1);
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req
                .send_retrying_operation_aborted("list bucket objects")
                .await
                .unwrap();

            let page_keys = get_keys(resp.contents());
            let page_prefixes = get_prefixes(resp.common_prefixes());
            all_keys.extend(page_keys);
            all_prefixes.extend(page_prefixes);

            if resp.is_truncated() != Some(true) {
                break;
            }
            continuation_token = resp.next_continuation_token().map(String::from);
        }

        assert_eq!(all_keys, vec!["asdf", "cquux", "thud", "zoo"]);
        assert_eq!(all_prefixes, vec!["boo/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_prefix_delimiter_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, SET_A).await;

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .delimiter("/")
            .prefix("boo/")
            .send()
            .await
            .unwrap();

        let result_keys = get_keys(resp.contents());
        let result_prefixes = get_prefixes(resp.common_prefixes());
        assert_eq!(result_keys, vec!["boo/bar"]);
        assert_eq!(result_prefixes, vec!["boo/baz/"]);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_return_data_versioning() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Enable versioning.
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("enable bucket versioning")
            .await
            .unwrap();

        let key_names = ["bar", "baz", "foo"];
        for key in &key_names {
            s3_tests::put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec())
                .await;
        }

        // Gather expected metadata from HeadObject.
        let mut expected: std::collections::HashMap<String, (String, i64, String)> =
            std::collections::HashMap::new();
        for key in &key_names {
            let head = head_object_eventually_after_versioning_enable(client, &bucket, key).await;
            let etag = head.e_tag().unwrap().to_string();
            let size = head.content_length().unwrap();
            let version_id = head.version_id().unwrap().to_string();
            expected.insert(key.to_string(), (etag, size, version_id));
        }

        // List object versions and verify.
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list object versions")
            .await
            .unwrap();
        let versions = resp.versions();

        assert_eq!(versions.len(), 3);
        for v in versions {
            let key = v.key().unwrap();
            let (exp_etag, exp_size, exp_version_id) = expected.get(key).unwrap();
            assert_eq!(v.e_tag().unwrap(), exp_etag);
            assert_eq!(v.size().unwrap(), *exp_size);
            assert_eq!(v.version_id().unwrap(), exp_version_id);
            assert!(v.last_modified().is_some());
            assert_eq!(v.is_latest(), Some(true));
            // Owner must be present with a valid canonical ID (no DisplayName).
            let owner = v.owner().expect("version should have Owner");
            assert_canonical_owner_id(owner.id().unwrap());
            assert_eq!(owner.display_name(), None);
        }

        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_list_objects_v1_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        for key in ["aaa", "zzz"] {
            s3_tests::put_object_retrying_operation_aborted(client, &bucket, key, b"x".to_vec())
                .await;
        }

        // A truncated ListObjects (v1) response: Contents carry Owner, and
        // no NextMarker appears without a delimiter (full-template equality
        // pins its absence).
        let response = raw_bucket("GET", &bucket, Some("max-keys=1"));
        assert_shape(
            "ListObjectsV1 shape",
            &response,
            &shape()
                .status(200)
                .headers(xml_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{bucket}</Name>\
                     <Prefix></Prefix><Marker></Marker><MaxKeys>1</MaxKeys>\
                     <IsTruncated>true</IsTruncated><Contents><Key>aaa</Key>\
                     <LastModified>{iso8601}</LastModified><ETag>{etag}</ETag>\
                     <ChecksumAlgorithm>CRC32</ChecksumAlgorithm>\
                     <ChecksumType>FULL_OBJECT</ChecksumType><Size>1</Size>\
                     <Owner><ID>{owner_id}</ID></Owner>\
                     <StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>",
                )
                .sub("bucket", bucket.as_str()),
        );

        delete_all_and_bucket(client, &bucket, &["aaa".to_string(), "zzz".to_string()]).await;
    });
}

#[test]
fn test_list_objects_v2_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        for key in ["aaa", "zzz"] {
            s3_tests::put_object_retrying_operation_aborted(client, &bucket, key, b"x".to_vec())
                .await;
        }

        // Truncated v2: continuation token is endpoint-opaque, Contents
        // carry no Owner.
        let response = raw_bucket("GET", &bucket, Some("list-type=2&max-keys=1"));
        assert_shape(
            "ListObjectsV2 shape",
            &response,
            &shape()
                .status(200)
                .headers(xml_response_headers())
                .header("x-amz-bucket-region", CTX.region())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{bucket}</Name>\
                     <Prefix></Prefix><NextContinuationToken>{any}</NextContinuationToken>\
                     <KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>true</IsTruncated>\
                     <Contents><Key>aaa</Key><LastModified>{iso8601}</LastModified>\
                     <ETag>{etag}</ETag><ChecksumAlgorithm>CRC32</ChecksumAlgorithm>\
                     <ChecksumType>FULL_OBJECT</ChecksumType><Size>1</Size>\
                     <StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>",
                )
                .sub("bucket", bucket.as_str()),
        );

        delete_all_and_bucket(client, &bucket, &["aaa".to_string(), "zzz".to_string()]).await;
    });
}
