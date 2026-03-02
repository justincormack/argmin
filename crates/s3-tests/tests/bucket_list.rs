use aws_sdk_s3::types::EncodingType;
use s3_tests::{
    create_objects, create_objects_with_keys, delete_all_and_bucket, err_status, unique_bucket, CTX,
};

// ── Test data sets ──────────────────────────────────────────────────

/// Set A — 6 keys for delimiter tests.
const SET_A: &[&str] = &["asdf", "boo/bar", "boo/baz/xyzzy", "cquux", "thud", "zoo"];

/// Set B — 7 keys for prefix/general tests.
const SET_B: &[&str] = &["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"];

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

// ── Empty / basic ───────────────────────────────────────────────────

#[test]
fn test_bucket_list_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
        assert!(resp.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_list_distinct() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket_a, keys_a) = create_objects_with_keys(client, &["foo", "bar"]).await;
        let bucket_b = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket_b)
            .send()
            .await
            .unwrap();

        let resp = client
            .list_objects()
            .bucket(&bucket_b)
            .send()
            .await
            .unwrap();
        assert!(resp.contents().is_empty());

        client
            .delete_bucket()
            .bucket(&bucket_b)
            .send()
            .await
            .unwrap();
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

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
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
            let resp = req.send().await.unwrap();
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

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
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
            let resp = req.send().await.unwrap();
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

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
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
            let resp = req.send().await.unwrap();

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

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
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

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
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

        // Empty continuation token should behave like no token
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .continuation_token("")
            .send()
            .await
            .unwrap();
        assert_eq!(get_keys(resp.contents()).len(), 7);

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
            assert!(
                obj.owner().is_some(),
                "expected owner for key {:?}",
                obj.key()
            );
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

// ── Return data ─────────────────────────────────────────────────────

#[test]
fn test_bucket_list_return_data() {
    s3_tests::run(async {
        let client = CTX.client();
        let (bucket, keys) = create_objects_with_keys(client, &["bar", "baz"]).await;

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
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

fn anon_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
}

#[test]
fn test_bucket_list_objects_anonymous() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();
        for key in SET_B {
            client
                .put_object()
                .bucket(&bucket)
                .key(*key)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"content"))
                .send()
                .await
                .unwrap();
        }

        // Anonymous ListObjects V1
        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.body_mut().read_to_string().unwrap();
        assert!(body.contains("<Key>bar</Key>"), "expected 'bar' in listing");

        let keys: Vec<String> = SET_B.iter().map(|s| s.to_string()).collect();
        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_listv2_objects_anonymous() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();
        for key in SET_B {
            client
                .put_object()
                .bucket(&bucket)
                .key(*key)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"content"))
                .send()
                .await
                .unwrap();
        }

        // Anonymous ListObjects V2
        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.body_mut().read_to_string().unwrap();
        assert!(body.contains("<Key>bar</Key>"), "expected 'bar' in v2 listing");

        let keys: Vec<String> = SET_B.iter().map(|s| s.to_string()).collect();
        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_bucket_list_objects_anonymous_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        // Private bucket (default ACL)
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for anon list on private bucket, got {}", status);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_listv2_objects_anonymous_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        // Private bucket (default ACL)
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for anon listv2 on private bucket, got {}", status);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Additional Ceph tests ──────────────────────────────────────────────

#[test]
fn test_bucket_list_long_name() {
    s3_tests::run(async {
        let client = CTX.client();
        // Use a 63-char bucket name (max allowed)
        let bucket = "a".repeat(63);
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        let resp = client.list_objects().bucket(&bucket).send().await.unwrap();
        assert!(resp.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        let (bucket, keys) = create_objects_with_keys(
            client,
            &["_bla/1", "_bla/2", "_bla/3", "_bla/foo"],
        )
        .await;

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
            let resp = req.send().await.unwrap();

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
#[ignore = "not implemented: versioning"]
fn test_bucket_list_return_data_versioning() {
    s3_tests::run(async {});
}
