use s3_tests::{delete_all_and_bucket, CTX};

const SET_B: &[&str] = &["bar", "baz", "cab", "dog", "foo/bar", "foo/baz", "quux"];

fn anon_agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

#[test]
fn test_bucket_list_objects_anonymous() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;
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
        let bucket = s3_tests::create_public_bucket(client).await;
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

        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = anon_agent().get(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("<Key>bar</Key>"),
            "expected 'bar' in v2 listing"
        );

        let keys: Vec<String> = SET_B.iter().map(|s| s.to_string()).collect();
        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}
