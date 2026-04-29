use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
    LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, ObjectOwnership,
};
use s3_tests::{assert_s3_err_code, create_acl_enabled_bucket, err_status, CTX};

async fn cleanup_bucket(bucket: &str) {
    let client = CTX.client();
    let _ = client.delete_bucket_lifecycle().bucket(bucket).send().await;
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn assert_lifecycle_deleted_eventually(bucket: &str) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await;

        if err_status(&result) == 404 {
            assert_s3_err_code(&result, "NoSuchLifecycleConfiguration");
            return;
        }

        if attempt + 1 == MAX_ATTEMPTS {
            panic!("expected deleted lifecycle configuration, got {result:?}");
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[test]
fn test_bucket_lifecycle_acl_crud_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        let config = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("acl-expire-current")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(30).build())
                    .build()
                    .unwrap(),
            )
            .rules(
                LifecycleRule::builder()
                    .id("acl-disabled-abort")
                    .filter(LifecycleRuleFilter::builder().prefix("uploads/").build())
                    .status(ExpirationStatus::Disabled)
                    .abort_incomplete_multipart_upload(
                        AbortIncompleteMultipartUpload::builder()
                            .days_after_initiation(3)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        s3_tests::put_bucket_lifecycle_with_md5(client, &bucket, config)
            .send()
            .await
            .unwrap();

        let get = client
            .get_bucket_lifecycle_configuration()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(get.rules().len(), 2);
        assert_eq!(get.rules()[0].id(), Some("acl-expire-current"));
        assert_eq!(get.rules()[0].status(), &ExpirationStatus::Enabled);
        assert_eq!(
            get.rules()[0]
                .expiration()
                .and_then(LifecycleExpiration::days),
            Some(30)
        );
        assert_eq!(get.rules()[1].id(), Some("acl-disabled-abort"));
        assert_eq!(get.rules()[1].status(), &ExpirationStatus::Disabled);
        assert_eq!(
            get.rules()[1]
                .abort_incomplete_multipart_upload()
                .and_then(AbortIncompleteMultipartUpload::days_after_initiation),
            Some(3)
        );

        client
            .delete_bucket_lifecycle()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_lifecycle_deleted_eventually(&bucket).await;

        cleanup_bucket(&bucket).await;
    });
}
