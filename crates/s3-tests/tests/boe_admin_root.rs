use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ObjectAttributes, ObjectOwnership, Tag, Tagging,
    VersioningConfiguration,
};
use s3_tests::{cleanup_versioned_bucket, unique_bucket, SendRetryingOperationAborted, CTX};

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn eventually_err_status<T, E, F, Fut>(
    description: &str,
    expected_status: u16,
    expected_code: Option<&str>,
    mut op: F,
) where
    E: std::fmt::Debug + ProvideErrorMetadata,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        match &result {
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(expected_status)
                    && expected_code.is_none_or(|code| {
                        err.as_service_error().and_then(ProvideErrorMetadata::code) == Some(code)
                    }) =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => {
                let status = result.as_ref().err().and_then(|err| {
                    err.raw_response()
                        .map(|response| response.status().as_u16())
                });
                let code = result
                    .as_ref()
                    .err()
                    .and_then(|err| err.as_service_error().and_then(ProvideErrorMetadata::code));
                panic!(
                    "{description} did not converge to HTTP {expected_status} ({expected_code:?}); got status={status:?} code={code:?}"
                );
            }
        }
    }

    unreachable!()
}

async fn create_versioned_boe_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_ownership(ObjectOwnership::BucketOwnerEnforced)
        .send()
        .await
        .unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    s3_tests::wait_for_versioned_writes_visible(client, &bucket).await;
    bucket
}

fn simple_tagging(value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key("env").value(value).build().unwrap())
        .build()
        .unwrap()
}

async fn put_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap()
}

#[test]
fn test_same_account_root_and_non_root_boe_acl_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_versioned_boe_bucket(client).await;

        let non_root_version = put_object(client, &bucket, "non-root-object", b"one")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        eventually_ok("root GetBucketAcl on BOE bucket", || {
            root_client.get_bucket_acl().bucket(&bucket).send()
        })
        .await;
        eventually_ok("root GetObjectAcl on BOE object", || {
            root_client
                .get_object_acl()
                .bucket(&bucket)
                .key("non-root-object")
                .send()
        })
        .await;
        eventually_ok("root GetObjectAcl on BOE object version", || {
            root_client
                .get_object_acl()
                .bucket(&bucket)
                .key("non-root-object")
                .version_id(&non_root_version)
                .send()
        })
        .await;
        let root_version = put_object(root_client, &bucket, "root-object", b"two")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        eventually_ok("non-root GetBucketAcl on BOE bucket", || {
            client.get_bucket_acl().bucket(&bucket).send()
        })
        .await;
        eventually_ok("non-root GetObjectAcl on BOE object", || {
            client
                .get_object_acl()
                .bucket(&bucket)
                .key("root-object")
                .send()
        })
        .await;
        eventually_ok("non-root GetObjectAcl on BOE object version", || {
            client
                .get_object_acl()
                .bucket(&bucket)
                .key("root-object")
                .version_id(&root_version)
                .send()
        })
        .await;
        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_boe_object_attributes_follow_owner_principal() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_versioned_boe_bucket(client).await;

        let non_root_version = put_object(client, &bucket, "non-root-object", b"one")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        eventually_err_status(
            "root GetObjectAttributes on BOE object",
            403,
            Some("AccessDenied"),
            || {
                root_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key("non-root-object")
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;
        eventually_err_status(
            "root GetObjectAttributes on BOE object version",
            403,
            Some("AccessDenied"),
            || {
                root_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key("non-root-object")
                    .version_id(&non_root_version)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        let root_version = put_object(root_client, &bucket, "root-object", b"two")
            .await
            .version_id()
            .expect("expected version id")
            .to_string();
        let attrs = eventually_ok("non-root GetObjectAttributes on BOE object", || {
            client
                .get_object_attributes()
                .bucket(&bucket)
                .key("root-object")
                .object_attributes(ObjectAttributes::ObjectSize)
                .send()
        })
        .await;
        assert_eq!(attrs.object_size(), Some(3));
        let versioned_attrs =
            eventually_ok("non-root GetObjectAttributes on BOE object version", || {
                client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key("root-object")
                    .version_id(&root_version)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            })
            .await;
        assert_eq!(versioned_attrs.object_size(), Some(3));

        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_boe_object_tagging_admin() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_versioned_boe_bucket(client).await;

        let non_root_version = client
            .put_object()
            .bucket(&bucket)
            .key("non-root-object")
            .tagging("env=owner")
            .body(ByteStream::from_static(b"one"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        let root_view = eventually_ok("root GetObjectTagging on BOE object", || {
            root_client
                .get_object_tagging()
                .bucket(&bucket)
                .key("non-root-object")
                .send()
        })
        .await;
        assert_eq!(root_view.tag_set()[0].value(), "owner");
        let root_view_version =
            eventually_ok("root GetObjectTagging on BOE object version", || {
                root_client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("non-root-object")
                    .version_id(&non_root_version)
                    .send()
            })
            .await;
        assert_eq!(root_view_version.tag_set()[0].value(), "owner");
        eventually_ok("root PutObjectTagging on BOE object", || {
            root_client
                .put_object_tagging()
                .bucket(&bucket)
                .key("non-root-object")
                .tagging(simple_tagging("root"))
                .send_retrying_operation_aborted("root PutObjectTagging on BOE object")
        })
        .await;
        eventually_ok("root DeleteObjectTagging on BOE object version", || {
            root_client
                .delete_object_tagging()
                .bucket(&bucket)
                .key("non-root-object")
                .version_id(&non_root_version)
                .send()
        })
        .await;
        let cleared = eventually_ok(
            "non-root GetObjectTagging after root DeleteObjectTagging on BOE version",
            || {
                client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("non-root-object")
                    .version_id(&non_root_version)
                    .send()
            },
        )
        .await;
        assert!(cleared.tag_set().is_empty());

        let root_version = root_client
            .put_object()
            .bucket(&bucket)
            .key("root-object")
            .tagging("env=root")
            .body(ByteStream::from_static(b"two"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        let non_root_view = eventually_ok("non-root GetObjectTagging on BOE object", || {
            client
                .get_object_tagging()
                .bucket(&bucket)
                .key("root-object")
                .send()
        })
        .await;
        assert_eq!(non_root_view.tag_set()[0].value(), "root");
        let non_root_view_version =
            eventually_ok("non-root GetObjectTagging on BOE object version", || {
                client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("root-object")
                    .version_id(&root_version)
                    .send()
            })
            .await;
        assert_eq!(non_root_view_version.tag_set()[0].value(), "root");
        eventually_ok("non-root PutObjectTagging on BOE object", || {
            client
                .put_object_tagging()
                .bucket(&bucket)
                .key("root-object")
                .tagging(simple_tagging("owner"))
                .send_retrying_operation_aborted("non-root PutObjectTagging on BOE object")
        })
        .await;
        let updated = eventually_ok(
            "root GetObjectTagging after non-root PutObjectTagging",
            || {
                root_client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("root-object")
                    .send()
            },
        )
        .await;
        assert_eq!(updated.tag_set()[0].value(), "owner");
        eventually_ok("non-root DeleteObjectTagging on BOE object version", || {
            client
                .delete_object_tagging()
                .bucket(&bucket)
                .key("root-object")
                .version_id(&root_version)
                .send()
        })
        .await;
        let cleared = eventually_ok(
            "root GetObjectTagging after non-root DeleteObjectTagging on BOE version",
            || {
                root_client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("root-object")
                    .version_id(&root_version)
                    .send()
            },
        )
        .await;
        assert!(cleared.tag_set().is_empty());

        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}

#[test]
fn test_same_account_root_and_non_root_boe_missing_object_discovery() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_versioned_boe_bucket(client).await;

        put_object(client, &bucket, "existing", b"one").await;

        eventually_err_status("root HeadObject missing BOE object", 404, None, || {
            root_client
                .head_object()
                .bucket(&bucket)
                .key("missing")
                .send()
        })
        .await;
        eventually_err_status("root GetObject missing BOE object", 404, None, || {
            root_client
                .get_object()
                .bucket(&bucket)
                .key("missing")
                .send()
        })
        .await;
        eventually_err_status("root GetObjectAcl missing BOE object", 404, None, || {
            root_client
                .get_object_acl()
                .bucket(&bucket)
                .key("missing")
                .send()
        })
        .await;
        eventually_err_status(
            "root GetObjectAttributes missing BOE object",
            403,
            None,
            || {
                root_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key("missing")
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;
        eventually_err_status(
            "root GetObjectTagging missing BOE object",
            404,
            None,
            || {
                root_client
                    .get_object_tagging()
                    .bucket(&bucket)
                    .key("missing")
                    .send()
            },
        )
        .await;

        for (description, admin_client) in [("non-root", client)] {
            eventually_err_status(
                &format!("{description} HeadObject missing BOE object"),
                404,
                None,
                || {
                    admin_client
                        .head_object()
                        .bucket(&bucket)
                        .key("missing")
                        .send()
                },
            )
            .await;
            eventually_err_status(
                &format!("{description} GetObject missing BOE object"),
                404,
                None,
                || {
                    admin_client
                        .get_object()
                        .bucket(&bucket)
                        .key("missing")
                        .send()
                },
            )
            .await;
            eventually_err_status(
                &format!("{description} GetObjectAcl missing BOE object"),
                404,
                None,
                || {
                    admin_client
                        .get_object_acl()
                        .bucket(&bucket)
                        .key("missing")
                        .send()
                },
            )
            .await;
            eventually_err_status(
                &format!("{description} GetObjectAttributes missing BOE object"),
                404,
                None,
                || {
                    admin_client
                        .get_object_attributes()
                        .bucket(&bucket)
                        .key("missing")
                        .object_attributes(ObjectAttributes::ObjectSize)
                        .send()
                },
            )
            .await;
            eventually_err_status(
                &format!("{description} GetObjectTagging missing BOE object"),
                404,
                None,
                || {
                    admin_client
                        .get_object_tagging()
                        .bucket(&bucket)
                        .key("missing")
                        .send()
                },
            )
            .await;
        }

        cleanup_versioned_bucket(root_client, &bucket).await;
    });
}
