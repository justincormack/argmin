//! Multipart upload tests that intentionally exercise legacy ACL authorization.

use std::collections::BTreeMap;
use std::future::Future;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketCannedAcl, CompletedMultipartUpload, CompletedPart, ObjectCannedAcl, ObjectOwnership,
    Permission,
};
use s3_tests::{
    assert_s3_err_code, create_acl_enabled_bucket, create_public_write_bucket, err_status,
    unique_bucket, CTX,
};

const PART_SIZE: usize = 5 * 1024 * 1024;

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .expect("expected owner in GetBucketAcl")
        .id()
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
    owner_id
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }

    for _ in 0..10 {
        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send()
                .await;
        }

        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn complete_single_part_multipart_upload_with_client_and_acl(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    acl: Option<ObjectCannedAcl>,
) -> String {
    let mut create_req = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(acl) = acl {
        create_req = create_req.acl(acl);
    }
    let create = create_req.send().await.unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(part.e_tag().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    upload_id
}

fn has_canonical_grant(
    grants: &[aws_sdk_s3::types::Grant],
    permission: Permission,
    canonical_user_id: &str,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant.grantee().and_then(|grantee| grantee.id()) == Some(canonical_user_id)
    })
}

#[test]
fn test_create_multipart_upload_grant_write_header_persists_write_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        let key = "multipart-grant-write";
        let body = vec![b'x'; 1024];

        let owner_id = canonical_owner_id(client).await;
        let grant_write_owner_id = owner_id.clone();
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-write",
                    format!("id=\"{grant_write_owner_id}\""),
                );
            })
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_canonical_grant(acl.grants(), Permission::Write, &owner_id),
            "expected WRITE grant for object owner, got {:?}",
            acl.grants()
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_completed_multipart_upload_initiator_succeeds_when_owner_is_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-abort-after-complete-initiator-not-owner";

        let bucket_owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .expect("expected bucket owner in GetBucketAcl")
            .id()
            .expect("expected bucket owner ID in GetBucketAcl")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        let upload_id = complete_single_part_multipart_upload_with_client_and_acl(
            alt_client,
            &bucket,
            key,
            b"hello, world!",
            Some(ObjectCannedAcl::BucketOwnerFullControl),
        )
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let object_owner_id = acl
            .owner()
            .expect("expected object owner in GetObjectAcl")
            .id()
            .expect("expected object owner ID in GetObjectAcl")
            .to_string();
        assert_eq!(object_owner_id, bucket_owner_id);
        assert_ne!(object_owner_id, alt_owner_id);

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_completed_multipart_upload_owner_root_admin_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let root_client = owner_root_client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-abort-after-complete-owner-root";

        let upload_id = complete_single_part_multipart_upload_with_client_and_acl(
            alt_client,
            &bucket,
            key,
            b"hello, world!",
            None,
        )
        .await;

        root_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let resp = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_completed_multipart_upload_rejects_unrelated_cross_account_requester() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-abort-after-complete-unrelated-cross-account";

        let upload_id = complete_single_part_multipart_upload_with_client_and_acl(
            client,
            &bucket,
            key,
            b"hello, world!",
            None,
        )
        .await;

        let result = alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_owner_can_manage_cross_account_object_writer_multipart_without_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-cross-account-object-writer-owner-manage";

        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(
                aws_sdk_s3::types::OwnershipControls::builder()
                    .rules(
                        aws_sdk_s3::types::OwnershipControlsRule::builder()
                            .object_ownership(ObjectOwnership::ObjectWriter)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        let create = eventually_ok(
            "CreateMultipartUpload by cross-account writer on object-writer bucket",
            || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            },
        )
        .await;
        let upload_id = create.upload_id().unwrap().to_string();

        eventually_ok(
            "UploadPart by cross-account writer on object-writer bucket",
            || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(vec![b'm'; PART_SIZE]))
                    .send()
            },
        )
        .await;

        let listed = eventually_ok(
            "ListParts by bucket owner on cross-account object-writer upload",
            || {
                client
                    .list_parts()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;
        assert_eq!(listed.parts().len(), 1);
        assert_eq!(listed.parts()[0].part_number(), Some(1));

        eventually_ok(
            "AbortMultipartUpload by bucket owner on cross-account object-writer upload",
            || {
                client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
            },
        )
        .await;

        let result = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_upload_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_public_write_bucket(client).await;
        let owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .expect("expected bucket owner in GetBucketAcl")
            .id()
            .expect("expected bucket owner ID in GetBucketAcl")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        let upload1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart1")
            .send()
            .await
            .unwrap();
        let upload1_id = upload1.upload_id().unwrap().to_string();
        let upload2 = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart2")
            .send()
            .await
            .unwrap();
        let upload2_id = upload2.upload_id().unwrap().to_string();

        let mut views = Vec::new();
        for lister in [client, alt_client] {
            let resp = lister
                .list_multipart_uploads()
                .bucket(&bucket)
                .send()
                .await
                .unwrap();

            let uploads: BTreeMap<_, _> = resp
                .uploads()
                .iter()
                .map(|upload| {
                    let owner = upload.owner().expect("upload should have owner");
                    let initiator = upload.initiator().expect("upload should have initiator");
                    (
                        upload.key().expect("upload should have key").to_string(),
                        (
                            upload
                                .upload_id()
                                .expect("upload should have upload ID")
                                .to_string(),
                            owner.id().expect("owner should have ID").to_string(),
                            initiator
                                .id()
                                .expect("initiator should have ID")
                                .to_string(),
                        ),
                    )
                })
                .collect();

            assert_eq!(uploads.len(), 2);
            views.push(uploads);
        }

        assert_eq!(views[0], views[1]);

        let multipart1 = views[0].get("multipart1").expect("expected multipart1");
        assert_eq!(multipart1.0, upload1_id);
        assert_eq!(multipart1.1, owner_id);
        assert!(!multipart1.2.is_empty());

        let multipart2 = views[0].get("multipart2").expect("expected multipart2");
        assert_eq!(multipart2.0, upload2_id);
        assert_eq!(multipart2.1, alt_owner_id);
        assert!(!multipart2.2.is_empty());

        assert_ne!(
            multipart1.2, multipart2.2,
            "initiator IDs should distinguish the two upload creators"
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("multipart1")
            .upload_id(&upload1_id)
            .send()
            .await
            .unwrap();
        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("multipart2")
            .upload_id(&upload2_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_initiator_cannot_continue_after_bucket_acl_change() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-acl-change";

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        let upload_part = alt_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'x'; 1024]))
            .send()
            .await;
        assert_eq!(err_status(&upload_part), 403);
        assert_s3_err_code(&upload_part, "AccessDenied");

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}
