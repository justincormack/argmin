//! CopyObject tests that intentionally exercise legacy ACL authorization.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, Grant, Grantee, ObjectOwnership, Owner, Permission, Type,
};
use aws_sdk_s3::Client;
use s3_tests::{create_acl_enabled_bucket, unique_bucket, CTX};

async fn canonical_owner_id(client: &Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
    owner_id
}

fn canonical_user_full_control_grant(canonical_user_id: &str) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .id(canonical_user_id)
                .r#type(Type::CanonicalUser)
                .build()
                .expect("canonical grantee"),
        )
        .permission(Permission::FullControl)
        .build()
}

fn access_control_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

fn has_grant(
    grants: &[Grant],
    permission: Permission,
    canonical_user_id: Option<&str>,
    uri: Option<&str>,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == canonical_user_id && grantee.uri() == uri)
    })
}

#[test]
fn test_object_copy_not_owned_object_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        let bucket_owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected bucket owner ID")
            .to_string();
        let source_owner_id = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected source owner ID")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo123bar")
            .access_control_policy(access_control_policy(
                &source_owner_id,
                vec![
                    canonical_user_full_control_grant(&source_owner_id),
                    canonical_user_full_control_grant(&alt_owner_id),
                ],
            ))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(access_control_policy(
                &bucket_owner_id,
                vec![
                    canonical_user_full_control_grant(&bucket_owner_id),
                    canonical_user_full_control_grant(&alt_owner_id),
                ],
            ))
            .send()
            .await
            .unwrap();

        let src = alt_client
            .get_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let src_body = src.body.collect().await.unwrap().into_bytes();
        assert_eq!(&src_body[..], b"foo");

        alt_client
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let dst = alt_client
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let dst_body = dst.body.collect().await.unwrap().into_bytes();
        assert_eq!(&dst_body[..], b"foo");

        let dst_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            dst_acl.owner().and_then(|owner| owner.id()),
            Some(alt_owner_id.as_str())
        );
        assert!(
            has_grant(
                dst_acl.grants(),
                Permission::FullControl,
                Some(&alt_owner_id),
                None,
            ),
            "expected FULL_CONTROL grant for alternate owner, got {:?}",
            dst_acl.grants()
        );

        let _ = alt_client
            .delete_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await;
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
