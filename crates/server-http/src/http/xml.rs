/// Hand-formatted XML for S3 responses with lightweight request XML parsing.
use crate::coordinator::{
    BucketAcl, BucketObjectLockConfigurationUpdate, BucketSummary, ChecksumClaim, CompletePart,
    DeleteError, DeletedObject, ListEntry, ListObjectVersionsResult, ListObjectsResult,
    ListPartsResult, ObjectPartsInfo,
};
use crate::error::ServerError;
use checksum::{ChecksumAlgorithm, ChecksumType};
use quick_xml::{escape::unescape, events::Event, Reader};
#[cfg(test)]
use s3_types::VersionId;
use s3_types::{
    AclGrant, AclGrantee, AclGrants, AclPermission, BucketLifecycleConfiguration,
    BucketObjectLockConfig, BucketVersioningState, CanonicalUserId, LegalHoldStatus,
    LifecycleConfigError, ObjectLockDefaultRetention, ObjectLockMode, ObjectRetention,
    RetentionPeriod,
};
use server_core::system_metadata::SystemMetadata;
use storage::{
    BucketEncryptionConfig, BucketObjectOwnership, BucketOwnershipControls,
    EffectiveBucketEncryptionConfig, ManagedEncryptionAlgorithm, ObjectKey,
    PublicAccessBlockConfig,
};

use super::response::format_version_id;

fn ensure_xml_body_size(data: &[u8], max_message_length_bytes: usize) -> Result<(), ServerError> {
    if data.len() > max_message_length_bytes {
        return Err(ServerError::MaxMessageLengthExceeded {
            max_message_length_bytes,
        });
    }
    Ok(())
}

/// Format a POST Object 201 response XML.
#[must_use]
pub fn post_response_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <PostResponse>\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <ETag>{}</ETag>\
         </PostResponse>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
    )
}

/// Format an S3 error response XML.
#[must_use]
pub fn error_xml(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>{}</Code>\
         <Message>{}</Message>\
         <Resource>{}</Resource>\
         <RequestId>{}</RequestId>\
         </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(resource),
        xml_escape(request_id),
    )
}

/// Format an S3 error response XML with a `HostId`.
#[must_use]
pub fn error_xml_with_host_id(
    code: &str,
    message: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>{}</Code>\
         <Message>{}</Message>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 `RequestHeaderSectionTooLarge` error response.
#[must_use]
pub fn request_header_section_too_large_error_xml(
    max_size_allowed: usize,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>RequestHeaderSectionTooLarge</Code>\
         <Message>Your request header section exceeds the maximum allowed size.</Message>\
         <MaxSizeAllowed>{}</MaxSizeAllowed>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        max_size_allowed,
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 `NoSuchBucket` error response.
#[must_use]
pub fn no_such_bucket_error_xml(bucket_name: &str, request_id: &str, host_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>NoSuchBucket</Code>\
         <Message>The specified bucket does not exist</Message>\
         <BucketName>{}</BucketName>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(bucket_name),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 `NoSuchKey` error response.
#[must_use]
pub fn no_such_key_error_xml(key: &str, request_id: &str, host_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>NoSuchKey</Code>\
         <Message>The specified key does not exist.</Message>\
         <Key>{}</Key>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(key),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a NoSuchUpload error response XML.
#[must_use]
pub fn no_such_upload_error_xml(upload_id: &str, request_id: &str, host_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>NoSuchUpload</Code>\
         <Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>\
         <UploadId>{}</UploadId>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(upload_id),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

fn multipart_error_etag(etag: &str) -> &str {
    etag.strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(etag)
}

/// Format a complete-multipart `NoSuchUpload` error response XML.
#[must_use]
pub fn complete_multipart_no_such_upload_error_xml(
    upload_id: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>NoSuchUpload</Code>\
         <Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>\
         <UploadId>{}</UploadId>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(upload_id),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a complete-multipart `MalformedXML` error response XML.
#[must_use]
pub fn complete_multipart_malformed_xml_error_xml(request_id: &str, host_id: &str) -> String {
    format!(
        "<Error>\
         <Code>MalformedXML</Code>\
         <Message>The XML you provided was not well-formed or did not validate against our published schema</Message>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a complete-multipart `InvalidPart` error response XML.
#[must_use]
pub fn complete_multipart_invalid_part_error_xml(
    upload_id: &str,
    part_number: u32,
    etag: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>InvalidPart</Code>\
         <Message>One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.</Message>\
         <UploadId>{}</UploadId>\
         <PartNumber>{}</PartNumber>\
         <ETag>{}</ETag>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(upload_id),
        part_number,
        xml_escape(multipart_error_etag(etag)),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a complete-multipart `InvalidPartOrder` error response XML.
#[must_use]
pub fn complete_multipart_invalid_part_order_error_xml(
    upload_id: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>InvalidPartOrder</Code>\
         <Message>The list of parts was not in ascending order. Parts must be ordered by part number.</Message>\
         <UploadId>{}</UploadId>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(upload_id),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a complete-multipart `EntityTooSmall` error response XML.
#[must_use]
pub fn complete_multipart_entity_too_small_error_xml(
    proposed_size: u64,
    min_size_allowed: u64,
    part_number: u32,
    etag: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>EntityTooSmall</Code>\
         <Message>Your proposed upload is smaller than the minimum allowed size</Message>\
         <ProposedSize>{}</ProposedSize>\
         <MinSizeAllowed>{}</MinSizeAllowed>\
         <PartNumber>{}</PartNumber>\
         <ETag>{}</ETag>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        proposed_size,
        min_size_allowed,
        part_number,
        xml_escape(multipart_error_etag(etag)),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a complete-multipart missing-part-checksum error response.
#[must_use]
pub fn complete_multipart_missing_part_checksum_error_xml(
    algorithm: &str,
    part_number: u32,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>InvalidRequest</Code>\
         <Message>The upload was created using a {} checksum. The complete request must include the checksum for each part. It was missing for part {} in the request.</Message>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape_text(algorithm),
        part_number,
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format a complete-multipart invalid checksum-header error response.
#[must_use]
pub fn complete_multipart_checksum_header_invalid_error_xml(
    header_name: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>InvalidRequest</Code>\
         <Message>Value for {} header is invalid.</Message>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape_text(header_name),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an UploadPartCopy invalid range error response.
#[must_use]
pub fn upload_part_copy_invalid_range_error_xml(
    range_header: &str,
    source_size: u64,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>InvalidArgument</Code>\
         <Message>Range specified is not valid for source object of size: {}</Message>\
         <ArgumentName>x-amz-copy-source-range</ArgumentName>\
         <ArgumentValue>{}</ArgumentValue>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        source_size,
        xml_escape(range_header),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an UploadPartCopy precondition-failed error response.
#[must_use]
pub fn upload_part_copy_precondition_failed_error_xml(
    condition: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<Error>\
         <Code>PreconditionFailed</Code>\
         <Message>At least one of the pre-conditions you specified did not hold</Message>\
         <Condition>{}</Condition>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape_text(condition),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 `NoSuchBucketPolicy` error response.
#[must_use]
pub fn no_such_bucket_policy_error_xml(
    bucket_name: &str,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>NoSuchBucketPolicy</Code>\
         <Message>The bucket policy does not exist</Message>\
         <BucketName>{}</BucketName>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(bucket_name),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 `KeyTooLongError` response.
#[must_use]
pub fn key_too_long_error_xml(size: usize, max_size_allowed: usize, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>KeyTooLongError</Code>\
         <Message>Your key is too long</Message>\
         <Size>{}</Size>\
         <MaxSizeAllowed>{}</MaxSizeAllowed>\
         <RequestId>{}</RequestId>\
         </Error>",
        size,
        max_size_allowed,
        xml_escape(request_id),
    )
}

/// Format an S3 `MetadataTooLarge` error response.
#[must_use]
pub fn metadata_too_large_error_xml(
    size: usize,
    max_size_allowed: usize,
    request_id: &str,
    host_id: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>MetadataTooLarge</Code>\
         <Message>Your metadata headers exceed the maximum allowed metadata size</Message>\
         <Size>{}</Size>\
         <MaxSizeAllowed>{}</MaxSizeAllowed>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        size,
        max_size_allowed,
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 error response XML with an extra `<Region>` element.
#[must_use]
pub fn error_xml_with_region(
    code: &str,
    message: &str,
    request_id: &str,
    host_id: &str,
    region: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>{}</Code>\
         <Message>{}</Message>\
         <Region>{}</Region>\
         <RequestId>{}</RequestId>\
         <HostId>{}</HostId>\
         </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(region),
        xml_escape(request_id),
        xml_escape(host_id),
    )
}

/// Format an S3 error response XML with an extra `<BucketNamespace>` element.
#[must_use]
pub fn error_xml_with_bucket_namespace(
    code: &str,
    message: &str,
    bucket_namespace: &str,
    request_id: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>{}</Code>\
         <Message>{}</Message>\
         <BucketNamespace>{}</BucketNamespace>\
         <RequestId>{}</RequestId>\
         </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(bucket_namespace),
        xml_escape(request_id),
    )
}

/// Format an S3 `NotImplemented` error that identifies the header causing it.
#[must_use]
pub fn header_not_implemented_xml(header: &str, resource: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>NotImplemented</Code>\
         <Message>A header you provided implies functionality that is not implemented</Message>\
         <Header>{}</Header>\
         <Resource>{}</Resource>\
         <RequestId>{}</RequestId>\
         </Error>",
        xml_escape(header),
        xml_escape(resource),
        xml_escape(request_id),
    )
}

/// Format an S3 `NotImplemented` error that identifies the query parameter.
#[must_use]
pub fn query_parameter_not_implemented_xml(
    query_parameter: &str,
    resource: &str,
    request_id: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\
         <Code>NotImplemented</Code>\
         <Message>A query parameter you provided implies functionality that is not implemented</Message>\
         <QueryParameter>{}</QueryParameter>\
         <Resource>{}</Resource>\
         <RequestId>{}</RequestId>\
         </Error>",
        xml_escape(query_parameter),
        xml_escape(resource),
        xml_escape(request_id),
    )
}

/// Format a `ListAllMyBucketsResult` XML response.
#[must_use]
pub fn list_buckets_xml(
    buckets: &[BucketSummary],
    owner_display_name: &str,
    owner_canonical_id: &CanonicalUserId,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>",
    );
    xml.push_str(&xml_escape(owner_canonical_id.as_str()));
    xml.push_str("</ID><DisplayName>");
    xml.push_str(&xml_escape(owner_display_name));
    xml.push_str("</DisplayName></Owner><Buckets>");

    for bucket in buckets {
        xml.push_str("<Bucket><Name>");
        xml.push_str(&xml_escape(bucket.name.as_str()));
        xml.push_str("</Name><CreationDate>");
        xml.push_str(&format_timestamp(bucket.created_at));
        xml.push_str("</CreationDate></Bucket>");
    }

    xml.push_str("</Buckets></ListAllMyBucketsResult>");
    xml
}

/// Format a `GetBucketLocation` XML response.
#[must_use]
pub fn get_bucket_location_xml(location_constraint: Option<&str>) -> String {
    match location_constraint {
        Some(location_constraint) => format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
            xml_escape(location_constraint)
        ),
        None => String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>",
        ),
    }
}

/// Format a `GetBucketAcl` XML response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedAclGrant {
    pub grantee: AclGrantee,
    pub permission: AclPermission,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedCanonicalUser {
    pub canonical_id: CanonicalUserId,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedMultipartUploadEntry {
    pub key: String,
    pub upload_id: String,
    pub initiated: u64,
    pub owner: RenderedCanonicalUser,
    pub initiator: RenderedCanonicalUser,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<checksum::ChecksumType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedListMultipartUploadsResult {
    pub uploads: Vec<RenderedMultipartUploadEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

fn append_canonical_owner_xml(xml: &mut String, element: &str, owner: &RenderedCanonicalUser) {
    xml.push('<');
    xml.push_str(element);
    xml.push_str("><ID>");
    xml.push_str(&xml_escape(owner.canonical_id.as_str()));
    xml.push_str("</ID>");
    if let Some(display_name) = &owner.display_name {
        xml.push_str("<DisplayName>");
        xml.push_str(&xml_escape(display_name));
        xml.push_str("</DisplayName>");
    }
    xml.push_str("</");
    xml.push_str(element);
    xml.push('>');
}

/// Format a `GetBucketAcl`/`GetObjectAcl` XML response.
#[must_use]
pub fn acl_xml(
    _owner_display_name: &str,
    owner_canonical_id: &CanonicalUserId,
    grants: &[RenderedAclGrant],
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>",
    );
    xml.push_str(&xml_escape(owner_canonical_id.as_str()));
    xml.push_str("</ID></Owner><AccessControlList>");
    for grant in grants {
        append_rendered_acl_grant(&mut xml, grant);
    }
    xml.push_str("</AccessControlList></AccessControlPolicy>");
    xml
}

#[must_use]
pub fn bucket_acl_xml(
    owner_display_name: &str,
    owner_canonical_id: &CanonicalUserId,
    acl: BucketAcl,
) -> String {
    let mut grants = vec![RenderedAclGrant {
        grantee: AclGrantee::CanonicalUser(owner_canonical_id.clone()),
        permission: AclPermission::FullControl,
        display_name: Some(owner_display_name.to_string()),
    }];
    match acl {
        BucketAcl::Private => {}
        BucketAcl::PublicRead => grants.push(RenderedAclGrant {
            grantee: AclGrantee::AllUsers,
            permission: AclPermission::Read,
            display_name: None,
        }),
        BucketAcl::PublicReadWrite => {
            grants.push(RenderedAclGrant {
                grantee: AclGrantee::AllUsers,
                permission: AclPermission::Read,
                display_name: None,
            });
            grants.push(RenderedAclGrant {
                grantee: AclGrantee::AllUsers,
                permission: AclPermission::Write,
                display_name: None,
            });
        }
        BucketAcl::AuthenticatedRead => grants.push(RenderedAclGrant {
            grantee: AclGrantee::AuthenticatedUsers,
            permission: AclPermission::Read,
            display_name: None,
        }),
    }
    acl_xml(owner_display_name, owner_canonical_id, &grants)
}

fn append_rendered_acl_grant(xml: &mut String, grant: &RenderedAclGrant) {
    xml.push_str(
        "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"",
    );
    match &grant.grantee {
        AclGrantee::CanonicalUser(id) => {
            xml.push_str("CanonicalUser\"><ID>");
            xml.push_str(&xml_escape(id.as_str()));
            xml.push_str("</ID>");
        }
        AclGrantee::AllUsers | AclGrantee::AuthenticatedUsers => {
            xml.push_str("Group\"><URI>");
            xml.push_str(
                grant
                    .grantee
                    .group_uri()
                    .expect("group grantees always have a URI"),
            );
            xml.push_str("</URI>");
        }
    }
    xml.push_str("</Grantee><Permission>");
    xml.push_str(grant.permission.as_str());
    xml.push_str("</Permission></Grant>");
}

pub fn parse_acl_xml(data: &[u8]) -> Result<AclGrants, ServerError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum TextField {
        Permission,
        GranteeId,
        GranteeUri,
    }

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut grants = Vec::new();
    let mut current_grantee_type: Option<String> = None;
    let mut current_grantee: Option<AclGrantee> = None;
    let mut current_permission: Option<AclPermission> = None;
    let mut current_text_field: Option<TextField> = None;
    let mut in_grantee = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"Grantee" => {
                    in_grantee = true;
                    current_grantee_type = None;
                    current_grantee = None;
                    for attr in e.attributes() {
                        let attr = attr.map_err(|_| ServerError::InvalidArgument {
                            reason: "invalid ACL XML attributes".to_string(),
                        })?;
                        if attr.key.as_ref().ends_with(b"type") {
                            current_grantee_type = Some(
                                attr.decode_and_unescape_value(reader.decoder())
                                    .map_err(|_| ServerError::InvalidArgument {
                                        reason: "invalid ACL grantee type".to_string(),
                                    })?
                                    .into_owned(),
                            );
                        }
                    }
                }
                b"ID" if in_grantee => current_text_field = Some(TextField::GranteeId),
                b"URI" if in_grantee => current_text_field = Some(TextField::GranteeUri),
                b"Permission" => current_text_field = Some(TextField::Permission),
                _ => {}
            },
            Ok(Event::Empty(e)) if e.local_name().as_ref() == b"Grantee" => {
                in_grantee = false;
                current_grantee_type = None;
                current_grantee = None;
                for attr in e.attributes() {
                    let attr = attr.map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid ACL XML attributes".to_string(),
                    })?;
                    if attr.key.as_ref().ends_with(b"type") {
                        current_grantee_type = Some(
                            attr.decode_and_unescape_value(reader.decoder())
                                .map_err(|_| ServerError::InvalidArgument {
                                    reason: "invalid ACL grantee type".to_string(),
                                })?
                                .into_owned(),
                        );
                    }
                }
            }
            Ok(Event::Text(e)) => {
                let Some(field) = current_text_field else {
                    buf.clear();
                    continue;
                };
                let text = decode_xml_text(
                    e.as_ref(),
                    "invalid UTF-8 in ACL XML",
                    "invalid escaped text in ACL XML",
                )?;
                match field {
                    TextField::Permission => {
                        current_permission =
                            Some(AclPermission::parse(text.trim()).ok_or_else(|| {
                                ServerError::InvalidArgument {
                                    reason: format!("unsupported ACL permission: {}", text.trim()),
                                }
                            })?);
                    }
                    TextField::GranteeId => {
                        let grantee_type = current_grantee_type.as_deref().ok_or_else(|| {
                            ServerError::InvalidArgument {
                                reason: "missing ACL grantee type".to_string(),
                            }
                        })?;
                        if grantee_type != "CanonicalUser" {
                            return Err(ServerError::InvalidArgument {
                                reason: format!("unsupported ACL grantee type: {grantee_type}"),
                            });
                        }
                        current_grantee = Some(AclGrantee::CanonicalUser(
                            CanonicalUserId::new(text.trim()).ok_or_else(|| {
                                ServerError::InvalidArgument {
                                    reason: "invalid canonical user ID in ACL XML".to_string(),
                                }
                            })?,
                        ));
                    }
                    TextField::GranteeUri => {
                        let grantee_type = current_grantee_type.as_deref().ok_or_else(|| {
                            ServerError::InvalidArgument {
                                reason: "missing ACL grantee type".to_string(),
                            }
                        })?;
                        if grantee_type != "Group" {
                            return Err(ServerError::InvalidArgument {
                                reason: format!("unsupported ACL grantee type: {grantee_type}"),
                            });
                        }
                        current_grantee =
                            Some(AclGrantee::parse_group_uri(text.trim()).ok_or_else(|| {
                                ServerError::InvalidArgument {
                                    reason: format!("unsupported ACL group URI: {}", text.trim()),
                                }
                            })?);
                    }
                }
            }
            Ok(Event::End(e)) => match e.local_name().as_ref() {
                b"Grantee" => {
                    in_grantee = false;
                    current_text_field = None;
                }
                b"ID" | b"URI" | b"Permission" => {
                    current_text_field = None;
                }
                b"Grant" => {
                    let grantee =
                        current_grantee
                            .take()
                            .ok_or_else(|| ServerError::InvalidArgument {
                                reason: "missing ACL grantee".to_string(),
                            })?;
                    let permission =
                        current_permission
                            .take()
                            .ok_or_else(|| ServerError::InvalidArgument {
                                reason: "missing ACL permission".to_string(),
                            })?;
                    grants.push(AclGrant::new(grantee, permission));
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Ok(Event::Decl(_))
            | Ok(Event::DocType(_))
            | Ok(Event::Comment(_))
            | Ok(Event::CData(_))
            | Ok(Event::PI(_))
            | Ok(Event::Empty(_)) => {}
            Err(_) => {
                return Err(ServerError::InvalidArgument {
                    reason: "malformed ACL XML".to_string(),
                });
            }
        }
        buf.clear();
    }

    Ok(AclGrants::new(grants))
}

/// Format a `ListBucketResult` (`ListObjectsV2`) XML response.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn list_objects_v2_xml(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    encoding_type: Option<&str>,
    continuation_token: Option<&str>,
    start_after: Option<&str>,
    fetch_owner: bool,
    max_keys: u32,
    result: &ListObjectsResult,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    xml.push_str("<Name>");
    xml.push_str(&xml_escape(bucket));
    xml.push_str("</Name>");

    xml.push_str("<Prefix>");
    if let Some(p) = prefix {
        xml.push_str(&xml_escape_list_value(&encode_value(p, encoding_type)));
    }
    xml.push_str("</Prefix>");

    if let Some(d) = delimiter {
        xml.push_str("<Delimiter>");
        xml.push_str(&xml_escape_list_value(&encode_value(d, encoding_type)));
        xml.push_str("</Delimiter>");
    }

    if let Some(e) = encoding_type {
        xml.push_str("<EncodingType>");
        xml.push_str(&xml_escape(e));
        xml.push_str("</EncodingType>");
    }

    if let Some(t) = continuation_token {
        xml.push_str("<ContinuationToken>");
        xml.push_str(&xml_escape(t));
        xml.push_str("</ContinuationToken>");
    }

    if let Some(s) = start_after {
        xml.push_str("<StartAfter>");
        xml.push_str(&xml_escape_list_value(&encode_value(s, encoding_type)));
        xml.push_str("</StartAfter>");
    }

    if let Some(ref token) = result.next_continuation_token {
        xml.push_str("<NextContinuationToken>");
        xml.push_str(&xml_escape(token));
        xml.push_str("</NextContinuationToken>");
    }

    xml.push_str("<KeyCount>");
    xml.push_str(&(result.objects.len() + result.common_prefixes.len()).to_string());
    xml.push_str("</KeyCount>");

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    let owner_id = result.owner_canonical_id.as_str();

    for obj in &result.objects {
        render_list_entry_xml(&mut xml, obj, owner_id, encoding_type, fetch_owner);
    }

    for prefix in &result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        xml.push_str(&xml_escape_list_value(&encode_value(prefix, encoding_type)));
        xml.push_str("</Prefix></CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// Format a `ListBucketResult` (`ListObjects` v1) XML response.
#[must_use]
pub fn list_objects_v1_xml(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    marker: Option<&str>,
    encoding_type: Option<&str>,
    max_keys: u32,
    result: &ListObjectsResult,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    xml.push_str("<Name>");
    xml.push_str(&xml_escape(bucket));
    xml.push_str("</Name>");

    xml.push_str("<Prefix>");
    if let Some(p) = prefix {
        xml.push_str(&xml_escape_list_value(&encode_value(p, encoding_type)));
    }
    xml.push_str("</Prefix>");

    xml.push_str("<Marker>");
    if let Some(m) = marker {
        xml.push_str(&xml_escape_list_value(&encode_value(m, encoding_type)));
    }
    xml.push_str("</Marker>");

    if let Some(d) = delimiter {
        xml.push_str("<Delimiter>");
        xml.push_str(&xml_escape_list_value(&encode_value(d, encoding_type)));
        xml.push_str("</Delimiter>");
    }

    if let Some(e) = encoding_type {
        xml.push_str("<EncodingType>");
        xml.push_str(&xml_escape(e));
        xml.push_str("</EncodingType>");
    }

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    if result.is_truncated && delimiter.is_some() {
        if let Some(ref token) = result.next_continuation_token {
            xml.push_str("<NextMarker>");
            xml.push_str(&xml_escape_list_value(&encode_value(token, encoding_type)));
            xml.push_str("</NextMarker>");
        }
    }

    let owner_id = result.owner_canonical_id.as_str();

    for obj in &result.objects {
        render_list_entry_xml(&mut xml, obj, owner_id, encoding_type, true);
    }

    for prefix in &result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        xml.push_str(&xml_escape_list_value(&encode_value(prefix, encoding_type)));
        xml.push_str("</Prefix></CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// An entry in a `DeleteObjects` request.
#[derive(Debug)]
pub struct DeleteObjectEntry {
    pub key: ObjectKey,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub last_modified_time: Option<String>,
    pub size: Option<String>,
}

/// Parse a `DeleteObjects` XML request body.
///
/// Returns the list of object entries and the quiet flag.
pub fn parse_delete_objects_xml(
    data: &[u8],
) -> Result<(Vec<DeleteObjectEntry>, bool), ServerError> {
    const MAX_DELETE_OBJECTS_XML_BYTES: usize = 2_048_000;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InDelete,
        InObject,
        InEtag,
        InKey,
        InLastModifiedTime,
        InSize,
        InVersionId,
        InQuiet,
        Done,
    }

    fn malformed_delete_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    ensure_xml_body_size(data, MAX_DELETE_OBJECTS_XML_BYTES)?;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut entries = Vec::new();
    let mut quiet = false;
    let mut current_etag: Option<String> = None;
    let mut current_key: Option<String> = None;
    let mut current_last_modified_time: Option<String> = None;
    let mut current_size: Option<String> = None;
    let mut current_version_id: Option<String> = None;
    let mut current_text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"Delete") => state = State::InDelete,
                (State::InDelete, b"Object") => {
                    current_etag = None;
                    current_key = None;
                    current_last_modified_time = None;
                    current_size = None;
                    current_version_id = None;
                    state = State::InObject;
                }
                (State::InDelete, b"Quiet") => {
                    current_text.clear();
                    state = State::InQuiet;
                }
                (State::InObject, b"ETag") => {
                    current_text.clear();
                    state = State::InEtag;
                }
                (State::InObject, b"Key") => {
                    current_text.clear();
                    state = State::InKey;
                }
                (State::InObject, b"LastModifiedTime") => {
                    current_text.clear();
                    state = State::InLastModifiedTime;
                }
                (State::InObject, b"Size") => {
                    current_text.clear();
                    state = State::InSize;
                }
                (State::InObject, b"VersionId") => {
                    current_text.clear();
                    state = State::InVersionId;
                }
                _ => return Err(malformed_delete_xml("unexpected element in delete XML")),
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"Delete") => state = State::Done,
                (State::InDelete, b"Object") => {
                    return Err(malformed_delete_xml("Object missing <Key> element"));
                }
                (State::InDelete, b"Quiet") => {}
                (State::InObject, b"ETag") => {
                    current_etag = Some(String::new());
                }
                (State::InObject, b"Key") => {
                    return Err(malformed_delete_xml("Object missing <Key> element"));
                }
                (State::InObject, b"LastModifiedTime") => {
                    current_last_modified_time = Some(String::new());
                }
                (State::InObject, b"Size") => {
                    current_size = Some(String::new());
                }
                (State::InObject, b"VersionId") => {
                    current_version_id = Some(String::new());
                }
                _ => {
                    return Err(malformed_delete_xml(
                        "unexpected empty element in delete XML",
                    ))
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InDelete, b"Delete") => state = State::Done,
                (State::InObject, b"Object") => {
                    let key = current_key
                        .take()
                        .ok_or_else(|| malformed_delete_xml("Object missing <Key> element"))?;
                    if key.len() > 1024 {
                        return Err(ServerError::KeyTooLongError {
                            size: key.len(),
                            max_size_allowed: 1024,
                        });
                    }
                    let key = ObjectKey::try_from(key).map_err(|error| match error {
                        storage::ObjectKeyError::InvalidLength { length } if length > 1024 => {
                            ServerError::KeyTooLongError {
                                size: length,
                                max_size_allowed: 1024,
                            }
                        }
                        storage::ObjectKeyError::InvalidLength { .. }
                        | storage::ObjectKeyError::ContainsNullByte => {
                            ServerError::InvalidRequest {
                                reason: error.to_string(),
                            }
                        }
                    })?;
                    entries.push(DeleteObjectEntry {
                        etag: current_etag.take(),
                        key,
                        last_modified_time: current_last_modified_time.take(),
                        size: current_size.take(),
                        version_id: current_version_id.take(),
                    });
                    if entries.len() > 1000 {
                        return Err(malformed_delete_xml(
                            "delete objects list too large (max 1000)",
                        ));
                    }
                    state = State::InDelete;
                }
                (State::InEtag, b"ETag") => {
                    current_etag = Some(std::mem::take(&mut current_text));
                    state = State::InObject;
                }
                (State::InKey, b"Key") => {
                    current_key = Some(std::mem::take(&mut current_text));
                    state = State::InObject;
                }
                (State::InLastModifiedTime, b"LastModifiedTime") => {
                    current_last_modified_time = Some(std::mem::take(&mut current_text));
                    state = State::InObject;
                }
                (State::InSize, b"Size") => {
                    current_size = Some(std::mem::take(&mut current_text));
                    state = State::InObject;
                }
                (State::InVersionId, b"VersionId") => {
                    current_version_id = Some(std::mem::take(&mut current_text));
                    state = State::InObject;
                }
                (State::InQuiet, b"Quiet") => {
                    quiet = current_text == "true";
                    current_text.clear();
                    state = State::InDelete;
                }
                _ => {
                    return Err(malformed_delete_xml(
                        "unexpected closing element in delete XML",
                    ))
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_xml_text(
                    t.as_ref(),
                    "invalid UTF-8 in delete XML body",
                    "invalid XML entity in delete XML body",
                )?;
                match state {
                    State::InEtag
                    | State::InKey
                    | State::InLastModifiedTime
                    | State::InSize
                    | State::InVersionId
                    | State::InQuiet => {
                        current_text.push_str(&text);
                    }
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_delete_xml("unexpected text in delete XML")),
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref())
                    .map_err(|_| malformed_delete_xml("invalid UTF-8 in delete XML body"))?;
                match state {
                    State::InEtag
                    | State::InKey
                    | State::InLastModifiedTime
                    | State::InSize
                    | State::InVersionId
                    | State::InQuiet => {
                        current_text.push_str(text);
                    }
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_delete_xml("unexpected CDATA in delete XML")),
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => Ok((entries, quiet)),
                    State::Start => Err(malformed_delete_xml("missing <Delete> element")),
                    State::InObject
                    | State::InEtag
                    | State::InKey
                    | State::InLastModifiedTime
                    | State::InSize
                    | State::InVersionId => Err(malformed_delete_xml("unclosed <Object> element")),
                    _ => Err(malformed_delete_xml("unexpected end of delete XML")),
                };
            }
            Err(_) => return Err(malformed_delete_xml("malformed delete XML")),
        }
        buf.clear();
    }
}

/// Format a `DeleteResult` XML response.
#[must_use]
pub fn delete_objects_result_xml(
    deleted: &[DeletedObject],
    errors: &[DeleteError],
    quiet: bool,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    if !quiet {
        for d in deleted {
            xml.push_str("<Deleted><Key>");
            xml.push_str(&xml_escape(&d.key));
            xml.push_str("</Key>");
            let vid = super::response::format_version_id(d.version_id);
            if d.version_id.is_versioned() {
                xml.push_str("<VersionId>");
                xml.push_str(&xml_escape(&vid));
                xml.push_str("</VersionId>");
            }
            if d.delete_marker {
                xml.push_str("<DeleteMarker>true</DeleteMarker>");
                xml.push_str("<DeleteMarkerVersionId>");
                xml.push_str(&xml_escape(&vid));
                xml.push_str("</DeleteMarkerVersionId>");
            }
            xml.push_str("</Deleted>");
        }
    }

    for e in errors {
        xml.push_str("<Error><Key>");
        xml.push_str(&xml_escape(&e.key));
        xml.push_str("</Key>");
        if let Some(version_id) = e.version_id {
            let vid = super::response::format_version_id(version_id);
            xml.push_str("<VersionId>");
            xml.push_str(&xml_escape(&vid));
            xml.push_str("</VersionId>");
        }
        xml.push_str("<Code>");
        xml.push_str(&xml_escape(&e.code));
        xml.push_str("</Code><Message>");
        xml.push_str(&xml_escape(&e.message));
        xml.push_str("</Message></Error>");
    }

    xml.push_str("</DeleteResult>");
    xml
}

/// Parse a `PutBucketVersioning` XML request body.
///
/// Returns the versioning state as a `BucketVersioningState` enum.
pub fn parse_versioning_config_xml(data: &[u8]) -> Result<BucketVersioningState, ServerError> {
    const MAX_VERSIONING_CONFIGURATION_BYTES: usize = 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InStatus,
        Done,
    }

    fn invalid_versioning_status() -> ServerError {
        ServerError::MalformedXML {
            reason:
                "The XML you provided was not well-formed or did not validate against our published schema"
                    .to_string(),
        }
    }

    ensure_xml_body_size(data, MAX_VERSIONING_CONFIGURATION_BYTES)?;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut status_text: Option<String> = None;
    let mut current_text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"VersioningConfiguration") => state = State::InRoot,
                (State::InRoot, b"Status") => {
                    current_text.clear();
                    state = State::InStatus;
                }
                _ => {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected element in versioning XML".to_string(),
                    });
                }
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"VersioningConfiguration") => state = State::Done,
                (State::InRoot, b"Status") => status_text = Some(String::new()),
                _ => {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected empty element in versioning XML".to_string(),
                    });
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"VersioningConfiguration") => state = State::Done,
                (State::InStatus, b"Status") => {
                    status_text = Some(std::mem::take(&mut current_text));
                    state = State::InRoot;
                }
                _ => {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected closing element in versioning XML".to_string(),
                    });
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_xml_text(
                    t.as_ref(),
                    "invalid UTF-8 in versioning XML body",
                    "invalid XML entity in versioning XML body",
                )?;
                match state {
                    State::InStatus => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(ServerError::MalformedXML {
                            reason: "unexpected text in versioning XML".to_string(),
                        });
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text =
                    std::str::from_utf8(t.as_ref()).map_err(|_| ServerError::MalformedXML {
                        reason: "invalid UTF-8 in versioning XML body".to_string(),
                    })?;
                match state {
                    State::InStatus => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(ServerError::MalformedXML {
                            reason: "unexpected CDATA in versioning XML".to_string(),
                        });
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => match status_text.as_deref() {
                        Some("Enabled") => Ok(BucketVersioningState::Enabled),
                        Some("Suspended") => Ok(BucketVersioningState::Suspended),
                        Some(_) => Err(invalid_versioning_status()),
                        None => Err(ServerError::IllegalVersioningConfiguration {
                            reason: "The Versioning element must be specified".to_string(),
                        }),
                    },
                    State::Start => Err(ServerError::MalformedXML {
                        reason: "missing <VersioningConfiguration> element".to_string(),
                    }),
                    _ => Err(ServerError::MalformedXML {
                        reason: "unexpected end of versioning XML".to_string(),
                    }),
                };
            }
            Err(_) => {
                return Err(ServerError::MalformedXML {
                    reason: "malformed versioning XML".to_string(),
                });
            }
        }
        buf.clear();
    }
}

/// Format a `GetBucketVersioning` XML response.
#[must_use]
pub fn get_bucket_versioning_xml(state: BucketVersioningState) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    match state {
        BucketVersioningState::Enabled => xml.push_str("<Status>Enabled</Status>"),
        BucketVersioningState::Suspended => xml.push_str("<Status>Suspended</Status>"),
        BucketVersioningState::Disabled => {} // empty element per S3 spec
    }

    xml.push_str("</VersioningConfiguration>");
    xml
}

fn invalid_object_lock_configuration_xml() -> ServerError {
    ServerError::MalformedXML {
        reason:
            "The XML you provided was not well-formed or did not validate against our published schema"
                .to_string(),
    }
}

/// Parse a `PutObjectLockConfiguration` XML request body.
pub fn parse_bucket_object_lock_configuration_xml(
    data: &[u8],
) -> Result<BucketObjectLockConfigurationUpdate, ServerError> {
    const MAX_OBJECT_LOCK_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InObjectLockEnabled,
        InRule,
        InDefaultRetention,
        InMode,
        InDays,
        InYears,
        Done,
    }

    fn decode_object_lock_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in object lock configuration XML body",
            "invalid XML entity in object lock configuration XML body",
        )
    }

    ensure_xml_body_size(data, MAX_OBJECT_LOCK_CONFIGURATION_BYTES)?;

    fn parse_mode(text: &str) -> Result<ObjectLockMode, ServerError> {
        match text.trim() {
            "GOVERNANCE" => Ok(ObjectLockMode::Governance),
            "COMPLIANCE" => Ok(ObjectLockMode::Compliance),
            _ => Err(invalid_object_lock_configuration_xml()),
        }
    }

    fn parse_period(raw_value: &str, unit: &str) -> Result<RetentionPeriod, ServerError> {
        let parsed: i64 = raw_value
            .trim()
            .parse()
            .map_err(|_| invalid_object_lock_configuration_xml())?;
        let positive = u32::try_from(parsed).map_err(|_| ServerError::InvalidArgument {
            reason: format!("DefaultRetention {unit} must be greater than zero"),
        })?;
        match unit {
            "Days" => RetentionPeriod::days(positive),
            "Years" => RetentionPeriod::years(positive),
            _ => unreachable!("unexpected retention period unit"),
        }
        .ok_or_else(|| ServerError::InvalidArgument {
            reason: format!("DefaultRetention {unit} must be greater than zero"),
        })
    }

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut object_lock_enabled = None;
    let mut mode_text: Option<String> = None;
    let mut days_text: Option<String> = None;
    let mut years_text: Option<String> = None;
    let mut saw_rule = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.local_name().as_ref()) {
                (State::Start, b"ObjectLockConfiguration") => state = State::InRoot,
                (State::InRoot, b"ObjectLockEnabled") => {
                    current_text.clear();
                    state = State::InObjectLockEnabled;
                }
                (State::InRoot, b"Rule") => {
                    saw_rule = true;
                    state = State::InRule;
                }
                (State::InRule, b"DefaultRetention") => state = State::InDefaultRetention,
                (State::InDefaultRetention, b"Mode") => {
                    current_text.clear();
                    state = State::InMode;
                }
                (State::InDefaultRetention, b"Days") => {
                    current_text.clear();
                    state = State::InDays;
                }
                (State::InDefaultRetention, b"Years") => {
                    current_text.clear();
                    state = State::InYears;
                }
                _ => {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected element in object lock configuration XML".to_string(),
                    });
                }
            },
            Ok(Event::Empty(e)) => match (state, e.local_name().as_ref()) {
                (State::Start, b"ObjectLockConfiguration") => state = State::Done,
                (State::InRoot, b"ObjectLockEnabled") => object_lock_enabled = Some(String::new()),
                (State::InRoot, b"Rule") => {
                    saw_rule = true;
                }
                (State::InRule, b"DefaultRetention") => {}
                (State::InDefaultRetention, b"Mode") => mode_text = Some(String::new()),
                (State::InDefaultRetention, b"Days") => days_text = Some(String::new()),
                (State::InDefaultRetention, b"Years") => years_text = Some(String::new()),
                _ => {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected empty element in object lock configuration XML"
                            .to_string(),
                    });
                }
            },
            Ok(Event::End(e)) => match (state, e.local_name().as_ref()) {
                (State::InRoot, b"ObjectLockConfiguration") => state = State::Done,
                (State::InObjectLockEnabled, b"ObjectLockEnabled") => {
                    object_lock_enabled = Some(std::mem::take(&mut current_text));
                    state = State::InRoot;
                }
                (State::InRule, b"Rule") => state = State::InRoot,
                (State::InDefaultRetention, b"DefaultRetention") => state = State::InRule,
                (State::InMode, b"Mode") => {
                    mode_text = Some(std::mem::take(&mut current_text));
                    state = State::InDefaultRetention;
                }
                (State::InDays, b"Days") => {
                    days_text = Some(std::mem::take(&mut current_text));
                    state = State::InDefaultRetention;
                }
                (State::InYears, b"Years") => {
                    years_text = Some(std::mem::take(&mut current_text));
                    state = State::InDefaultRetention;
                }
                _ => {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected closing element in object lock configuration XML"
                            .to_string(),
                    });
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_object_lock_text(t.as_ref())?;
                match state {
                    State::InObjectLockEnabled | State::InMode | State::InDays | State::InYears => {
                        current_text.push_str(&text);
                    }
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(ServerError::MalformedXML {
                            reason: "unexpected text in object lock configuration XML".to_string(),
                        });
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text =
                    std::str::from_utf8(t.as_ref()).map_err(|_| ServerError::MalformedXML {
                        reason: "invalid UTF-8 in object lock configuration XML body".to_string(),
                    })?;
                match state {
                    State::InObjectLockEnabled | State::InMode | State::InDays | State::InYears => {
                        current_text.push_str(text);
                    }
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(ServerError::MalformedXML {
                            reason: "unexpected CDATA in object lock configuration XML".to_string(),
                        });
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                if state == State::Start {
                    return Err(ServerError::MalformedXML {
                        reason: "missing <ObjectLockConfiguration> element".to_string(),
                    });
                }
                if state != State::Done {
                    return Err(ServerError::MalformedXML {
                        reason: "unexpected end of object lock configuration XML".to_string(),
                    });
                }

                let object_lock_enabled = match object_lock_enabled.as_deref() {
                    Some("Enabled") => Some(true),
                    Some(_) => return Err(invalid_object_lock_configuration_xml()),
                    None => None,
                };

                let default_retention = match (
                    mode_text.as_deref(),
                    days_text.as_deref(),
                    years_text.as_deref(),
                ) {
                    (None, None, None) => None,
                    (Some(_), Some(_), Some(_)) => {
                        return Err(invalid_object_lock_configuration_xml())
                    }
                    (Some(mode), Some(days), None) => Some(ObjectLockDefaultRetention {
                        mode: parse_mode(mode)?,
                        period: parse_period(days, "Days")?,
                    }),
                    (Some(mode), None, Some(years)) => Some(ObjectLockDefaultRetention {
                        mode: parse_mode(mode)?,
                        period: parse_period(years, "Years")?,
                    }),
                    _ => return Err(invalid_object_lock_configuration_xml()),
                };

                if object_lock_enabled.is_none() && !saw_rule {
                    return Err(invalid_object_lock_configuration_xml());
                }

                return Ok(BucketObjectLockConfigurationUpdate {
                    object_lock_enabled,
                    default_retention,
                });
            }
            Err(_) => {
                return Err(ServerError::MalformedXML {
                    reason: "malformed object lock configuration XML".to_string(),
                });
            }
        }
        buf.clear();
    }
}

/// Format a `GetObjectLockConfiguration` XML response.
#[must_use]
pub fn get_bucket_object_lock_configuration_xml(config: BucketObjectLockConfig) -> String {
    debug_assert!(
        config.enabled,
        "S3 only returns object lock XML for enabled buckets"
    );

    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <ObjectLockEnabled>Enabled</ObjectLockEnabled>",
    );

    if let Some(default_retention) = config.default_retention {
        xml.push_str("<Rule><DefaultRetention><Mode>");
        xml.push_str(default_retention.mode.as_str());
        xml.push_str("</Mode>");
        match default_retention.period {
            RetentionPeriod::Days(days) => {
                xml.push_str("<Days>");
                xml.push_str(&days.get().to_string());
                xml.push_str("</Days>");
            }
            RetentionPeriod::Years(years) => {
                xml.push_str("<Years>");
                xml.push_str(&years.get().to_string());
                xml.push_str("</Years>");
            }
        }
        xml.push_str("</DefaultRetention></Rule>");
    }

    xml.push_str("</ObjectLockConfiguration>");
    xml
}

/// Parse a `PutObjectRetention` XML request body.
pub fn parse_object_retention_xml(data: &[u8]) -> Result<ObjectRetention, ServerError> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InMode,
        InRetainUntilDate,
        Done,
    }

    fn decode_retention_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in object retention XML body",
            "invalid XML entity in object retention XML body",
        )
    }

    fn malformed_retention_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut mode_text: Option<String> = None;
    let mut retain_until_text: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.local_name().as_ref()) {
                (State::Start, b"Retention" | b"ObjectLockRetention") => state = State::InRoot,
                (State::InRoot, b"Mode") => {
                    current_text.clear();
                    state = State::InMode;
                }
                (State::InRoot, b"RetainUntilDate") => {
                    current_text.clear();
                    state = State::InRetainUntilDate;
                }
                _ => {
                    return Err(malformed_retention_xml(
                        "unexpected element in object retention XML",
                    ))
                }
            },
            Ok(Event::Empty(_)) => {
                return Err(malformed_retention_xml(
                    "unexpected empty element in object retention XML",
                ));
            }
            Ok(Event::End(e)) => match (state, e.local_name().as_ref()) {
                (State::InRoot, b"Retention" | b"ObjectLockRetention") => state = State::Done,
                (State::InMode, b"Mode") => {
                    mode_text = Some(std::mem::take(&mut current_text));
                    state = State::InRoot;
                }
                (State::InRetainUntilDate, b"RetainUntilDate") => {
                    retain_until_text = Some(std::mem::take(&mut current_text));
                    state = State::InRoot;
                }
                _ => {
                    return Err(malformed_retention_xml(
                        "unexpected closing element in object retention XML",
                    ))
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_retention_text(t.as_ref())?;
                match state {
                    State::InMode | State::InRetainUntilDate => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_retention_xml(
                            "unexpected text in object retention XML",
                        ))
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref()).map_err(|_| {
                    malformed_retention_xml("invalid UTF-8 in object retention XML body")
                })?;
                match state {
                    State::InMode | State::InRetainUntilDate => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_retention_xml(
                            "unexpected CDATA in object retention XML",
                        ))
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                if state == State::Start {
                    return Err(malformed_retention_xml("missing <Retention> element"));
                }
                if state != State::Done {
                    return Err(malformed_retention_xml(
                        "unexpected end of object retention XML",
                    ));
                }
                let mode = match mode_text.as_deref() {
                    Some("GOVERNANCE") => ObjectLockMode::Governance,
                    Some("COMPLIANCE") => ObjectLockMode::Compliance,
                    _ => return Err(malformed_retention_xml("malformed object retention XML")),
                };
                let retain_until = retain_until_text
                    .as_deref()
                    .ok_or_else(|| malformed_retention_xml("malformed object retention XML"))
                    .and_then(parse_object_lock_timestamp_secs)?;
                return Ok(ObjectRetention {
                    mode,
                    retain_until_unix_seconds: retain_until,
                });
            }
            Err(_) => return Err(malformed_retention_xml("malformed object retention XML")),
        }
        buf.clear();
    }
}

/// Format a `GetObjectRetention` XML response.
#[must_use]
pub fn get_object_retention_xml(retention: Option<ObjectRetention>) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Retention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    if let Some(retention) = retention {
        xml.push_str("<Mode>");
        xml.push_str(retention.mode.as_str());
        xml.push_str("</Mode><RetainUntilDate>");
        xml.push_str(&format_object_lock_timestamp(
            retention.retain_until_unix_seconds,
        ));
        xml.push_str("</RetainUntilDate>");
    }
    xml.push_str("</Retention>");
    xml
}

/// Parse a `PutObjectLegalHold` XML request body.
pub fn parse_object_legal_hold_xml(data: &[u8]) -> Result<LegalHoldStatus, ServerError> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InStatus,
        Done,
    }

    fn decode_legal_hold_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in legal hold XML body",
            "invalid XML entity in legal hold XML body",
        )
    }

    fn malformed_legal_hold_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut status_text: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.local_name().as_ref()) {
                (State::Start, b"LegalHold") => state = State::InRoot,
                (State::InRoot, b"Status") => {
                    current_text.clear();
                    state = State::InStatus;
                }
                _ => {
                    return Err(malformed_legal_hold_xml(
                        "unexpected element in legal hold XML",
                    ))
                }
            },
            Ok(Event::Empty(_)) => {
                return Err(malformed_legal_hold_xml(
                    "unexpected empty element in legal hold XML",
                ));
            }
            Ok(Event::End(e)) => match (state, e.local_name().as_ref()) {
                (State::InRoot, b"LegalHold") => state = State::Done,
                (State::InStatus, b"Status") => {
                    status_text = Some(std::mem::take(&mut current_text));
                    state = State::InRoot;
                }
                _ => {
                    return Err(malformed_legal_hold_xml(
                        "unexpected closing element in legal hold XML",
                    ))
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_legal_hold_text(t.as_ref())?;
                match state {
                    State::InStatus => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_legal_hold_xml(
                            "unexpected text in legal hold XML",
                        ))
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref()).map_err(|_| {
                    malformed_legal_hold_xml("invalid UTF-8 in legal hold XML body")
                })?;
                match state {
                    State::InStatus => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_legal_hold_xml(
                            "unexpected CDATA in legal hold XML",
                        ))
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                if state == State::Start {
                    return Err(malformed_legal_hold_xml("missing <LegalHold> element"));
                }
                if state != State::Done {
                    return Err(malformed_legal_hold_xml("unexpected end of legal hold XML"));
                }
                return match status_text.as_deref() {
                    Some("OFF") => Ok(LegalHoldStatus::Off),
                    Some("ON") => Ok(LegalHoldStatus::On),
                    _ => Err(malformed_legal_hold_xml("malformed legal hold XML")),
                };
            }
            Err(_) => return Err(malformed_legal_hold_xml("malformed legal hold XML")),
        }
        buf.clear();
    }
}

/// Format a `GetObjectLegalHold` XML response.
#[must_use]
pub fn get_object_legal_hold_xml(status: Option<LegalHoldStatus>) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    if let Some(status) = status {
        xml.push_str("<Status>");
        xml.push_str(status.as_str());
        xml.push_str("</Status>");
    }
    xml.push_str("</LegalHold>");
    xml
}

/// Parse a `PutBucketEncryption` XML request body.
///
/// This currently supports the SSE-C bucket blocking subset:
/// - default encryption may be omitted or set to SSE-S3 (`AES256`)
/// - `BlockedEncryptionTypes` may contain `SSE-C` or `NONE`
pub fn parse_bucket_encryption_xml(data: &[u8]) -> Result<BucketEncryptionConfig, ServerError> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InRule,
        InApplyDefault,
        InSseAlgorithm,
        InKmsMasterKeyId,
        InBucketKeyEnabled,
        InBlockedTypes,
        InEncryptionType,
        Done,
    }

    fn malformed_bucket_encryption_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    fn decode_bucket_encryption_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in bucket encryption XML body",
            "invalid XML entity in bucket encryption XML body",
        )
    }

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut rule_seen = false;
    let mut apply_default_seen = false;
    let mut sse_algorithm: Option<String> = None;
    let mut bucket_key_enabled = false;
    let mut encryption_types: Vec<String> = Vec::new();
    let mut kms_master_key_seen = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"ServerSideEncryptionConfiguration") => state = State::InRoot,
                (State::InRoot, b"Rule") => {
                    if rule_seen {
                        return Err(malformed_bucket_encryption_xml(
                            "multiple Rule elements are not supported",
                        ));
                    }
                    rule_seen = true;
                    state = State::InRule;
                }
                (State::InRule, b"ApplyServerSideEncryptionByDefault") => {
                    current_text.clear();
                    apply_default_seen = true;
                    state = State::InApplyDefault;
                }
                (State::InRule, b"BucketKeyEnabled") => {
                    current_text.clear();
                    state = State::InBucketKeyEnabled;
                }
                (State::InRule, b"BlockedEncryptionTypes") => {
                    state = State::InBlockedTypes;
                }
                (State::InApplyDefault, b"SSEAlgorithm") => {
                    current_text.clear();
                    state = State::InSseAlgorithm;
                }
                (State::InApplyDefault, b"KMSMasterKeyID") => {
                    current_text.clear();
                    kms_master_key_seen = true;
                    state = State::InKmsMasterKeyId;
                }
                (State::InBlockedTypes, b"EncryptionType") => {
                    current_text.clear();
                    state = State::InEncryptionType;
                }
                _ => {
                    return Err(malformed_bucket_encryption_xml(
                        "unexpected element in bucket encryption XML",
                    ));
                }
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"ServerSideEncryptionConfiguration") => state = State::Done,
                (State::InRoot, b"Rule") => {
                    if rule_seen {
                        return Err(malformed_bucket_encryption_xml(
                            "multiple Rule elements are not supported",
                        ));
                    }
                    rule_seen = true;
                }
                (State::InRule, b"ApplyServerSideEncryptionByDefault") => {
                    apply_default_seen = true;
                }
                (State::InRule, b"BucketKeyEnabled") => {}
                (State::InRule, b"BlockedEncryptionTypes") => {}
                (State::InApplyDefault, b"SSEAlgorithm") => sse_algorithm = Some(String::new()),
                (State::InApplyDefault, b"KMSMasterKeyID") => {
                    kms_master_key_seen = true;
                }
                (State::InBlockedTypes, b"EncryptionType") => {
                    encryption_types.push(String::new());
                }
                _ => {
                    return Err(malformed_bucket_encryption_xml(
                        "unexpected empty element in bucket encryption XML",
                    ));
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"ServerSideEncryptionConfiguration") => state = State::Done,
                (State::InRule, b"Rule") => state = State::InRoot,
                (State::InApplyDefault, b"ApplyServerSideEncryptionByDefault") => {
                    state = State::InRule;
                }
                (State::InSseAlgorithm, b"SSEAlgorithm") => {
                    sse_algorithm = Some(std::mem::take(&mut current_text));
                    state = State::InApplyDefault;
                }
                (State::InKmsMasterKeyId, b"KMSMasterKeyID") => {
                    current_text.clear();
                    state = State::InApplyDefault;
                }
                (State::InBucketKeyEnabled, b"BucketKeyEnabled") => {
                    bucket_key_enabled = current_text.trim().eq_ignore_ascii_case("true");
                    current_text.clear();
                    state = State::InRule;
                }
                (State::InBlockedTypes, b"BlockedEncryptionTypes") => state = State::InRule,
                (State::InEncryptionType, b"EncryptionType") => {
                    encryption_types.push(std::mem::take(&mut current_text));
                    state = State::InBlockedTypes;
                }
                _ => {
                    return Err(malformed_bucket_encryption_xml(
                        "unexpected closing element in bucket encryption XML",
                    ));
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_bucket_encryption_text(t.as_ref())?;
                match state {
                    State::InSseAlgorithm
                    | State::InKmsMasterKeyId
                    | State::InBucketKeyEnabled
                    | State::InEncryptionType => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_bucket_encryption_xml(
                            "unexpected text in bucket encryption XML",
                        ));
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref()).map_err(|_| {
                    malformed_bucket_encryption_xml("invalid UTF-8 in bucket encryption XML body")
                })?;
                match state {
                    State::InSseAlgorithm
                    | State::InKmsMasterKeyId
                    | State::InBucketKeyEnabled
                    | State::InEncryptionType => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_bucket_encryption_xml(
                            "unexpected CDATA in bucket encryption XML",
                        ));
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                if state != State::Done {
                    return Err(match state {
                        State::Start => malformed_bucket_encryption_xml(
                            "missing ServerSideEncryptionConfiguration element",
                        ),
                        _ => malformed_bucket_encryption_xml(
                            "unexpected end of bucket encryption XML",
                        ),
                    });
                }
                break;
            }
            Err(_) => {
                return Err(malformed_bucket_encryption_xml(
                    "malformed bucket encryption XML",
                ));
            }
        }
        buf.clear();
    }

    if bucket_key_enabled {
        return Err(ServerError::NotImplemented {
            feature: "BucketKeyEnabled=true".to_string(),
        });
    }
    if kms_master_key_seen {
        return Err(ServerError::NotImplemented {
            feature: "KMS bucket encryption configuration".to_string(),
        });
    }
    let mut default_encryption = if apply_default_seen {
        match sse_algorithm.as_deref().map(str::trim) {
            Some("AES256") => Some(ManagedEncryptionAlgorithm::Aes256),
            Some(other) => {
                return Err(ServerError::NotImplemented {
                    feature: format!("bucket encryption algorithm {other}"),
                });
            }
            None => {
                return Err(malformed_bucket_encryption_xml(
                    "missing SSEAlgorithm element in ApplyServerSideEncryptionByDefault",
                ));
            }
        }
    } else {
        None
    };

    let encryption_types: Vec<&str> = encryption_types
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect();

    let sse_c_blocked = match encryption_types.as_slice() {
        [] => default_encryption.is_some(),
        ["NONE"] => false,
        ["SSE-C"] => true,
        [other] => {
            return Err(ServerError::InvalidArgument {
                reason: format!("unsupported blocked encryption type: {other}"),
            });
        }
        _ => {
            return Err(ServerError::InvalidArgument {
                reason: "unsupported blocked encryption types configuration".to_string(),
            });
        }
    };

    if matches!(encryption_types.as_slice(), ["NONE"]) && default_encryption.is_none() {
        default_encryption = Some(ManagedEncryptionAlgorithm::Aes256);
    }

    Ok(BucketEncryptionConfig {
        default_encryption,
        sse_c_blocked,
    })
}

/// Format a `GetBucketEncryption` XML response.
#[must_use]
pub fn get_bucket_encryption_xml(config: EffectiveBucketEncryptionConfig) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule>",
    );
    xml.push_str("<BucketKeyEnabled>false</BucketKeyEnabled>");
    xml.push_str("<ApplyServerSideEncryptionByDefault><SSEAlgorithm>");
    xml.push_str(config.default_encryption.as_str());
    xml.push_str("</SSEAlgorithm></ApplyServerSideEncryptionByDefault>");
    if config.sse_c_blocked {
        xml.push_str(
            "<BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes>",
        );
    }
    xml.push_str("</Rule></ServerSideEncryptionConfiguration>");
    xml
}

pub fn parse_bucket_lifecycle_configuration_xml(
    data: &[u8],
) -> Result<BucketLifecycleConfiguration, ServerError> {
    const MAX_LIFECYCLE_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;

    ensure_xml_body_size(data, MAX_LIFECYCLE_CONFIGURATION_BYTES)?;
    s3_types::parse_lifecycle_configuration_xml(data).map_err(|error| match error {
        LifecycleConfigError::MalformedXml { reason } => ServerError::MalformedXML { reason },
        LifecycleConfigError::InvalidRequest { reason } => ServerError::InvalidRequest { reason },
        LifecycleConfigError::InvalidArgument { reason } => ServerError::InvalidArgument { reason },
        LifecycleConfigError::NotImplemented { feature } => ServerError::NotImplemented { feature },
    })
}

#[must_use]
pub fn get_bucket_lifecycle_configuration_xml(config: &BucketLifecycleConfiguration) -> String {
    s3_types::render_lifecycle_configuration_xml(config)
}

/// Format a `ListVersionsResult` XML response.
#[must_use]
pub fn list_object_versions_xml(
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
    encoding_type: Option<&str>,
    max_keys: u32,
    result: &ListObjectVersionsResult,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    xml.push_str("<Name>");
    xml.push_str(&xml_escape(bucket));
    xml.push_str("</Name>");

    xml.push_str("<Prefix>");
    if let Some(p) = prefix {
        xml.push_str(&xml_escape_list_value(&encode_list_versions_value(
            p,
            encoding_type,
        )));
    }
    xml.push_str("</Prefix>");

    xml.push_str("<KeyMarker>");
    if let Some(km) = key_marker {
        xml.push_str(&xml_escape_list_value(&encode_list_versions_value(
            km,
            encoding_type,
        )));
    }
    xml.push_str("</KeyMarker>");

    xml.push_str("<VersionIdMarker></VersionIdMarker>");

    if let Some(e) = encoding_type {
        xml.push_str("<EncodingType>");
        xml.push_str(&xml_escape(e));
        xml.push_str("</EncodingType>");
    }

    if let Some(ref nkm) = result.next_key_marker {
        xml.push_str("<NextKeyMarker>");
        xml.push_str(&xml_escape_list_value(&encode_list_versions_value(
            nkm,
            encoding_type,
        )));
        xml.push_str("</NextKeyMarker>");
    }

    if let Some(nvm) = result.next_version_id_marker {
        xml.push_str("<NextVersionIdMarker>");
        xml.push_str(&format_version_id(nvm));
        xml.push_str("</NextVersionIdMarker>");
    }

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    let owner_id = result.owner_canonical_id.as_str();

    for entry in &result.versions {
        let vid = format_version_id(entry.version_id);
        let is_latest = if entry.is_latest { "true" } else { "false" };

        if entry.is_delete_marker {
            xml.push_str("<DeleteMarker>");
            xml.push_str("<Key>");
            xml.push_str(&xml_escape_list_value(&encode_list_versions_value(
                &entry.key,
                encoding_type,
            )));
            xml.push_str("</Key>");
            xml.push_str("<VersionId>");
            xml.push_str(&vid);
            xml.push_str("</VersionId>");
            xml.push_str("<IsLatest>");
            xml.push_str(is_latest);
            xml.push_str("</IsLatest>");
            xml.push_str("<LastModified>");
            xml.push_str(&format_timestamp(entry.last_modified));
            xml.push_str("</LastModified>");
            xml.push_str("<Owner><ID>");
            xml.push_str(&xml_escape(owner_id));
            xml.push_str("</ID></Owner>");
            xml.push_str("</DeleteMarker>");
        } else {
            xml.push_str("<Version>");
            xml.push_str("<Key>");
            xml.push_str(&xml_escape_list_value(&encode_list_versions_value(
                &entry.key,
                encoding_type,
            )));
            xml.push_str("</Key>");
            xml.push_str("<VersionId>");
            xml.push_str(&vid);
            xml.push_str("</VersionId>");
            xml.push_str("<IsLatest>");
            xml.push_str(is_latest);
            xml.push_str("</IsLatest>");
            xml.push_str("<LastModified>");
            xml.push_str(&format_timestamp(entry.last_modified));
            xml.push_str("</LastModified>");
            xml.push_str("<ETag>");
            xml.push_str(&xml_escape(&entry.etag));
            xml.push_str("</ETag>");
            if let Some(checksum_algorithm) = entry.checksum_algorithm {
                xml.push_str("<ChecksumAlgorithm>");
                xml.push_str(checksum_algorithm.as_str());
                xml.push_str("</ChecksumAlgorithm>");
            }
            if let Some(checksum_type) = entry.checksum_type {
                xml.push_str("<ChecksumType>");
                xml.push_str(checksum_type.as_str());
                xml.push_str("</ChecksumType>");
            }
            xml.push_str("<Size>");
            xml.push_str(&entry.size.to_string());
            xml.push_str("</Size>");
            xml.push_str("<Owner><ID>");
            xml.push_str(&xml_escape(owner_id));
            xml.push_str("</ID></Owner>");
            xml.push_str("<StorageClass>STANDARD</StorageClass>");
            xml.push_str("</Version>");
        }
    }

    xml.push_str("</ListVersionsResult>");
    xml
}

#[allow(clippy::format_push_string)]
fn encode_url_listing_value(value: &str) -> String {
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

fn encode_value(value: &str, encoding_type: Option<&str>) -> String {
    match encoding_type {
        Some("url") => encode_url_listing_value(value),
        _ => value.to_string(),
    }
}

fn encode_list_versions_value(value: &str, encoding_type: Option<&str>) -> String {
    match encoding_type {
        Some("url") => encode_url_listing_value(value),
        _ => value.to_string(),
    }
}

#[allow(clippy::format_push_string)]
fn xml_escape_list_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\u{0001}'..='\u{0008}' | '\u{000B}' | '\u{000C}' | '\u{000E}'..='\u{001F}' => {
                out.push_str(&format!("&#x{:x};", c as u32));
            }
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

fn render_list_owner_xml(xml: &mut String, owner_id: &str) {
    xml.push_str("<Owner><ID>");
    xml.push_str(&xml_escape(owner_id));
    xml.push_str("</ID></Owner>");
}

fn render_list_entry_xml(
    xml: &mut String,
    entry: &ListEntry,
    owner_id: &str,
    encoding_type: Option<&str>,
    include_owner: bool,
) {
    xml.push_str("<Contents>");
    xml.push_str("<Key>");
    xml.push_str(&xml_escape_list_value(&encode_value(
        &entry.key,
        encoding_type,
    )));
    xml.push_str("</Key>");
    xml.push_str("<LastModified>");
    xml.push_str(&format_timestamp(entry.last_modified));
    xml.push_str("</LastModified>");
    xml.push_str("<ETag>");
    xml.push_str(&xml_escape(&entry.etag));
    xml.push_str("</ETag>");
    if let Some(checksum_algorithm) = entry.checksum_algorithm {
        xml.push_str("<ChecksumAlgorithm>");
        xml.push_str(checksum_algorithm.as_str());
        xml.push_str("</ChecksumAlgorithm>");
    }
    if let Some(checksum_type) = entry.checksum_type {
        xml.push_str("<ChecksumType>");
        xml.push_str(checksum_type.as_str());
        xml.push_str("</ChecksumType>");
    }
    xml.push_str("<Size>");
    xml.push_str(&entry.size.to_string());
    xml.push_str("</Size>");
    if include_owner {
        render_list_owner_xml(xml, owner_id);
    }
    xml.push_str("<StorageClass>STANDARD</StorageClass>");
    xml.push_str("</Contents>");
}

/// Escape special XML characters.
#[must_use]
pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

pub fn xml_escape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(c),
        }
    }
    out
}

/// Parse a CORS configuration XML body.
///
/// Expected format:
/// ```xml
/// <CORSConfiguration>
///   <CORSRule>
///     <AllowedOrigin>...</AllowedOrigin>
///     <AllowedMethod>GET</AllowedMethod>
///     <AllowedHeader>...</AllowedHeader>
///     <ExposeHeader>...</ExposeHeader>
///     <MaxAgeSeconds>3600</MaxAgeSeconds>
///   </CORSRule>
/// </CORSConfiguration>
/// ```
pub fn parse_cors_config_xml(data: &[u8]) -> Result<crate::cors::CorsConfiguration, ServerError> {
    const MAX_CORS_CONFIGURATION_BYTES: usize = 64 * 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InRule,
        InAllowedOrigin,
        InAllowedMethod,
        InAllowedHeader,
        InExposeHeader,
        InMaxAgeSeconds,
        Done,
    }

    fn malformed_cors_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    fn decode_cors_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in CORS XML body",
            "invalid XML entity in CORS XML body",
        )
    }

    if data.len() > MAX_CORS_CONFIGURATION_BYTES {
        return Err(ServerError::MaxMessageLengthExceeded {
            max_message_length_bytes: MAX_CORS_CONFIGURATION_BYTES,
        });
    }

    let valid_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"];
    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut rules = Vec::new();
    let mut current_rule: Option<crate::cors::CorsRule> = None;
    let mut current_text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"CORSConfiguration") => state = State::InRoot,
                (State::InRoot, b"CORSRule") => {
                    current_rule = Some(crate::cors::CorsRule {
                        allowed_origins: Vec::new(),
                        allowed_methods: Vec::new(),
                        allowed_headers: Vec::new(),
                        expose_headers: Vec::new(),
                        max_age_seconds: None,
                    });
                    state = State::InRule;
                }
                (State::InRule, b"AllowedOrigin") => {
                    current_text.clear();
                    state = State::InAllowedOrigin;
                }
                (State::InRule, b"AllowedMethod") => {
                    current_text.clear();
                    state = State::InAllowedMethod;
                }
                (State::InRule, b"AllowedHeader") => {
                    current_text.clear();
                    state = State::InAllowedHeader;
                }
                (State::InRule, b"ExposeHeader") => {
                    current_text.clear();
                    state = State::InExposeHeader;
                }
                (State::InRule, b"MaxAgeSeconds") => {
                    current_text.clear();
                    state = State::InMaxAgeSeconds;
                }
                _ => return Err(malformed_cors_xml("unexpected element in CORS XML")),
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"CORSConfiguration") => state = State::Done,
                (State::InRoot, b"CORSRule") => {}
                (
                    State::InRule,
                    b"AllowedOrigin" | b"AllowedMethod" | b"AllowedHeader" | b"ExposeHeader"
                    | b"MaxAgeSeconds",
                ) => {}
                _ => return Err(malformed_cors_xml("unexpected empty element in CORS XML")),
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"CORSConfiguration") => state = State::Done,
                (State::InRule, b"CORSRule") => {
                    let rule = current_rule
                        .take()
                        .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?;
                    if rule.allowed_origins.is_empty() {
                        return Err(malformed_cors_xml(
                            "CORSRule missing <AllowedOrigin> element",
                        ));
                    }
                    if rule.allowed_methods.is_empty() {
                        return Err(malformed_cors_xml(
                            "CORSRule missing <AllowedMethod> element",
                        ));
                    }
                    rules.push(rule);
                    state = State::InRoot;
                }
                (State::InAllowedOrigin, b"AllowedOrigin") => {
                    current_rule
                        .as_mut()
                        .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?
                        .allowed_origins
                        .push(std::mem::take(&mut current_text));
                    state = State::InRule;
                }
                (State::InAllowedMethod, b"AllowedMethod") => {
                    let method = std::mem::take(&mut current_text);
                    if !valid_methods.contains(&method.as_str()) {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "Found unsupported HTTP method in CORS config. Unsupported method is {method}"
                            ),
                        });
                    }
                    current_rule
                        .as_mut()
                        .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?
                        .allowed_methods
                        .push(method);
                    state = State::InRule;
                }
                (State::InAllowedHeader, b"AllowedHeader") => {
                    current_rule
                        .as_mut()
                        .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?
                        .allowed_headers
                        .push(std::mem::take(&mut current_text));
                    state = State::InRule;
                }
                (State::InExposeHeader, b"ExposeHeader") => {
                    current_rule
                        .as_mut()
                        .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?
                        .expose_headers
                        .push(std::mem::take(&mut current_text));
                    state = State::InRule;
                }
                (State::InMaxAgeSeconds, b"MaxAgeSeconds") => {
                    if current_rule
                        .as_ref()
                        .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?
                        .max_age_seconds
                        .is_none()
                    {
                        let max_age = current_text.parse::<u32>().map_err(|_| {
                            malformed_cors_xml(&format!("invalid MaxAgeSeconds: {current_text}"))
                        })?;
                        current_rule
                            .as_mut()
                            .ok_or_else(|| malformed_cors_xml("missing active CORS rule"))?
                            .max_age_seconds = Some(max_age);
                    }
                    current_text.clear();
                    state = State::InRule;
                }
                _ => return Err(malformed_cors_xml("unexpected closing element in CORS XML")),
            },
            Ok(Event::Text(t)) => {
                let text = decode_cors_text(t.as_ref())?;
                match state {
                    State::InAllowedOrigin
                    | State::InAllowedMethod
                    | State::InAllowedHeader
                    | State::InExposeHeader
                    | State::InMaxAgeSeconds => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_cors_xml("unexpected text in CORS XML")),
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref())
                    .map_err(|_| malformed_cors_xml("invalid UTF-8 in CORS XML body"))?;
                match state {
                    State::InAllowedOrigin
                    | State::InAllowedMethod
                    | State::InAllowedHeader
                    | State::InExposeHeader
                    | State::InMaxAgeSeconds => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_cors_xml("unexpected CDATA in CORS XML")),
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => {
                        if rules.is_empty() {
                            Err(malformed_cors_xml(
                                "CORS configuration must contain at least one rule",
                            ))
                        } else if rules.len() > 100 {
                            Err(ServerError::InvalidRequest {
                                reason:
                                    "The number of CORS rules should not exceed allowed limit of 100 rules."
                                        .to_string(),
                            })
                        } else {
                            Ok(crate::cors::CorsConfiguration { rules })
                        }
                    }
                    State::Start => Err(malformed_cors_xml("missing <CORSConfiguration> element")),
                    _ => Err(malformed_cors_xml("unexpected end of CORS XML")),
                };
            }
            Err(_) => return Err(malformed_cors_xml("malformed CORS XML")),
        }
        buf.clear();
    }
}

/// Serialize a CORS configuration to XML.
#[must_use]
pub fn get_cors_config_xml(config: &crate::cors::CorsConfiguration) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    for rule in &config.rules {
        xml.push_str("<CORSRule>");

        for origin in &rule.allowed_origins {
            xml.push_str("<AllowedOrigin>");
            xml.push_str(&xml_escape(origin));
            xml.push_str("</AllowedOrigin>");
        }

        for method in &rule.allowed_methods {
            xml.push_str("<AllowedMethod>");
            xml.push_str(&xml_escape(method));
            xml.push_str("</AllowedMethod>");
        }

        if let Some(max_age) = rule.max_age_seconds {
            xml.push_str("<MaxAgeSeconds>");
            xml.push_str(&max_age.to_string());
            xml.push_str("</MaxAgeSeconds>");
        }

        for header in &rule.expose_headers {
            xml.push_str("<ExposeHeader>");
            xml.push_str(&xml_escape(header));
            xml.push_str("</ExposeHeader>");
        }

        for header in &rule.allowed_headers {
            xml.push_str("<AllowedHeader>");
            xml.push_str(&xml_escape(header));
            xml.push_str("</AllowedHeader>");
        }

        xml.push_str("</CORSRule>");
    }

    xml.push_str("</CORSConfiguration>");
    xml
}

fn malformed_tagging_xml(reason: &str) -> ServerError {
    ServerError::MalformedXML {
        reason: reason.to_string(),
    }
}

fn decode_xml_text(
    bytes: &[u8],
    invalid_utf8_reason: &str,
    invalid_entity_reason: &str,
) -> Result<String, ServerError> {
    let raw = std::str::from_utf8(bytes).map_err(|_| ServerError::MalformedXML {
        reason: invalid_utf8_reason.to_string(),
    })?;
    unescape(raw)
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| ServerError::MalformedXML {
            reason: invalid_entity_reason.to_string(),
        })
}

fn decode_tagging_text(bytes: &[u8]) -> Result<String, ServerError> {
    decode_xml_text(
        bytes,
        "invalid UTF-8 in tagging XML body",
        "invalid XML entity in tagging XML body",
    )
}

fn validate_tag_set(tags: &[(String, String)], max_tags: usize) -> Result<(), ServerError> {
    let mut seen_keys = std::collections::HashSet::new();
    for (key, value) in tags {
        let key_chars = key.chars().count();
        if key_chars == 0 || key_chars > 128 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag key must be 1-128 characters, got {key_chars}"),
            });
        }
        if key.starts_with("aws:") {
            return Err(ServerError::InvalidTag {
                reason: "tag key must not start with 'aws:'".to_string(),
            });
        }
        let value_chars = value.chars().count();
        if value_chars > 256 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag value must be 0-256 characters, got {value_chars}"),
            });
        }
        if !seen_keys.insert(key.clone()) {
            return Err(ServerError::InvalidTag {
                reason: format!("duplicate tag key: {key}"),
            });
        }
    }
    if tags.len() > max_tags {
        return Err(ServerError::InvalidTag {
            reason: format!(
                "tags cannot be greater than {}, got {}",
                max_tags,
                tags.len()
            ),
        });
    }
    Ok(())
}

fn parse_tag_collection_xml(
    data: &[u8],
    max_tags: usize,
    root_name: &[u8],
    collection_name: &[u8],
    missing_root_reason: &str,
    missing_collection_reason: &str,
) -> Result<Vec<(String, String)>, ServerError> {
    const MAX_TAGGING_XML_BYTES: usize = 160 * 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InCollection,
        InTag,
        InKey,
        InValue,
        Done,
    }

    ensure_xml_body_size(data, MAX_TAGGING_XML_BYTES)?;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut tags = Vec::new();
    let mut current_key: Option<String> = None;
    let mut current_value: Option<String> = None;
    let mut current_text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, name) if name == root_name => state = State::InRoot,
                (State::InRoot, name) if name == collection_name => state = State::InCollection,
                (State::InCollection, b"Tag") => {
                    current_key = None;
                    current_value = None;
                    state = State::InTag;
                }
                (State::InTag, b"Key") => {
                    current_text.clear();
                    state = State::InKey;
                }
                (State::InTag, b"Value") => {
                    current_text.clear();
                    state = State::InValue;
                }
                _ => return Err(malformed_tagging_xml("unexpected element in tagging XML")),
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, name) if name == collection_name => {}
                (State::InCollection, b"Tag") => {
                    return Err(ServerError::InvalidTag {
                        reason: "missing <Key> element in <Tag>".to_string(),
                    });
                }
                (State::InTag, b"Key") => {
                    current_key = Some(String::new());
                }
                (State::InTag, b"Value") => {
                    current_value = Some(String::new());
                }
                _ => {
                    return Err(malformed_tagging_xml(
                        "unexpected empty element in tagging XML",
                    ));
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, name) if name == root_name => state = State::Done,
                (State::InCollection, name) if name == collection_name => state = State::InRoot,
                (State::InTag, b"Tag") => {
                    let key = current_key.take().ok_or(ServerError::InvalidTag {
                        reason: "missing <Key> element in <Tag>".to_string(),
                    })?;
                    let value = current_value.take().unwrap_or_default();
                    tags.push((key, value));
                    validate_tag_set(&tags, max_tags)?;
                    state = State::InCollection;
                }
                (State::InKey, b"Key") => {
                    current_key = Some(std::mem::take(&mut current_text));
                    state = State::InTag;
                }
                (State::InValue, b"Value") => {
                    current_value = Some(std::mem::take(&mut current_text));
                    state = State::InTag;
                }
                _ => {
                    return Err(malformed_tagging_xml(
                        "unexpected closing element in tagging XML",
                    ));
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_tagging_text(t.as_ref())?;
                match state {
                    State::InKey | State::InValue => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_tagging_xml("unexpected text in tagging XML")),
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref())
                    .map_err(|_| malformed_tagging_xml("invalid UTF-8 in tagging XML body"))?;
                match state {
                    State::InKey | State::InValue => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_tagging_xml("unexpected CDATA in tagging XML")),
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => Ok(tags),
                    State::Start => Err(malformed_tagging_xml(missing_root_reason)),
                    State::InRoot => Err(malformed_tagging_xml(missing_collection_reason)),
                    _ => Err(malformed_tagging_xml("unexpected end of tagging XML")),
                };
            }
            Err(_) => return Err(malformed_tagging_xml("malformed tagging XML")),
        }
        buf.clear();
    }
}

/// Format a `CopyObjectResult` XML response.
#[must_use]
pub fn copy_object_result_xml(
    etag: &str,
    last_modified: u64,
    system_metadata: &SystemMetadata,
) -> String {
    let mut checksum_xml = String::new();
    for (header, value) in system_metadata.checksum_header_pairs() {
        let Some(tag) = checksum_header_to_xml_tag(header) else {
            continue;
        };
        checksum_xml.push('<');
        checksum_xml.push_str(tag);
        checksum_xml.push('>');
        checksum_xml.push_str(&xml_escape(value));
        checksum_xml.push_str("</");
        checksum_xml.push_str(tag);
        checksum_xml.push('>');
    }
    if let Some(checksum_type) = system_metadata.checksum_type() {
        checksum_xml.push_str("<ChecksumType>");
        checksum_xml.push_str(checksum_type.as_str());
        checksum_xml.push_str("</ChecksumType>");
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <LastModified>{}</LastModified>\
         <ETag>{}</ETag>\
         {}\
         </CopyObjectResult>",
        format_timestamp(last_modified),
        xml_escape(etag),
        checksum_xml,
    )
}

/// Format a `CopyPartResult` XML response.
#[must_use]
pub fn copy_part_result_xml(etag: &str, last_modified: u64) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyPartResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <LastModified>{}</LastModified>\
         <ETag>{}</ETag>\
         </CopyPartResult>",
        format_timestamp(last_modified),
        etag,
    )
}

/// Format a unix millisecond timestamp as ISO 8601.
pub(crate) fn format_timestamp(millis: u64) -> String {
    let secs = millis / 1000;
    // Simple UTC formatting without pulling in chrono
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Convert days since epoch to date (civil calendar)
    let (year, month, day) = days_to_date(days_since_epoch as i64);

    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.000Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: i64) -> (i64, u32, u32) {
    // Algorithm from Howard Hinnant's date algorithms
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

pub(crate) fn format_object_lock_timestamp(unix_seconds: u64) -> String {
    let days_since_epoch = unix_seconds / 86400;
    let time_of_day = unix_seconds % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_date(days_since_epoch as i64);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.000Z")
}

fn parse_object_lock_timestamp_secs_with<F>(raw: &str, invalid: F) -> Result<u64, ServerError>
where
    F: Fn() -> ServerError,
{
    auth::canonical::parse_iso8601_utc_seconds_with_options(
        raw,
        auth::canonical::Iso8601UtcOptions {
            trim_whitespace: true,
            require_fixed_width_fields: false,
        },
    )
    .ok_or_else(invalid)
}

fn parse_object_lock_timestamp_secs(raw: &str) -> Result<u64, ServerError> {
    parse_object_lock_timestamp_secs_with(raw, || ServerError::MalformedXML {
        reason: "malformed object retention XML".to_string(),
    })
}

pub(crate) fn parse_object_lock_header_timestamp_secs(raw: &str) -> Result<u64, ServerError> {
    parse_object_lock_timestamp_secs_with(raw, || ServerError::InvalidArgument {
        reason: format!("invalid x-amz-object-lock-retain-until-date value: {raw}"),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagSet {
    tags: Vec<(String, String)>,
    max_tags: usize,
}

impl TagSet {
    pub fn new(tags: Vec<(String, String)>, max_tags: usize) -> Result<Self, ServerError> {
        validate_tag_set(&tags, max_tags)?;
        Ok(Self { tags, max_tags })
    }

    pub fn empty(max_tags: usize) -> Self {
        Self {
            tags: Vec::new(),
            max_tags,
        }
    }

    /// Parse a `<Tagging>` XML request body into a validated tag set.
    pub fn parse_tagging_xml(data: &[u8], max_tags: usize) -> Result<Self, ServerError> {
        Self::new(
            parse_tag_collection_xml(
                data,
                max_tags,
                b"Tagging",
                b"TagSet",
                "missing <Tagging> element in tagging XML",
                "missing <TagSet> element in tagging XML",
            )?,
            max_tags,
        )
    }

    /// Parse a `TagResource` request body into a validated bucket tag set fragment.
    pub fn parse_tag_resource_xml(data: &[u8]) -> Result<Self, ServerError> {
        Self::new(
            parse_tag_collection_xml(
                data,
                50,
                b"TagResourceRequest",
                b"Tags",
                "missing <TagResourceRequest> element in tagging XML",
                "missing <Tags> element in tagging XML",
            )?,
            50,
        )
    }

    #[must_use]
    pub fn as_slice(&self) -> &[(String, String)] {
        &self.tags
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn reverse(&mut self) {
        self.tags.reverse();
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<(String, String)> {
        self.tags
    }

    pub fn merge(&self, updates: &Self) -> Result<Self, ServerError> {
        let mut merged = self.tags.clone();
        for (key, value) in &updates.tags {
            if let Some((_, existing_value)) = merged
                .iter_mut()
                .find(|(existing_key, _)| existing_key == key)
            {
                *existing_value = value.clone();
            } else {
                merged.push((key.clone(), value.clone()));
            }
        }
        Self::new(merged, self.max_tags)
    }

    #[must_use]
    pub fn remove_keys(&self, tag_keys: &[String]) -> Self {
        let to_remove: std::collections::HashSet<&str> =
            tag_keys.iter().map(String::as_str).collect();
        Self {
            tags: self
                .tags
                .iter()
                .filter(|(key, _)| !to_remove.contains(key.as_str()))
                .cloned()
                .collect(),
            max_tags: self.max_tags,
        }
    }

    #[must_use]
    pub fn to_xml(&self) -> String {
        serialize_tagging_xml(&self.tags)
    }
}

/// Parse a `<Tagging>` XML request body into a list of (key, value) pairs.
///
/// Validates S3 constraints: key 1–128 chars, value 0–256 chars,
/// unique keys, no `aws:` key prefix.
///
/// `max_tags` sets the limit: 10 for object tags, 50 for bucket tags.
pub fn parse_tagging_xml(
    data: &[u8],
    max_tags: usize,
) -> Result<Vec<(String, String)>, ServerError> {
    TagSet::parse_tagging_xml(data, max_tags).map(TagSet::into_vec)
}

/// Parse a `TagResource` request body into a list of (key, value) pairs.
pub fn parse_tag_resource_xml(data: &[u8]) -> Result<Vec<(String, String)>, ServerError> {
    TagSet::parse_tag_resource_xml(data).map(TagSet::into_vec)
}

fn serialize_tagging_xml(tags: &[(String, String)]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>",
    );
    for (k, v) in tags {
        xml.push_str("<Tag><Key>");
        xml.push_str(&xml_escape(k));
        xml.push_str("</Key><Value>");
        xml.push_str(&xml_escape(v));
        xml.push_str("</Value></Tag>");
    }
    xml.push_str("</TagSet></Tagging>");
    xml
}

/// Serialize a list of (key, value) tag pairs into S3 tagging XML.
#[must_use]
pub fn get_tagging_xml(tags: &[(String, String)]) -> String {
    serialize_tagging_xml(tags)
}

#[must_use]
pub fn merge_tag_set(
    existing: &[(String, String)],
    updates: &[(String, String)],
) -> Vec<(String, String)> {
    TagSet::new(existing.to_vec(), usize::MAX)
        .expect("existing tag set should already be valid")
        .merge(
            &TagSet::new(updates.to_vec(), usize::MAX)
                .expect("update tag set should already be valid"),
        )
        .expect("merged tag set should remain valid")
        .into_vec()
}

#[must_use]
pub fn remove_tag_keys(
    existing: &[(String, String)],
    tag_keys: &[String],
) -> Vec<(String, String)> {
    TagSet::new(existing.to_vec(), usize::MAX)
        .expect("existing tag set should already be valid")
        .remove_keys(tag_keys)
        .into_vec()
}

/// Parse URL-encoded tags from the `x-amz-tagging` header.
///
/// Format: `key1=value1&key2=value2`
/// Applies the same S3 validation constraints as `parse_tagging_xml`.
pub fn parse_url_encoded_tags(input: &str) -> Result<Vec<(String, String)>, ServerError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut tags = Vec::new();
    let mut seen_keys = std::collections::HashSet::new();

    for pair in input.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = match pair.find('=') {
            Some(pos) => (&pair[..pos], &pair[pos + 1..]),
            None => (pair, ""),
        };

        let key = percent_decode_tag(raw_key)?;
        let value = percent_decode_tag(raw_value)?;

        let key_chars = key.chars().count();
        if key_chars == 0 || key_chars > 128 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag key must be 1-128 characters, got {key_chars}"),
            });
        }
        if key.starts_with("aws:") {
            return Err(ServerError::InvalidTag {
                reason: "tag key must not start with 'aws:'".to_string(),
            });
        }
        let value_chars = value.chars().count();
        if value_chars > 256 {
            return Err(ServerError::InvalidTag {
                reason: format!("tag value must be 0-256 characters, got {value_chars}"),
            });
        }

        if !seen_keys.insert(key.clone()) {
            return Err(ServerError::InvalidTag {
                reason: format!("duplicate tag key: {key}"),
            });
        }

        tags.push((key, value));
    }

    if tags.len() > 10 {
        return Err(ServerError::InvalidTag {
            reason: format!("Object tags cannot be greater than 10, got {}", tags.len()),
        });
    }

    Ok(tags)
}

/// Count tags in canonical stored tagging XML.
pub fn count_tags_in_xml(xml: &str) -> Result<usize, ServerError> {
    parse_tagging_xml(xml.as_bytes(), 10).map(|tags| tags.len())
}

/// Percent-decode a tag key or value from URL-encoded form.
///
/// Collects decoded bytes first, then converts to UTF-8, so multibyte
/// percent-encoded sequences (e.g. `%C3%A9` for `é`) decode correctly.
fn percent_decode_tag(input: &str) -> Result<String, ServerError> {
    let mut bytes = Vec::with_capacity(input.len());
    let mut iter = input.bytes();
    while let Some(b) = iter.next() {
        if b == b'+' {
            bytes.push(b' ');
        } else if b == b'%' {
            let hi = iter.next().ok_or(ServerError::InvalidArgument {
                reason: "invalid percent-encoding in tagging header".to_string(),
            })?;
            let lo = iter.next().ok_or(ServerError::InvalidArgument {
                reason: "invalid percent-encoding in tagging header".to_string(),
            })?;
            let byte = decode_hex_pair(hi, lo).ok_or(ServerError::InvalidArgument {
                reason: "invalid percent-encoding in tagging header".to_string(),
            })?;
            bytes.push(byte);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).map_err(|_| ServerError::InvalidTag {
        reason: "The TagValue you have provided is invalid".to_string(),
    })
}

/// Decode a pair of hex characters into a byte.
fn decode_hex_pair(hi: u8, lo: u8) -> Option<u8> {
    let h = match hi {
        b'0'..=b'9' => hi - b'0',
        b'a'..=b'f' => hi - b'a' + 10,
        b'A'..=b'F' => hi - b'A' + 10,
        _ => return None,
    };
    let l = match lo {
        b'0'..=b'9' => lo - b'0',
        b'a'..=b'f' => lo - b'a' + 10,
        b'A'..=b'F' => lo - b'A' + 10,
        _ => return None,
    };
    Some(h << 4 | l)
}

/// Parse a `<PublicAccessBlockConfiguration>` XML request body.
///
/// Missing boolean elements default to `false`.
pub fn parse_public_access_block_xml(data: &[u8]) -> Result<PublicAccessBlockConfig, ServerError> {
    const MAX_PUBLIC_ACCESS_BLOCK_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InBlockPublicAcls,
        InIgnorePublicAcls,
        InBlockPublicPolicy,
        InRestrictPublicBuckets,
        Done,
    }

    fn malformed_pab_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    fn decode_pab_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in public access block XML body",
            "invalid XML entity in public access block XML body",
        )
    }

    fn parse_bool_text(text: &str) -> bool {
        text.trim().eq_ignore_ascii_case("true")
    }

    ensure_xml_body_size(data, MAX_PUBLIC_ACCESS_BLOCK_CONFIGURATION_BYTES)?;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut config = PublicAccessBlockConfig {
        block_public_acls: false,
        ignore_public_acls: false,
        block_public_policy: false,
        restrict_public_buckets: false,
    };
    let mut seen_block_public_acls = false;
    let mut seen_ignore_public_acls = false;
    let mut seen_block_public_policy = false;
    let mut seen_restrict_public_buckets = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"PublicAccessBlockConfiguration") => state = State::InRoot,
                (State::InRoot, b"BlockPublicAcls") => {
                    current_text.clear();
                    state = State::InBlockPublicAcls;
                }
                (State::InRoot, b"IgnorePublicAcls") => {
                    current_text.clear();
                    state = State::InIgnorePublicAcls;
                }
                (State::InRoot, b"BlockPublicPolicy") => {
                    current_text.clear();
                    state = State::InBlockPublicPolicy;
                }
                (State::InRoot, b"RestrictPublicBuckets") => {
                    current_text.clear();
                    state = State::InRestrictPublicBuckets;
                }
                _ => {
                    return Err(malformed_pab_xml(
                        "unexpected element in public access block XML",
                    ))
                }
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"BlockPublicAcls") => seen_block_public_acls = true,
                (State::InRoot, b"IgnorePublicAcls") => seen_ignore_public_acls = true,
                (State::InRoot, b"BlockPublicPolicy") => seen_block_public_policy = true,
                (State::InRoot, b"RestrictPublicBuckets") => {
                    seen_restrict_public_buckets = true;
                }
                _ => {
                    return Err(malformed_pab_xml(
                        "unexpected empty element in public access block XML",
                    ));
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"PublicAccessBlockConfiguration") => state = State::Done,
                (State::InBlockPublicAcls, b"BlockPublicAcls") => {
                    if !seen_block_public_acls {
                        config.block_public_acls = parse_bool_text(&current_text);
                        seen_block_public_acls = true;
                    }
                    current_text.clear();
                    state = State::InRoot;
                }
                (State::InIgnorePublicAcls, b"IgnorePublicAcls") => {
                    if !seen_ignore_public_acls {
                        config.ignore_public_acls = parse_bool_text(&current_text);
                        seen_ignore_public_acls = true;
                    }
                    current_text.clear();
                    state = State::InRoot;
                }
                (State::InBlockPublicPolicy, b"BlockPublicPolicy") => {
                    if !seen_block_public_policy {
                        config.block_public_policy = parse_bool_text(&current_text);
                        seen_block_public_policy = true;
                    }
                    current_text.clear();
                    state = State::InRoot;
                }
                (State::InRestrictPublicBuckets, b"RestrictPublicBuckets") => {
                    if !seen_restrict_public_buckets {
                        config.restrict_public_buckets = parse_bool_text(&current_text);
                        seen_restrict_public_buckets = true;
                    }
                    current_text.clear();
                    state = State::InRoot;
                }
                _ => {
                    return Err(malformed_pab_xml(
                        "unexpected closing element in public access block XML",
                    ));
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_pab_text(t.as_ref())?;
                match state {
                    State::InBlockPublicAcls
                    | State::InIgnorePublicAcls
                    | State::InBlockPublicPolicy
                    | State::InRestrictPublicBuckets => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_pab_xml(
                            "unexpected text in public access block XML",
                        ))
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref()).map_err(|_| {
                    malformed_pab_xml("invalid UTF-8 in public access block XML body")
                })?;
                match state {
                    State::InBlockPublicAcls
                    | State::InIgnorePublicAcls
                    | State::InBlockPublicPolicy
                    | State::InRestrictPublicBuckets => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_pab_xml(
                            "unexpected CDATA in public access block XML",
                        ));
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => Ok(config),
                    State::Start => Err(malformed_pab_xml(
                        "missing PublicAccessBlockConfiguration element",
                    )),
                    _ => Err(malformed_pab_xml(
                        "unexpected end of public access block XML",
                    )),
                };
            }
            Err(_) => return Err(malformed_pab_xml("malformed public access block XML")),
        }
        buf.clear();
    }
}

/// Serialize a `PublicAccessBlockConfig` into S3 response XML.
#[must_use]
pub fn get_public_access_block_xml(config: &PublicAccessBlockConfig) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <PublicAccessBlockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <BlockPublicAcls>{}</BlockPublicAcls>\
         <IgnorePublicAcls>{}</IgnorePublicAcls>\
         <BlockPublicPolicy>{}</BlockPublicPolicy>\
         <RestrictPublicBuckets>{}</RestrictPublicBuckets>\
         </PublicAccessBlockConfiguration>",
        if config.block_public_acls {
            "true"
        } else {
            "false"
        },
        if config.ignore_public_acls {
            "true"
        } else {
            "false"
        },
        if config.block_public_policy {
            "true"
        } else {
            "false"
        },
        if config.restrict_public_buckets {
            "true"
        } else {
            "false"
        },
    )
}

/// Parse an `<OwnershipControls>` XML request body.
///
/// Extracts the `ObjectOwnership` value from
/// `<OwnershipControls><Rule><ObjectOwnership>VALUE</ObjectOwnership></Rule></OwnershipControls>`.
/// Validates VALUE is one of `BucketOwnerEnforced`, `BucketOwnerPreferred`, or `ObjectWriter`.
pub fn parse_ownership_controls_xml(data: &[u8]) -> Result<BucketOwnershipControls, ServerError> {
    const MAX_OWNERSHIP_CONTROLS_XML_BYTES: usize = 2048;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InRule,
        InObjectOwnership,
        Done,
    }

    fn malformed_ownership_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    fn decode_ownership_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "invalid UTF-8 in ownership controls XML body",
            "invalid XML entity in ownership controls XML body",
        )
    }

    ensure_xml_body_size(data, MAX_OWNERSHIP_CONTROLS_XML_BYTES)?;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut object_ownership: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"OwnershipControls") => state = State::InRoot,
                (State::InRoot, b"Rule") => state = State::InRule,
                (State::InRule, b"ObjectOwnership") => {
                    current_text.clear();
                    state = State::InObjectOwnership;
                }
                _ => {
                    return Err(malformed_ownership_xml(
                        "unexpected element in ownership controls XML",
                    ))
                }
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::InRule, b"ObjectOwnership") => object_ownership = Some(String::new()),
                _ => {
                    return Err(malformed_ownership_xml(
                        "unexpected empty element in ownership controls XML",
                    ));
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"OwnershipControls") => state = State::Done,
                (State::InRule, b"Rule") => {
                    if object_ownership.is_none() {
                        return Err(malformed_ownership_xml(
                            "missing ObjectOwnership element in Rule",
                        ));
                    }
                    state = State::InRoot;
                }
                (State::InObjectOwnership, b"ObjectOwnership") => {
                    object_ownership = Some(std::mem::take(&mut current_text));
                    state = State::InRule;
                }
                _ => {
                    return Err(malformed_ownership_xml(
                        "unexpected closing element in ownership controls XML",
                    ));
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_ownership_text(t.as_ref())?;
                match state {
                    State::InObjectOwnership => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_ownership_xml(
                            "unexpected text in ownership controls XML",
                        ))
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref()).map_err(|_| {
                    malformed_ownership_xml("invalid UTF-8 in ownership controls XML body")
                })?;
                match state {
                    State::InObjectOwnership => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_ownership_xml(
                            "unexpected CDATA in ownership controls XML",
                        ));
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => match object_ownership.as_deref() {
                        Some("BucketOwnerEnforced") => Ok(BucketOwnershipControls {
                            object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
                        }),
                        Some("BucketOwnerPreferred") => Ok(BucketOwnershipControls {
                            object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
                        }),
                        Some("ObjectWriter") => Ok(BucketOwnershipControls {
                            object_ownership: BucketObjectOwnership::ObjectWriter,
                        }),
                        Some(_) => Err(malformed_ownership_xml(
                            "invalid ObjectOwnership value in OwnershipControls",
                        )),
                        None => Err(malformed_ownership_xml(
                            "missing Rule element in OwnershipControls",
                        )),
                    },
                    State::Start => {
                        Err(malformed_ownership_xml("missing OwnershipControls element"))
                    }
                    _ => Err(malformed_ownership_xml(
                        "unexpected end of ownership controls XML",
                    )),
                };
            }
            Err(_) => return Err(malformed_ownership_xml("malformed ownership controls XML")),
        }
        buf.clear();
    }
}

/// Serialize an `ObjectOwnership` value into S3 response XML.
#[must_use]
pub fn get_ownership_controls_xml(config: &BucketOwnershipControls) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ObjectOwnership>{}</ObjectOwnership></Rule>\
         </OwnershipControls>",
        config.object_ownership.as_str(),
    )
}

/// Parse an `<AbacStatus>` XML request body.
pub fn parse_bucket_abac_xml(data: &[u8]) -> Result<bool, ServerError> {
    const MAX_BUCKET_ABAC_XML_BYTES: usize = 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InStatus,
        Done,
    }

    fn malformed_abac_xml(reason: &str) -> ServerError {
        ServerError::MalformedXML {
            reason: reason.to_string(),
        }
    }

    ensure_xml_body_size(data, MAX_BUCKET_ABAC_XML_BYTES)?;

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut status: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"AbacStatus") => state = State::InRoot,
                (State::InRoot, b"Status") => {
                    current_text.clear();
                    state = State::InStatus;
                }
                _ => return Err(malformed_abac_xml("unexpected element in bucket ABAC XML")),
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"Status") => status = Some(String::new()),
                _ => {
                    return Err(malformed_abac_xml(
                        "unexpected empty element in bucket ABAC XML",
                    ));
                }
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"AbacStatus") => state = State::Done,
                (State::InStatus, b"Status") => {
                    status = Some(std::mem::take(&mut current_text));
                    state = State::InRoot;
                }
                _ => {
                    return Err(malformed_abac_xml(
                        "unexpected closing element in bucket ABAC XML",
                    ));
                }
            },
            Ok(Event::Text(t)) => {
                let text = decode_xml_text(
                    t.as_ref(),
                    "invalid UTF-8 in bucket ABAC XML body",
                    "invalid XML entity in bucket ABAC XML body",
                )?;
                match state {
                    State::InStatus => current_text.push_str(&text),
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_abac_xml("unexpected text in bucket ABAC XML")),
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref())
                    .map_err(|_| malformed_abac_xml("invalid UTF-8 in bucket ABAC XML body"))?;
                match state {
                    State::InStatus => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_abac_xml("unexpected CDATA in bucket ABAC XML")),
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => match status.as_deref() {
                        Some("Enabled") => Ok(true),
                        Some("Disabled") => Ok(false),
                        Some(_) => Err(malformed_abac_xml("invalid Status value in AbacStatus")),
                        None => Err(malformed_abac_xml("missing Status element in AbacStatus")),
                    },
                    State::Start => Err(malformed_abac_xml("missing AbacStatus element")),
                    _ => Err(malformed_abac_xml("unexpected end of bucket ABAC XML")),
                };
            }
            Err(_) => return Err(malformed_abac_xml("malformed bucket ABAC XML")),
        }
        buf.clear();
    }
}

#[must_use]
pub fn get_bucket_abac_xml(enabled: bool) -> String {
    let status = if enabled { "Enabled" } else { "Disabled" };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <AbacStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Status>{status}</Status>\
         </AbacStatus>"
    )
}

/// Recognized object attribute names for `GetObjectAttributes`.
const VALID_OBJECT_ATTRIBUTES: &[&str] = &[
    "ETag",
    "Checksum",
    "ObjectParts",
    "StorageClass",
    "ObjectSize",
];

/// Check whether an attribute name is valid for `GetObjectAttributes`.
#[must_use]
pub fn is_valid_object_attribute(name: &str) -> bool {
    VALID_OBJECT_ATTRIBUTES.contains(&name)
}

/// Build a `<GetObjectAttributesResponse>` XML body.
///
/// `requested` is the set of attribute names from the `x-amz-object-attributes`
/// header. Only requested attributes appear in the response.
///
/// `etag` should be the quoted `ETag` string (quotes will be stripped).
/// `checksum_entries` are the `x-amz-checksum-*` metadata entries.
#[must_use]
#[allow(clippy::format_push_string)]
pub fn get_object_attributes_xml(
    requested: &[&str],
    etag: &str,
    size: u64,
    checksum_entries: &[(&str, &str)],
    object_parts: Option<&ObjectPartsInfo>,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    let wants_etag = requested.contains(&"ETag");
    let wants_checksum = requested.contains(&"Checksum");
    let wants_object_parts = requested.contains(&"ObjectParts");
    let wants_storage_class = requested.contains(&"StorageClass");
    let wants_object_size = requested.contains(&"ObjectSize");

    if wants_etag {
        // Strip surrounding quotes from etag
        let unquoted = etag.trim_matches('"');
        xml.push_str("<ETag>");
        xml.push_str(&xml_escape(unquoted));
        xml.push_str("</ETag>");
    }

    if wants_checksum && !checksum_entries.is_empty() {
        xml.push_str("<Checksum>");
        for &(header_key, value) in checksum_entries {
            if header_key == "x-amz-checksum-type" {
                xml.push_str("<ChecksumType>");
                xml.push_str(&xml_escape(value));
                xml.push_str("</ChecksumType>");
            } else if let Some(xml_tag) = checksum_header_to_xml_tag(header_key) {
                // Strip the composite "-N" suffix (part count) from checksum
                // values. GetObjectAttributes uses ChecksumType to convey
                // composite vs full-object; the hash itself has no suffix.
                let bare = strip_composite_suffix(value);
                xml.push('<');
                xml.push_str(xml_tag);
                xml.push('>');
                xml.push_str(&xml_escape(bare));
                xml.push_str("</");
                xml.push_str(xml_tag);
                xml.push('>');
            }
        }
        xml.push_str("</Checksum>");
    }

    if wants_object_parts {
        if let Some(parts_info) = object_parts {
            xml.push_str("<ObjectParts>");
            xml.push_str("<PartsCount>");
            xml.push_str(&parts_info.total_parts_count.to_string());
            xml.push_str("</PartsCount>");
            if parts_info.has_detail {
                xml.push_str("<PartNumberMarker>");
                xml.push_str(&parts_info.part_number_marker.to_string());
                xml.push_str("</PartNumberMarker>");
                if let Some(next) = parts_info.next_part_number_marker {
                    xml.push_str("<NextPartNumberMarker>");
                    xml.push_str(&next.to_string());
                    xml.push_str("</NextPartNumberMarker>");
                }
                xml.push_str("<MaxParts>");
                xml.push_str(&parts_info.max_parts.to_string());
                xml.push_str("</MaxParts>");
                xml.push_str("<IsTruncated>");
                xml.push_str(if parts_info.is_truncated {
                    "true"
                } else {
                    "false"
                });
                xml.push_str("</IsTruncated>");
                for part in &parts_info.parts {
                    xml.push_str("<Part>");
                    xml.push_str("<PartNumber>");
                    xml.push_str(&part.part_number.to_string());
                    xml.push_str("</PartNumber>");
                    xml.push_str("<Size>");
                    xml.push_str(&part.size.to_string());
                    xml.push_str("</Size>");
                    if let (Some(algo), Some(ref val)) = (checksum_algorithm, &part.checksum) {
                        let elem = algo.xml_element_name();
                        xml.push_str(&format!("<{elem}>{}</{elem}>", xml_escape(val)));
                    }
                    xml.push_str("</Part>");
                }
            }
            xml.push_str("</ObjectParts>");
        }
    }

    if wants_storage_class {
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
    }

    if wants_object_size {
        xml.push_str("<ObjectSize>");
        xml.push_str(&size.to_string());
        xml.push_str("</ObjectSize>");
    }

    xml.push_str("</GetObjectAttributesResponse>");
    xml
}

/// Strip the composite checksum "-N" suffix (e.g. "abc=-3" → "abc=").
///
/// AWS returns the bare hash (no part count) in `GetObjectAttributes`; the part
/// count is conveyed by `<ChecksumType>COMPOSITE</ChecksumType>` instead.
/// Standard base64 never contains '-', so a trailing "-\d+" is always the
/// composite suffix.
fn strip_composite_suffix(value: &str) -> &str {
    if let Some(pos) = value.rfind('-') {
        if value[pos + 1..].bytes().all(|b| b.is_ascii_digit()) && !value[pos + 1..].is_empty() {
            return &value[..pos];
        }
    }
    value
}

/// Map a metadata header key like `x-amz-checksum-sha256` to an XML element
/// name like `ChecksumSHA256`.
fn checksum_header_to_xml_tag(header: &str) -> Option<&'static str> {
    match header {
        "x-amz-checksum-sha256" => Some("ChecksumSHA256"),
        "x-amz-checksum-sha1" => Some("ChecksumSHA1"),
        "x-amz-checksum-crc32" => Some("ChecksumCRC32"),
        "x-amz-checksum-crc32c" => Some("ChecksumCRC32C"),
        "x-amz-checksum-crc64nvme" => Some("ChecksumCRC64NVME"),
        _ => None,
    }
}

// ── Multipart upload XML ─────────────────────────────────────────

/// Format an `InitiateMultipartUploadResult` XML response.
#[must_use]
pub fn initiate_multipart_upload_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    checksum_algorithm: Option<&str>,
    checksum_type: Option<&str>,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <UploadId>{}</UploadId>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    );
    if let Some(algo) = checksum_algorithm {
        xml.push_str("<ChecksumAlgorithm>");
        xml.push_str(algo);
        xml.push_str("</ChecksumAlgorithm>");
    }
    if let Some(ctype) = checksum_type {
        xml.push_str("<ChecksumType>");
        xml.push_str(ctype);
        xml.push_str("</ChecksumType>");
    }
    xml.push_str("</InitiateMultipartUploadResult>");
    xml
}

/// Percent-encode a logical key for use in URLs, preserving '/'.
///
/// Unlike `uri_encode_path` (which preserves existing %XX sequences for
/// canonical request paths), this encodes every byte that needs encoding,
/// including literal '%' characters. Use this for Location URLs where the
/// input is a logical object key, not a raw request path.
fn uri_encode_key(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0F) as usize] as char);
            }
        }
    }
    out
}

/// Format a `CompleteMultipartUploadResult` XML response.
#[must_use]
pub fn complete_multipart_upload_xml(
    bucket: &str,
    key: &str,
    etag: &str,
    checksum_algorithm: Option<ChecksumAlgorithm>,
    checksum_type: Option<ChecksumType>,
    checksum_value: Option<&str>,
) -> String {
    // Location uses path-style: http://s3.amazonaws.com/<bucket>/<key>
    let location = format!(
        "http://s3.amazonaws.com/{}/{}",
        uri_encode_key(bucket),
        uri_encode_key(key)
    );
    let checksum_xml = match (checksum_algorithm, checksum_value) {
        (Some(algo), Some(val)) => {
            let elem = algo.xml_element_name();
            let mut xml = format!("<{elem}>{}</{elem}>", xml_escape(val));
            if let Some(checksum_type) = checksum_type {
                xml.push_str("<ChecksumType>");
                xml.push_str(checksum_type.as_str());
                xml.push_str("</ChecksumType>");
            }
            xml
        }
        _ => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Location>{}</Location>\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <ETag>{}</ETag>\
         {}\
         </CompleteMultipartUploadResult>",
        xml_escape(&location),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
        checksum_xml,
    )
}

/// Format a `ListMultipartUploadsResult` XML response.
#[must_use]
#[allow(clippy::format_push_string)]
pub fn list_multipart_uploads_xml(
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
    encoding_type: Option<&str>,
    max_uploads: u32,
    result: &RenderedListMultipartUploadsResult,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket>",
        xml_escape(bucket),
    );
    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape_list_value(&encode_value(p, encoding_type)));
        xml.push_str("</Prefix>");
    }
    xml.push_str("<KeyMarker>");
    if let Some(km) = key_marker {
        xml.push_str(&xml_escape_list_value(&encode_value(km, encoding_type)));
    }
    xml.push_str("</KeyMarker>");
    xml.push_str("<UploadIdMarker>");
    if let Some(um) = upload_id_marker {
        xml.push_str(&xml_escape(um));
    }
    xml.push_str("</UploadIdMarker>");
    if let Some(e) = encoding_type {
        xml.push_str("<EncodingType>");
        xml.push_str(&xml_escape(e));
        xml.push_str("</EncodingType>");
    }
    if let Some(ref nkm) = result.next_key_marker {
        xml.push_str("<NextKeyMarker>");
        xml.push_str(&xml_escape_list_value(&encode_value(nkm, encoding_type)));
        xml.push_str("</NextKeyMarker>");
    }
    if let Some(ref num) = result.next_upload_id_marker {
        xml.push_str(&format!(
            "<NextUploadIdMarker>{}</NextUploadIdMarker>",
            xml_escape(num)
        ));
    }
    xml.push_str(&format!("<MaxUploads>{max_uploads}</MaxUploads>"));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    for upload in &result.uploads {
        xml.push_str("<Upload>");
        xml.push_str("<Key>");
        xml.push_str(&xml_escape_list_value(&encode_value(
            &upload.key,
            encoding_type,
        )));
        xml.push_str("</Key>");
        xml.push_str(&format!(
            "<UploadId>{}</UploadId>",
            xml_escape(&upload.upload_id)
        ));
        append_canonical_owner_xml(&mut xml, "Initiator", &upload.initiator);
        append_canonical_owner_xml(&mut xml, "Owner", &upload.owner);
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str(&format!(
            "<Initiated>{}</Initiated>",
            format_timestamp(upload.initiated)
        ));
        if let Some(algo) = upload.checksum_algorithm {
            xml.push_str(&format!(
                "<ChecksumAlgorithm>{}</ChecksumAlgorithm>",
                algo.as_str()
            ));
        }
        if let Some(checksum_type) = upload.checksum_type {
            xml.push_str(&format!(
                "<ChecksumType>{}</ChecksumType>",
                checksum_type.as_str()
            ));
        }
        xml.push_str("</Upload>");
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml
}

/// Format a `ListPartsResult` XML response.
#[must_use]
#[allow(clippy::format_push_string)]
pub fn list_parts_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number_marker: Option<u32>,
    max_parts: u32,
    result: &ListPartsResult,
) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{}</Bucket>\
         <Key>{}</Key>\
         <UploadId>{}</UploadId>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    );
    if let Some(pm) = part_number_marker {
        xml.push_str(&format!("<PartNumberMarker>{pm}</PartNumberMarker>"));
    } else {
        xml.push_str("<PartNumberMarker>0</PartNumberMarker>");
    }
    xml.push_str(&format!("<MaxParts>{max_parts}</MaxParts>"));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    if let Some(npm) = result.next_part_number_marker {
        xml.push_str(&format!(
            "<NextPartNumberMarker>{npm}</NextPartNumberMarker>"
        ));
    }
    if let Some(algo) = result.checksum_algorithm {
        xml.push_str(&format!(
            "<ChecksumAlgorithm>{}</ChecksumAlgorithm>",
            algo.as_str()
        ));
    }
    for part in &result.parts {
        xml.push_str(&format!(
            "<Part>\
             <PartNumber>{}</PartNumber>\
             <LastModified>{}</LastModified>\
             <ETag>{}</ETag>\
             <Size>{}</Size>",
            part.part_number,
            format_timestamp(part.last_modified),
            xml_escape(&part.etag),
            part.size,
        ));
        if let (Some(algo), Some(ref val)) = (result.checksum_algorithm, &part.checksum) {
            let elem = algo.xml_element_name();
            xml.push_str(&format!("<{elem}>{}</{elem}>", xml_escape(val)));
        }
        xml.push_str("</Part>");
    }
    xml.push_str("</ListPartsResult>");
    xml
}

/// S3 checksum XML element names mapped to their algorithms.
const CHECKSUM_ELEMENTS: &[(&str, ChecksumAlgorithm)] = &[
    ("ChecksumCRC32C", ChecksumAlgorithm::Crc32c),
    ("ChecksumCRC32", ChecksumAlgorithm::Crc32),
    ("ChecksumSHA1", ChecksumAlgorithm::Sha1),
    ("ChecksumSHA256", ChecksumAlgorithm::Sha256),
    ("ChecksumCRC64NVME", ChecksumAlgorithm::Crc64nvme),
];

/// Parse a `CompleteMultipartUpload` request XML body into a list of parts.
///
/// Expected format:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>
///   ...
/// </CompleteMultipartUpload>
/// ```
pub fn parse_complete_multipart_upload_xml(body: &[u8]) -> Result<Vec<CompletePart>, ServerError> {
    const MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES: usize = 2_621_440;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InRoot,
        InPart,
        InPartNumber,
        InETag,
        InChecksum(ChecksumAlgorithm),
        Done,
    }

    fn malformed_complete_multipart_xml() -> ServerError {
        ServerError::MalformedXML {
            reason: "malformed CompleteMultipartUpload XML".to_string(),
        }
    }

    fn checksum_algorithm_for_element(name: &[u8]) -> Option<ChecksumAlgorithm> {
        CHECKSUM_ELEMENTS
            .iter()
            .find_map(|&(elem, algo)| (name == elem.as_bytes()).then_some(algo))
    }

    fn decode_complete_multipart_text(bytes: &[u8]) -> Result<String, ServerError> {
        decode_xml_text(
            bytes,
            "malformed CompleteMultipartUpload XML",
            "malformed CompleteMultipartUpload XML",
        )
    }

    ensure_xml_body_size(body, MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES)?;

    let mut reader = Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut current_text = String::new();
    let mut current_part_number: Option<u32> = None;
    let mut current_etag: Option<String> = None;
    let mut current_checksum: Option<ChecksumClaim> = None;
    let mut parts = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"CompleteMultipartUpload") => state = State::InRoot,
                (State::InRoot, b"Part") => {
                    current_part_number = None;
                    current_etag = None;
                    current_checksum = None;
                    state = State::InPart;
                }
                (State::InPart, b"PartNumber") => {
                    current_text.clear();
                    state = State::InPartNumber;
                }
                (State::InPart, b"ETag") => {
                    current_text.clear();
                    state = State::InETag;
                }
                (State::InPart, name) => {
                    if let Some(algo) = checksum_algorithm_for_element(name) {
                        if current_checksum.is_some() {
                            return Err(ServerError::MalformedXML {
                                reason: "multiple checksum elements in a single Part".to_string(),
                            });
                        }
                        current_text.clear();
                        state = State::InChecksum(algo);
                    } else {
                        return Err(malformed_complete_multipart_xml());
                    }
                }
                _ => return Err(malformed_complete_multipart_xml()),
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"Part") => {}
                (State::InPart, b"PartNumber" | b"ETag") => {}
                (State::InPart, name) if checksum_algorithm_for_element(name).is_some() => {}
                _ => return Err(malformed_complete_multipart_xml()),
            },
            Ok(Event::End(e)) => match (state, e.name().as_ref()) {
                (State::InRoot, b"CompleteMultipartUpload") => state = State::Done,
                (State::InPart, b"Part") => {
                    let part_number =
                        current_part_number.ok_or_else(malformed_complete_multipart_xml)?;
                    let etag = current_etag
                        .take()
                        .ok_or_else(malformed_complete_multipart_xml)?;
                    parts.push(CompletePart {
                        part_number,
                        etag,
                        checksum: current_checksum.take(),
                    });
                    state = State::InRoot;
                }
                (State::InPartNumber, b"PartNumber") => {
                    if current_part_number.is_none() {
                        current_part_number = Some(
                            current_text
                                .trim()
                                .parse()
                                .map_err(|_| malformed_complete_multipart_xml())?,
                        );
                    }
                    current_text.clear();
                    state = State::InPart;
                }
                (State::InETag, b"ETag") => {
                    if current_etag.is_none() {
                        current_etag = Some(current_text.trim().to_string());
                    }
                    current_text.clear();
                    state = State::InPart;
                }
                (State::InChecksum(algo), name)
                    if checksum_algorithm_for_element(name) == Some(algo) =>
                {
                    if current_checksum.is_none() {
                        current_checksum =
                            Some(ChecksumClaim::from_base64(algo, current_text.trim())?);
                    }
                    current_text.clear();
                    state = State::InPart;
                }
                _ => return Err(malformed_complete_multipart_xml()),
            },
            Ok(Event::Text(t)) => {
                let text = decode_complete_multipart_text(t.as_ref())?;
                match state {
                    State::InPartNumber | State::InETag | State::InChecksum(_) => {
                        current_text.push_str(&text);
                    }
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_complete_multipart_xml()),
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref())
                    .map_err(|_| malformed_complete_multipart_xml())?;
                match state {
                    State::InPartNumber | State::InETag | State::InChecksum(_) => {
                        current_text.push_str(text);
                    }
                    _ if text.trim().is_empty() => {}
                    _ => return Err(malformed_complete_multipart_xml()),
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done if !parts.is_empty() => Ok(parts),
                    _ => Err(malformed_complete_multipart_xml()),
                };
            }
            Err(_) => return Err(malformed_complete_multipart_xml()),
        }
        buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conditional::{DeleteCondition, WriteCondition};
    use crate::coordinator::ListEntry;
    use crate::coordinator::{
        test_helpers, Coordinator, DeleteEntry, DeleteObjectsRequest, ListObjectVersionsRequest,
        ListObjectsV2Request, PutObjectAcl, PutObjectRequest, Requester,
    };
    use crate::metadata_blob::MetadataBlob;
    use ec::EcConfig;
    use server_core::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider};
    use server_core::system_metadata::SystemMetadata;
    use std::sync::Arc;
    use storage::SharedStorageNode;

    const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

    fn setup_coordinator(dir: &std::path::Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::default();
        let sse_s3_provider = StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        );
        Coordinator::new_with_managed_key_provider(
            storage_node,
            ec_config,
            "us-east-1".to_string(),
            None,
            sse_s3_provider,
        )
        .unwrap()
    }

    const NO_WRITE: &WriteCondition = &WriteCondition::None;
    const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

    fn test_requester() -> Requester {
        test_helpers::requester("default-owner")
    }

    fn test_bucket_request(name: &str) -> crate::coordinator::BucketRequest<'_> {
        crate::coordinator::BucketRequest::new(
            storage::BucketName::try_from(name.to_string()).unwrap(),
            test_requester(),
            None,
        )
    }

    fn test_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
    ) -> crate::coordinator::ObjectRequest<'a> {
        crate::coordinator::ObjectRequest::new(
            storage::BucketName::try_from(bucket.to_string()).unwrap(),
            storage::ObjectKey::try_from(key.to_string()).unwrap(),
            test_requester(),
            None,
        )
    }

    fn create_test_bucket(coord: &Coordinator, name: &str) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: storage::BucketName::try_from(name.to_string()).unwrap(),
                requester: test_requester(),
                namespace: s3_types::BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
    }

    #[test]
    fn error_xml_format() {
        let xml = error_xml(
            "NoSuchBucket",
            "The bucket does not exist",
            "/mybucket",
            "req-1",
        );
        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Message>The bucket does not exist</Message>"));
        assert!(xml.contains("<?xml"));
    }

    #[test]
    fn list_buckets_xml_format() {
        let buckets = vec![BucketSummary {
            name: storage::BucketName::try_from("test-bucket").unwrap(),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
            created_at: 1685000000000,
            acl_grants: s3_types::AclGrants::default(),
            versioning: BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            public_read: false,
            public_write: false,
            public_access_block: None,
            ownership_controls: None,
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }];
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let xml = list_buckets_xml(&buckets, "Owner A", &owner_canonical_id);
        assert!(xml.contains("<Name>test-bucket</Name>"));
        assert!(xml.contains("ListAllMyBucketsResult"));
        assert!(xml.contains(&format!("<ID>{}</ID>", owner_canonical_id.as_str())));
        assert!(xml.contains("<DisplayName>Owner A</DisplayName>"));
    }

    #[test]
    fn parse_bucket_lifecycle_configuration_invalid_status_maps_to_malformed_xml() {
        let err = parse_bucket_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::MalformedXML { .. }));
    }

    #[test]
    fn parse_bucket_lifecycle_configuration_transition_maps_to_not_implemented() {
        let err = parse_bucket_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Transition><Days>1</Days></Transition>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::NotImplemented { .. }));
    }

    #[test]
    fn get_bucket_lifecycle_configuration_xml_renders_canonical_xml() {
        let config = parse_bucket_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <ID>expire-current</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>3</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap();
        let xml = get_bucket_lifecycle_configuration_xml(&config);
        assert!(xml.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<LifecycleConfiguration"));
        assert!(xml.contains("<ID>expire-current</ID>"));
        assert!(xml.contains("<Prefix>logs/</Prefix>"));
        assert!(xml.contains("<Days>3</Days>"));
    }

    #[test]
    fn bucket_acl_xml_uses_canonical_owner_id() {
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let xml = bucket_acl_xml("owner", &owner_canonical_id, BucketAcl::PublicRead);
        assert!(xml.contains(&format!("<Owner><ID>{}</ID>", owner_canonical_id.as_str())));
        assert!(!xml.contains("<DisplayName>"));
        assert!(xml.contains("CanonicalUser"));
    }

    #[test]
    fn bucket_acl_xml_renders_authenticated_users_group() {
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let xml = bucket_acl_xml("owner", &owner_canonical_id, BucketAcl::AuthenticatedRead);
        assert!(xml.contains(AclGrantee::authenticated_users_uri()));
        assert!(xml.contains("<Permission>READ</Permission>"));
    }

    #[test]
    fn parse_acl_xml_accepts_authenticated_users_group() {
        let grants = parse_acl_xml(
            format!(
                "<AccessControlPolicy><AccessControlList>\
                 <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\
                 <URI>{}</URI></Grantee><Permission>READ</Permission></Grant>\
                 </AccessControlList></AccessControlPolicy>",
                AclGrantee::authenticated_users_uri()
            )
            .as_bytes(),
        )
        .unwrap();

        assert!(grants.iter().any(|grant| {
            grant.grantee() == &AclGrantee::AuthenticatedUsers
                && grant.permission() == AclPermission::Read
        }));
    }

    #[test]
    fn parse_acl_xml_ignores_owner_id() {
        let owner_canonical_id = CanonicalUserId::from_principal("owner");
        let grants = parse_acl_xml(
            format!(
                "<AccessControlPolicy><Owner><ID>{}</ID></Owner><AccessControlList>\
                 <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
                 <ID>{}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
                 </AccessControlList></AccessControlPolicy>",
                owner_canonical_id.as_str(),
                owner_canonical_id.as_str()
            )
            .as_bytes(),
        )
        .unwrap();

        assert!(grants.iter().any(|grant| {
            grant.grantee() == &AclGrantee::CanonicalUser(owner_canonical_id.clone())
                && grant.permission() == AclPermission::FullControl
        }));
    }

    #[test]
    fn list_objects_xml_format() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "my-key".to_string(),
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(ChecksumType::FullObject),
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v2_xml("bucket", None, None, None, None, None, true, 1000, &result);
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<Size>42</Size>"));
        assert!(xml.contains("ListBucketResult"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID></Owner>",
            result.owner_canonical_id.as_str()
        )));
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
    }

    #[test]
    fn list_objects_xml_with_prefix_delimiter_and_truncation() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "photos/cat.jpg".to_string(),
                size: 100,
                etag: "\"aabbccdd\"".to_string(),
                last_modified: 1685000000000,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            common_prefixes: vec!["photos/2024/".to_string()],
            is_truncated: true,
            next_continuation_token: Some("photos/cat.jpg".to_string()),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v2_xml(
            "bucket",
            Some("photos/"),
            Some("/"),
            None,
            None,
            None,
            false,
            1,
            &result,
        );
        assert!(xml.contains("<Prefix>photos/</Prefix>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<NextContinuationToken>photos/cat.jpg</NextContinuationToken>"));
        assert!(xml.contains("<KeyCount>2</KeyCount>"));
        assert!(xml.contains("<MaxKeys>1</MaxKeys>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        let next_token_pos = xml.find("<NextContinuationToken>").unwrap();
        let key_count_pos = xml.find("<KeyCount>").unwrap();
        let max_keys_pos = xml.find("<MaxKeys>").unwrap();
        let truncated_pos = xml.find("<IsTruncated>").unwrap();
        assert!(next_token_pos < key_count_pos);
        assert!(key_count_pos < max_keys_pos);
        assert!(max_keys_pos < truncated_pos);
        assert!(xml.contains("<CommonPrefixes><Prefix>photos/2024/</Prefix></CommonPrefixes>"));
    }

    #[test]
    fn list_objects_xml_with_url_encoding() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "dir/hello world&plus+".to_string(),
                size: 7,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            common_prefixes: vec!["dir/prefix here+".to_string()],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v2_xml(
            "bucket",
            None,
            Some("/"),
            Some("url"),
            None,
            None,
            false,
            1000,
            &result,
        );
        assert!(xml.contains("<EncodingType>url</EncodingType>"));
        assert!(xml.contains("<Key>dir/hello+world%26plus%2B</Key>"));
        assert!(
            xml.contains("<CommonPrefixes><Prefix>dir/prefix+here%2B</Prefix></CommonPrefixes>")
        );
    }

    #[test]
    fn list_objects_xml_key_count_includes_prefixes() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "root.txt".to_string(),
                size: 10,
                etag: "\"abc\"".to_string(),
                last_modified: 0,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            common_prefixes: vec!["photos/".to_string(), "docs/".to_string()],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v2_xml(
            "bucket",
            None,
            Some("/"),
            None,
            None,
            None,
            false,
            1000,
            &result,
        );
        // 1 object + 2 prefixes = 3
        assert!(xml.contains("<KeyCount>3</KeyCount>"));
    }

    #[test]
    fn list_objects_xml_empty_prefix() {
        let result = ListObjectsResult {
            objects: vec![],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        // With prefix=None AWS emits an explicit empty Prefix element.
        let xml = list_objects_v2_xml("bucket", None, None, None, None, None, false, 1000, &result);
        assert!(xml.contains("<Prefix></Prefix>"));
        assert!(!xml.contains("<Delimiter>"));
        assert!(!xml.contains("<NextContinuationToken>"));
    }

    #[test]
    fn list_objects_v1_xml_format() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "my-key".to_string(),
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(ChecksumType::FullObject),
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v1_xml("bucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<Prefix></Prefix>"));
        assert!(xml.contains("<Marker></Marker>"));
        assert!(xml.contains("ListBucketResult"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID></Owner>",
            result.owner_canonical_id.as_str()
        )));
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
        // V1 should NOT have KeyCount or ContinuationToken
        assert!(!xml.contains("<KeyCount>"));
        assert!(!xml.contains("<ContinuationToken>"));
    }

    #[test]
    fn list_objects_v1_xml_with_prefix_delimiter_and_common_prefixes() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "photos/cat.jpg".to_string(),
                size: 100,
                etag: "\"aabbccdd\"".to_string(),
                last_modified: 1685000000000,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            common_prefixes: vec!["photos/2024/".to_string()],
            is_truncated: true,
            next_continuation_token: Some("photos/cat.jpg".to_string()),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v1_xml(
            "bucket",
            Some("photos/"),
            Some("/"),
            Some("a"),
            None,
            1,
            &result,
        );
        assert!(xml.contains("<Prefix>photos/</Prefix>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<Marker>a</Marker>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextMarker>photos/cat.jpg</NextMarker>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>photos/2024/</Prefix></CommonPrefixes>"));
        // V1: no KeyCount, no ContinuationToken
        assert!(!xml.contains("<KeyCount>"));
        assert!(!xml.contains("<ContinuationToken>"));
    }

    #[test]
    fn list_objects_v1_xml_empty_prefix_no_delimiter() {
        let result = ListObjectsResult {
            objects: vec![],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v1_xml("bucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Prefix></Prefix>"));
        assert!(xml.contains("<Marker></Marker>"));
        assert!(!xml.contains("<Delimiter>"));
        assert!(!xml.contains("<NextMarker>"));
    }

    #[test]
    fn list_objects_v1_xml_with_marker_and_truncation() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "key2".to_string(),
                size: 10,
                etag: "\"etag\"".to_string(),
                last_modified: 0,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            common_prefixes: vec![],
            is_truncated: true,
            next_continuation_token: Some("key2".to_string()),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v1_xml("bucket", None, None, Some("key1"), None, 1, &result);
        assert!(xml.contains("<Marker>key1</Marker>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(!xml.contains("<NextMarker>"));
    }

    #[test]
    fn days_to_date_pre_epoch() {
        // 1969-12-31 is day -1 from epoch
        let (y, m, d) = days_to_date(-1);
        assert_eq!((y, m, d), (1969, 12, 31));
    }

    #[test]
    fn days_to_date_january() {
        // 2024-01-15: mp >= 10 branch, m <= 2 branch (January)
        // 2024-01-01 = day 19723 from epoch
        // 2024-01-15 = day 19737
        let (y, m, d) = days_to_date(19737);
        assert_eq!((y, m, d), (2024, 1, 15));
    }

    #[test]
    fn days_to_date_february() {
        // 2024-02-15: m <= 2 branch (February)
        // 2024-02-15 = day 19768
        let (y, m, d) = days_to_date(19768);
        assert_eq!((y, m, d), (2024, 2, 15));
    }

    #[test]
    fn format_timestamp_january_date() {
        // 2024-01-15T12:30:45.000Z
        // days=19737, time=12*3600+30*60+45=45045
        // total seconds = 19737*86400 + 45045 = 1705321845
        let ts = format_timestamp(1705321845000);
        assert_eq!(ts, "2024-01-15T12:30:45.000Z");
    }

    #[test]
    fn xml_escape_special_chars() {
        assert_eq!(xml_escape("a&b<c>d\"e'f"), "a&amp;b&lt;c&gt;d&quot;e'f");
    }

    // ── parse_delete_objects_xml ─────────────────────────────────────

    #[test]
    fn parse_delete_objects_basic() {
        let xml =
            b"<Delete><Object><Key>key1</Key></Object><Object><Key>key2</Key></Object></Delete>";
        let (entries, quiet) = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "key1");
        assert_eq!(entries[1].key, "key2");
        assert!(!quiet);
    }

    #[test]
    fn parse_delete_objects_with_version_id() {
        let xml = b"<Delete><Object><Key>key1</Key><VersionId>v1</VersionId></Object></Delete>";
        let (entries, _) = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(entries[0].version_id.as_deref(), Some("v1"));
    }

    #[test]
    fn parse_delete_objects_with_conditional_fields() {
        let xml = b"<Delete><Object><ETag>\"etag\"</ETag><Key>key1</Key><LastModifiedTime>Tue, 15 Oct 2024 15:04:05 GMT</LastModifiedTime><Size>50</Size></Object></Delete>";
        let (entries, _) = parse_delete_objects_xml(xml).unwrap();
        assert_eq!(entries[0].etag.as_deref(), Some("\"etag\""));
        assert_eq!(
            entries[0].last_modified_time.as_deref(),
            Some("Tue, 15 Oct 2024 15:04:05 GMT")
        );
        assert_eq!(entries[0].size.as_deref(), Some("50"));
    }

    #[test]
    fn parse_delete_objects_quiet_mode() {
        let xml = b"<Delete><Quiet>true</Quiet><Object><Key>key1</Key></Object></Delete>";
        let (_, quiet) = parse_delete_objects_xml(xml).unwrap();
        assert!(quiet);
    }

    #[test]
    fn parse_delete_objects_empty_body_rejected() {
        assert!(parse_delete_objects_xml(b"").is_err());
    }

    #[test]
    fn parse_delete_objects_missing_key_rejected() {
        let xml = b"<Delete><Object><VersionId>v1</VersionId></Object></Delete>";
        assert!(parse_delete_objects_xml(xml).is_err());
    }

    #[test]
    fn parse_delete_objects_oversized_key_rejected() {
        let key = "a".repeat(1025);
        let xml = format!("<Delete><Object><Key>{key}</Key></Object></Delete>");
        match parse_delete_objects_xml(xml.as_bytes()) {
            Err(ServerError::KeyTooLongError {
                size,
                max_size_allowed,
            }) => {
                assert_eq!(size, 1025);
                assert_eq!(max_size_allowed, 1024);
            }
            other => panic!("expected KeyTooLongError, got {other:?}"),
        }
    }

    #[test]
    fn parse_delete_objects_nul_key_rejected() {
        let xml = b"<Delete><Object><Key>bad\0key</Key></Object></Delete>";
        match parse_delete_objects_xml(xml) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(reason, "object key must not contain null bytes");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn parse_delete_objects_empty_key_rejected() {
        let xml = b"<Delete><Object><Key></Key></Object></Delete>";
        match parse_delete_objects_xml(xml) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(reason, "object key must be 1-1024 bytes, got 0");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn parse_delete_objects_control_character_key_allowed() {
        let xml = b"<Delete><Object><Key>bad\x7fkey</Key></Object></Delete>";
        let (entries, quiet) = parse_delete_objects_xml(xml).unwrap();
        assert!(!quiet);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "bad\x7fkey");
    }

    // ── delete_objects_result_xml ────────────────────────────────────

    #[test]
    fn delete_result_xml_with_deletions_and_errors() {
        use crate::coordinator::{DeleteError, DeletedObject};
        let deleted = vec![DeletedObject {
            key: "key1".to_string(),
            version_id: VersionId::Null,
            delete_marker: false,
        }];
        let errors = vec![DeleteError {
            key: "key2".to_string(),
            version_id: None,
            code: "AccessDenied".to_string(),
            message: "Access Denied".to_string(),
        }];
        let xml = delete_objects_result_xml(&deleted, &errors, false);
        assert!(xml.contains("<Deleted><Key>key1</Key>"));
        assert!(!xml.contains("<VersionId>null</VersionId>"));
        assert!(xml.contains("<Error><Key>key2</Key>"));
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(xml.contains("DeleteResult"));
    }

    #[test]
    fn delete_result_xml_error_includes_version_id() {
        use crate::coordinator::DeleteError;
        let errors = vec![DeleteError {
            key: "key2".to_string(),
            version_id: Some(VersionId::from_u64(7)),
            code: "NotImplemented".to_string(),
            message: "A form field you provided implies functionality that is not implemented"
                .to_string(),
        }];
        let xml = delete_objects_result_xml(&[], &errors, false);
        assert!(xml
            .contains("<Error><Key>key2</Key><VersionId>7</VersionId><Code>NotImplemented</Code>"));
    }

    #[test]
    fn delete_result_xml_quiet_mode_omits_deleted() {
        use crate::coordinator::DeletedObject;
        let deleted = vec![DeletedObject {
            key: "key1".to_string(),
            version_id: VersionId::Null,
            delete_marker: false,
        }];
        let xml = delete_objects_result_xml(&deleted, &[], true);
        assert!(!xml.contains("<Deleted>"));
        assert!(xml.contains("DeleteResult"));
    }

    // ── list_object_versions_xml ────────────────────────────────────

    #[test]
    fn list_object_versions_xml_format() {
        use crate::coordinator::{ListObjectVersionsResult, VersionEntry};
        let result = ListObjectVersionsResult {
            versions: vec![VersionEntry {
                key: "my-key".to_string(),
                version_id: VersionId::Null,
                is_latest: true,
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
                is_delete_marker: false,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                checksum_type: Some(checksum::ChecksumType::FullObject),
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_object_versions_xml("bucket", None, None, None, 1000, &result);
        assert!(xml.contains("ListVersionsResult"));
        assert!(xml.contains("<Version>"));
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<VersionId>null</VersionId>"));
        assert!(xml.contains("<IsLatest>true</IsLatest>"));
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
        assert!(xml.contains("<Size>42</Size>"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID></Owner>",
            result.owner_canonical_id.as_str()
        )));
        assert!(!xml.contains("<DisplayName>"));
        assert!(xml.contains("<KeyMarker></KeyMarker>"));
        assert!(!xml.contains("<KeyCount>"));
    }

    #[test]
    fn list_object_versions_xml_empty() {
        use crate::coordinator::ListObjectVersionsResult;
        let result = ListObjectVersionsResult {
            versions: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_object_versions_xml("bucket", None, None, None, 1000, &result);
        assert!(xml.contains("ListVersionsResult"));
        assert!(!xml.contains("<Version>"));
    }

    #[test]
    fn list_object_versions_xml_with_prefix_and_key_marker() {
        use crate::coordinator::ListObjectVersionsResult;
        let result = ListObjectVersionsResult {
            versions: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml =
            list_object_versions_xml("bucket", Some("photos/"), Some("key1"), None, 100, &result);
        assert!(xml.contains("<Prefix>photos/</Prefix>"));
        assert!(xml.contains("<KeyMarker>key1</KeyMarker>"));
    }

    #[test]
    fn list_object_versions_xml_delete_marker_includes_owner() {
        use crate::coordinator::{ListObjectVersionsResult, VersionEntry};
        let result = ListObjectVersionsResult {
            versions: vec![VersionEntry {
                key: "gone".to_string(),
                version_id: VersionId::Null,
                is_latest: true,
                size: 0,
                etag: String::new(),
                last_modified: 1685000000000,
                is_delete_marker: true,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_object_versions_xml("bucket", None, None, None, 1000, &result);
        assert!(xml.contains("<DeleteMarker>"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID></Owner>",
            result.owner_canonical_id.as_str()
        )));
        assert!(!xml.contains("<DisplayName>"));
    }

    #[test]
    fn list_object_versions_xml_with_url_encoding() {
        use crate::coordinator::{ListObjectVersionsResult, VersionEntry};
        let result = ListObjectVersionsResult {
            versions: vec![VersionEntry {
                key: "dir/hello world&plus+".to_string(),
                version_id: VersionId::Null,
                is_latest: true,
                size: 1,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
                is_delete_marker: false,
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: true,
            next_key_marker: Some("dir/next key+".to_string()),
            next_version_id_marker: Some(VersionId::Null),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_object_versions_xml(
            "bucket",
            Some("dir/prefix here"),
            Some("dir/key marker+"),
            Some("url"),
            1000,
            &result,
        );
        assert!(xml.contains("<EncodingType>url</EncodingType>"));
        assert!(xml.contains("<Prefix>dir/prefix+here</Prefix>"));
        assert!(xml.contains("<KeyMarker>dir/key+marker%2B</KeyMarker>"));
        assert!(xml.contains("<NextKeyMarker>dir/next+key%2B</NextKeyMarker>"));
        assert!(xml.contains("<Key>dir/hello+world%26plus%2B</Key>"));
    }

    #[test]
    fn format_timestamp_basic() {
        // 2023-05-25T00:00:00.000Z = 1684972800000 ms
        let ts = format_timestamp(1684972800000);
        assert_eq!(ts, "2023-05-25T00:00:00.000Z");
    }

    #[test]
    fn format_timestamp_epoch() {
        let ts = format_timestamp(0);
        assert_eq!(ts, "1970-01-01T00:00:00.000Z");
    }

    // ── copy_object_result_xml ────────────────────────────────────

    #[test]
    fn copy_object_result_xml_format() {
        let mut system_metadata = SystemMetadata::new();
        system_metadata.set_checksum(
            ChecksumAlgorithm::Crc64nvme,
            Some(checksum::ChecksumType::FullObject),
            "AAAAAA==",
        );
        let xml = copy_object_result_xml("\"abcdef1234567890\"", 1705321845000, &system_metadata);
        assert!(xml.contains("<?xml"));
        assert!(
            xml.contains("<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
        );
        assert!(xml.contains(
            "<LastModified>2024-01-15T12:30:45.000Z</LastModified><ETag>&quot;abcdef1234567890&quot;</ETag><ChecksumCRC64NVME>AAAAAA==</ChecksumCRC64NVME><ChecksumType>FULL_OBJECT</ChecksumType>"
        ));
        assert!(xml.contains("</CopyObjectResult>"));
    }

    // ── versioning XML ──────────────────────────────────────────────

    #[test]
    fn parse_versioning_enabled() {
        let xml = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
        assert_eq!(
            parse_versioning_config_xml(xml).unwrap(),
            BucketVersioningState::Enabled
        );
    }

    #[test]
    fn parse_versioning_suspended() {
        let xml = b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
        assert_eq!(
            parse_versioning_config_xml(xml).unwrap(),
            BucketVersioningState::Suspended
        );
    }

    #[test]
    fn parse_versioning_invalid_status() {
        let xml = b"<VersioningConfiguration><Status>Invalid</Status></VersioningConfiguration>";
        assert!(parse_versioning_config_xml(xml).is_err());
    }

    #[test]
    fn parse_versioning_missing_status() {
        let xml = b"<VersioningConfiguration></VersioningConfiguration>";
        assert!(parse_versioning_config_xml(xml).is_err());
    }

    #[test]
    fn parse_versioning_missing_root_rejected() {
        let xml = b"<Status>Enabled</Status>";
        assert!(matches!(
            parse_versioning_config_xml(xml),
            Err(ServerError::MalformedXML { .. })
        ));
    }

    #[test]
    fn get_bucket_versioning_disabled() {
        let xml = get_bucket_versioning_xml(BucketVersioningState::Disabled);
        assert!(xml.contains("VersioningConfiguration"));
        assert!(!xml.contains("<Status>"));
    }

    #[test]
    fn get_bucket_versioning_enabled() {
        let xml = get_bucket_versioning_xml(BucketVersioningState::Enabled);
        assert!(xml.contains("<Status>Enabled</Status>"));
    }

    #[test]
    fn get_bucket_versioning_suspended() {
        let xml = get_bucket_versioning_xml(BucketVersioningState::Suspended);
        assert!(xml.contains("<Status>Suspended</Status>"));
    }

    #[test]
    fn parse_bucket_object_lock_configuration_days() {
        let xml = br#"
            <ObjectLockConfiguration>
              <ObjectLockEnabled>Enabled</ObjectLockEnabled>
              <Rule>
                <DefaultRetention>
                  <Mode>GOVERNANCE</Mode>
                  <Days>7</Days>
                </DefaultRetention>
              </Rule>
            </ObjectLockConfiguration>
        "#;
        let config = parse_bucket_object_lock_configuration_xml(xml).unwrap();
        assert_eq!(
            config,
            BucketObjectLockConfigurationUpdate {
                object_lock_enabled: Some(true),
                default_retention: Some(ObjectLockDefaultRetention {
                    mode: ObjectLockMode::Governance,
                    period: RetentionPeriod::days(7).unwrap(),
                }),
            }
        );
    }

    #[test]
    fn parse_bucket_object_lock_configuration_invalid_status() {
        let xml = br#"
            <ObjectLockConfiguration>
              <ObjectLockEnabled>Disabled</ObjectLockEnabled>
            </ObjectLockConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_object_lock_configuration_xml(xml),
            Err(ServerError::MalformedXML { .. })
        ));
    }

    #[test]
    fn parse_bucket_object_lock_configuration_invalid_mode() {
        let xml = br#"
            <ObjectLockConfiguration>
              <ObjectLockEnabled>Enabled</ObjectLockEnabled>
              <Rule>
                <DefaultRetention>
                  <Mode>governance</Mode>
                  <Days>1</Days>
                </DefaultRetention>
              </Rule>
            </ObjectLockConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_object_lock_configuration_xml(xml),
            Err(ServerError::MalformedXML { .. })
        ));
    }

    #[test]
    fn parse_bucket_object_lock_configuration_rejects_days_and_years() {
        let xml = br#"
            <ObjectLockConfiguration>
              <ObjectLockEnabled>Enabled</ObjectLockEnabled>
              <Rule>
                <DefaultRetention>
                  <Mode>GOVERNANCE</Mode>
                  <Days>1</Days>
                  <Years>1</Years>
                </DefaultRetention>
              </Rule>
            </ObjectLockConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_object_lock_configuration_xml(xml),
            Err(ServerError::MalformedXML { .. })
        ));
    }

    #[test]
    fn parse_bucket_object_lock_configuration_rejects_non_positive_periods() {
        let days_xml = br#"
            <ObjectLockConfiguration>
              <ObjectLockEnabled>Enabled</ObjectLockEnabled>
              <Rule>
                <DefaultRetention>
                  <Mode>GOVERNANCE</Mode>
                  <Days>0</Days>
                </DefaultRetention>
              </Rule>
            </ObjectLockConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_object_lock_configuration_xml(days_xml),
            Err(ServerError::InvalidArgument { .. })
        ));

        let years_xml = br#"
            <ObjectLockConfiguration>
              <ObjectLockEnabled>Enabled</ObjectLockEnabled>
              <Rule>
                <DefaultRetention>
                  <Mode>COMPLIANCE</Mode>
                  <Years>-1</Years>
                </DefaultRetention>
              </Rule>
            </ObjectLockConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_object_lock_configuration_xml(years_xml),
            Err(ServerError::InvalidArgument { .. })
        ));
    }

    #[test]
    fn bucket_object_lock_xml_round_trip() {
        let config = BucketObjectLockConfig {
            enabled: true,
            default_retention: Some(ObjectLockDefaultRetention {
                mode: ObjectLockMode::Compliance,
                period: RetentionPeriod::years(3).unwrap(),
            }),
        };
        let xml = get_bucket_object_lock_configuration_xml(config);
        assert!(xml.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(xml.contains("<Mode>COMPLIANCE</Mode>"));
        assert!(xml.contains("<Years>3</Years>"));
    }

    #[test]
    fn parse_object_retention_xml_accepts_retention_root() {
        let xml = br#"
            <Retention>
              <Mode>GOVERNANCE</Mode>
              <RetainUntilDate>2026-04-01T00:00:00Z</RetainUntilDate>
            </Retention>
        "#;
        assert_eq!(
            parse_object_retention_xml(xml).unwrap(),
            ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: 1_775_001_600,
            }
        );
    }

    #[test]
    fn parse_object_retention_xml_accepts_object_lock_retention_root() {
        let xml = br#"
            <ObjectLockRetention>
              <Mode>COMPLIANCE</Mode>
              <RetainUntilDate>2026-04-01T00:00:00Z</RetainUntilDate>
            </ObjectLockRetention>
        "#;
        assert_eq!(
            parse_object_retention_xml(xml).unwrap(),
            ObjectRetention {
                mode: ObjectLockMode::Compliance,
                retain_until_unix_seconds: 1_775_001_600,
            }
        );
    }

    #[test]
    fn parse_object_retention_xml_rejects_invalid_mode() {
        let xml = br#"
            <Retention>
              <Mode>governance</Mode>
              <RetainUntilDate>2026-04-01T00:00:00Z</RetainUntilDate>
            </Retention>
        "#;
        assert!(matches!(
            parse_object_retention_xml(xml),
            Err(ServerError::MalformedXML { .. })
        ));
    }

    #[test]
    fn object_retention_xml_round_trip() {
        let xml = get_object_retention_xml(Some(ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: 1_775_001_600,
        }));
        assert!(xml.contains("<Retention"));
        assert!(xml.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(xml.contains("<RetainUntilDate>2026-04-01T00:00:00.000Z</RetainUntilDate>"));
    }

    #[test]
    fn parse_object_legal_hold_xml_basic() {
        let xml = br#"
            <LegalHold>
              <Status>ON</Status>
            </LegalHold>
        "#;
        assert_eq!(
            parse_object_legal_hold_xml(xml).unwrap(),
            LegalHoldStatus::On
        );
    }

    #[test]
    fn parse_object_legal_hold_xml_rejects_invalid_status() {
        let xml = br#"
            <LegalHold>
              <Status>enabled</Status>
            </LegalHold>
        "#;
        assert!(matches!(
            parse_object_legal_hold_xml(xml),
            Err(ServerError::MalformedXML { .. })
        ));
    }

    #[test]
    fn object_legal_hold_xml_round_trip() {
        let xml = get_object_legal_hold_xml(Some(LegalHoldStatus::Off));
        assert!(xml.contains("<LegalHold"));
        assert!(xml.contains("<Status>OFF</Status>"));
    }

    #[test]
    fn parse_bucket_encryption_sse_c_blocked() {
        let xml = br#"
            <ServerSideEncryptionConfiguration>
              <Rule>
                <ApplyServerSideEncryptionByDefault>
                  <SSEAlgorithm>AES256</SSEAlgorithm>
                </ApplyServerSideEncryptionByDefault>
                <BlockedEncryptionTypes>
                  <EncryptionType>SSE-C</EncryptionType>
                </BlockedEncryptionTypes>
              </Rule>
            </ServerSideEncryptionConfiguration>
        "#;
        let config = parse_bucket_encryption_xml(xml).unwrap();
        assert!(config.sse_c_blocked);
    }

    #[test]
    fn parse_bucket_encryption_none_unblocks_sse_c() {
        let xml = br#"
            <ServerSideEncryptionConfiguration>
              <Rule>
                <BlockedEncryptionTypes>
                  <EncryptionType>NONE</EncryptionType>
                </BlockedEncryptionTypes>
              </Rule>
            </ServerSideEncryptionConfiguration>
        "#;
        let config = parse_bucket_encryption_xml(xml).unwrap();
        assert!(!config.sse_c_blocked);
    }

    #[test]
    fn parse_bucket_encryption_rejects_unsupported_algorithm() {
        let xml = br#"
            <ServerSideEncryptionConfiguration>
              <Rule>
                <ApplyServerSideEncryptionByDefault>
                  <SSEAlgorithm>aws:kms</SSEAlgorithm>
                </ApplyServerSideEncryptionByDefault>
              </Rule>
            </ServerSideEncryptionConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_encryption_xml(xml),
            Err(ServerError::NotImplemented { .. })
        ));
    }

    #[test]
    fn parse_bucket_encryption_rejects_unsupported_blocked_type() {
        let xml = br#"
            <ServerSideEncryptionConfiguration>
              <Rule>
                <BlockedEncryptionTypes>
                  <EncryptionType>aws:kms</EncryptionType>
                </BlockedEncryptionTypes>
              </Rule>
            </ServerSideEncryptionConfiguration>
        "#;
        assert!(matches!(
            parse_bucket_encryption_xml(xml),
            Err(ServerError::InvalidArgument { .. })
        ));
    }

    #[test]
    fn bucket_encryption_xml_round_trip() {
        let blocked = BucketEncryptionConfig {
            default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            sse_c_blocked: true,
        };
        let xml = get_bucket_encryption_xml(blocked.effective());
        let parsed = parse_bucket_encryption_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, blocked);
        assert!(xml.contains("<BucketKeyEnabled>false</BucketKeyEnabled>"));

        let unblocked_xml = get_bucket_encryption_xml(
            BucketEncryptionConfig {
                default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                sse_c_blocked: false,
            }
            .effective(),
        );
        assert!(!unblocked_xml.contains("<EncryptionType>"));
        assert!(unblocked_xml.contains("<SSEAlgorithm>AES256</SSEAlgorithm>"));
        assert!(unblocked_xml.contains("<BucketKeyEnabled>false</BucketKeyEnabled>"));
    }

    #[test]
    fn parse_bucket_encryption_none_normalizes_to_explicit_aes256() {
        let xml = br#"
            <ServerSideEncryptionConfiguration>
              <Rule>
                <BlockedEncryptionTypes>
                  <EncryptionType>NONE</EncryptionType>
                </BlockedEncryptionTypes>
              </Rule>
            </ServerSideEncryptionConfiguration>
        "#;
        let parsed = parse_bucket_encryption_xml(xml).unwrap();
        assert_eq!(
            parsed,
            BucketEncryptionConfig {
                default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                sse_c_blocked: false,
            }
        );
    }

    // ── CORS XML ─────────────────────────────────────────────────────

    #[test]
    fn parse_cors_config_basic() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://example.com</AllowedOrigin>\
                    <AllowedMethod>GET</AllowedMethod>\
                    <AllowedMethod>PUT</AllowedMethod>\
                    <AllowedHeader>*</AllowedHeader>\
                    <ExposeHeader>x-amz-request-id</ExposeHeader>\
                    <MaxAgeSeconds>3600</MaxAgeSeconds>\
                </CORSRule>\
            </CORSConfiguration>";
        let config = parse_cors_config_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);
        let rule = &config.rules[0];
        assert_eq!(rule.allowed_origins, vec!["http://example.com"]);
        assert_eq!(rule.allowed_methods, vec!["GET", "PUT"]);
        assert_eq!(rule.allowed_headers, vec!["*"]);
        assert_eq!(rule.expose_headers, vec!["x-amz-request-id"]);
        assert_eq!(rule.max_age_seconds, Some(3600));
    }

    #[test]
    fn parse_cors_config_multiple_rules() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://a.com</AllowedOrigin>\
                    <AllowedMethod>GET</AllowedMethod>\
                </CORSRule>\
                <CORSRule>\
                    <AllowedOrigin>http://b.com</AllowedOrigin>\
                    <AllowedMethod>POST</AllowedMethod>\
                </CORSRule>\
            </CORSConfiguration>";
        let config = parse_cors_config_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].allowed_origins, vec!["http://a.com"]);
        assert_eq!(config.rules[1].allowed_origins, vec!["http://b.com"]);
    }

    #[test]
    fn parse_cors_config_missing_origin() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedMethod>GET</AllowedMethod>\
                </CORSRule>\
            </CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_missing_method() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://example.com</AllowedOrigin>\
                </CORSRule>\
            </CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_invalid_method() {
        let xml = b"\
            <CORSConfiguration>\
                <CORSRule>\
                    <AllowedOrigin>http://example.com</AllowedOrigin>\
                    <AllowedMethod>PATCH</AllowedMethod>\
                </CORSRule>\
            </CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_no_rules() {
        let xml = b"<CORSConfiguration></CORSConfiguration>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_missing_wrapper() {
        let xml = b"<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>";
        assert!(parse_cors_config_xml(xml).is_err());
    }

    #[test]
    fn parse_cors_config_rejects_more_than_100_rules() {
        let mut xml = String::from("<CORSConfiguration>");
        for i in 0..101 {
            xml.push_str("<CORSRule><AllowedOrigin>https://");
            xml.push_str(&i.to_string());
            xml.push_str(
                ".example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>",
            );
        }
        xml.push_str("</CORSConfiguration>");
        assert!(matches!(
            parse_cors_config_xml(xml.as_bytes()),
            Err(ServerError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn parse_cors_config_rejects_over_64k_document() {
        let mut xml = String::from("<CORSConfiguration><CORSRule><AllowedOrigin>https://");
        xml.push_str(&"a".repeat(65 * 1024));
        xml.push_str(".example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>");
        assert!(matches!(
            parse_cors_config_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded { .. })
        ));
    }

    #[test]
    fn get_cors_config_xml_round_trip() {
        let config = crate::cors::CorsConfiguration {
            rules: vec![crate::cors::CorsRule {
                allowed_origins: vec!["http://example.com".into()],
                allowed_methods: vec!["GET".into(), "PUT".into()],
                allowed_headers: vec!["*".into()],
                expose_headers: vec!["x-amz-request-id".into()],
                max_age_seconds: Some(3600),
            }],
        };
        let xml = get_cors_config_xml(&config);
        assert!(
            xml.contains("<CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
        );
        assert!(xml.contains("<AllowedOrigin>http://example.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedMethod>GET</AllowedMethod>"));
        assert!(xml.contains("<AllowedMethod>PUT</AllowedMethod>"));
        assert!(xml.contains("<AllowedHeader>*</AllowedHeader>"));
        assert!(xml.contains("<ExposeHeader>x-amz-request-id</ExposeHeader>"));
        assert!(xml.contains("<MaxAgeSeconds>3600</MaxAgeSeconds>"));
        assert!(xml.contains(
            "<MaxAgeSeconds>3600</MaxAgeSeconds><ExposeHeader>x-amz-request-id</ExposeHeader><AllowedHeader>*</AllowedHeader>"
        ));

        // Parse it back
        let parsed = parse_cors_config_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed.rules.len(), 1);
        assert_eq!(parsed.rules[0].allowed_origins, vec!["http://example.com"]);
    }

    #[test]
    fn cors_xml_round_trip_multiple_origins() {
        let config = crate::cors::CorsConfiguration {
            rules: vec![crate::cors::CorsRule {
                allowed_origins: vec![
                    "http://first.com".into(),
                    "http://second.com".into(),
                    "http://*.example.com".into(),
                ],
                allowed_methods: vec!["GET".into()],
                allowed_headers: vec![],
                expose_headers: vec![],
                max_age_seconds: None,
            }],
        };
        let xml = get_cors_config_xml(&config);
        assert!(xml.contains("<AllowedOrigin>http://first.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedOrigin>http://second.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedOrigin>http://*.example.com</AllowedOrigin>"));

        let parsed = parse_cors_config_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed.rules.len(), 1);
        assert_eq!(
            parsed.rules[0].allowed_origins,
            vec![
                "http://first.com",
                "http://second.com",
                "http://*.example.com"
            ]
        );
    }

    // ── Tagging XML ─────────────────────────────────────────────────

    #[test]
    fn parse_tagging_xml_basic() {
        let xml =
            b"<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert_eq!(tags, vec![("env".to_string(), "prod".to_string())]);
    }

    #[test]
    fn parse_tagging_xml_multiple() {
        let xml = b"<Tagging><TagSet>\
            <Tag><Key>k1</Key><Value>v1</Value></Tag>\
            <Tag><Key>k2</Key><Value>v2</Value></Tag>\
            </TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], ("k1".to_string(), "v1".to_string()));
        assert_eq!(tags[1], ("k2".to_string(), "v2".to_string()));
    }

    #[test]
    fn parse_tagging_xml_empty_value() {
        let xml = b"<Tagging><TagSet><Tag><Key>k</Key><Value></Value></Tag></TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert_eq!(tags, vec![("k".to_string(), String::new())]);
    }

    #[test]
    fn parse_tagging_xml_empty_tagset() {
        let xml = b"<Tagging><TagSet></TagSet></Tagging>";
        let tags = parse_tagging_xml(xml, 10).unwrap();
        assert!(tags.is_empty());
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn parse_tagging_xml_too_many() {
        let mut xml = String::from("<Tagging><TagSet>");
        for i in 0..11 {
            xml.push_str(&format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"));
        }
        xml.push_str("</TagSet></Tagging>");
        let err = parse_tagging_xml(xml.as_bytes(), 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_key_too_long() {
        let long_key = "k".repeat(129);
        let xml = format!(
            "<Tagging><TagSet><Tag><Key>{long_key}</Key><Value>v</Value></Tag></TagSet></Tagging>"
        );
        let err = parse_tagging_xml(xml.as_bytes(), 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_value_too_long() {
        let long_val = "v".repeat(257);
        let xml = format!(
            "<Tagging><TagSet><Tag><Key>k</Key><Value>{long_val}</Value></Tag></TagSet></Tagging>"
        );
        let err = parse_tagging_xml(xml.as_bytes(), 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_duplicate_keys() {
        let xml = b"<Tagging><TagSet>\
            <Tag><Key>k</Key><Value>v1</Value></Tag>\
            <Tag><Key>k</Key><Value>v2</Value></Tag>\
            </TagSet></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_aws_prefix() {
        let xml = b"<Tagging><TagSet><Tag><Key>aws:internal</Key><Value>v</Value></Tag></TagSet></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_tagging_xml_missing_tagging_element() {
        let xml = b"<TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::MalformedXML { .. }));
    }

    #[test]
    fn parse_tagging_xml_missing_tagset_element() {
        let xml = b"<Tagging><Tag><Key>k</Key><Value>v</Value></Tag></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::MalformedXML { .. }));
    }

    #[test]
    fn parse_tagging_xml_unclosed_tag_rejected() {
        let xml = b"<Tagging><TagSet><Tag><Key>env</Key><Value>staging</Value></TagSet></Tagging>";
        let err = parse_tagging_xml(xml, 10).unwrap_err();
        assert!(matches!(err, ServerError::MalformedXML { .. }));
    }

    #[test]
    fn parse_tag_resource_xml_basic() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<TagResourceRequest xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">
  <Tags>
    <Tag><Key>security</Key><Value>public</Value></Tag>
  </Tags>
</TagResourceRequest>"#;
        let tags = parse_tag_resource_xml(xml).unwrap();
        assert_eq!(tags, vec![("security".to_string(), "public".to_string())]);
    }

    #[test]
    fn merge_tag_set_updates_in_place_and_appends_new_keys() {
        let existing = vec![
            ("env".to_string(), "prod".to_string()),
            ("tier".to_string(), "gold".to_string()),
        ];
        let updates = vec![
            ("tier".to_string(), "silver".to_string()),
            ("security".to_string(), "public".to_string()),
        ];
        assert_eq!(
            merge_tag_set(&existing, &updates),
            vec![
                ("env".to_string(), "prod".to_string()),
                ("tier".to_string(), "silver".to_string()),
                ("security".to_string(), "public".to_string()),
            ]
        );
    }

    #[test]
    fn tag_set_merge_rejects_final_bucket_tag_count_over_limit() {
        let existing = TagSet::new(
            (0..49)
                .map(|i| (format!("k{i}"), "v".to_string()))
                .collect(),
            50,
        )
        .unwrap();
        let updates = TagSet::new(
            vec![
                ("security".to_string(), "public".to_string()),
                ("env".to_string(), "prod".to_string()),
            ],
            50,
        )
        .unwrap();
        let err = existing.merge(&updates).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn remove_tag_keys_drops_only_requested_keys() {
        let existing = vec![
            ("env".to_string(), "prod".to_string()),
            ("tier".to_string(), "gold".to_string()),
            ("security".to_string(), "public".to_string()),
        ];
        let tag_keys = vec!["tier".to_string(), "missing".to_string()];
        assert_eq!(
            remove_tag_keys(&existing, &tag_keys),
            vec![
                ("env".to_string(), "prod".to_string()),
                ("security".to_string(), "public".to_string()),
            ]
        );
    }

    #[test]
    fn get_tagging_xml_round_trip() {
        let tags = vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "platform".to_string()),
        ];
        let xml = get_tagging_xml(&tags);
        let parsed = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert_eq!(parsed, tags);
    }

    #[test]
    fn get_tagging_xml_empty() {
        let xml = get_tagging_xml(&[]);
        assert!(xml.contains("<TagSet></TagSet>"));
        let parsed = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn get_tagging_xml_escapes_special_chars() {
        let tags = vec![("k&1".to_string(), "v<2>".to_string())];
        let xml = get_tagging_xml(&tags);
        assert!(xml.contains("k&amp;1"));
        assert!(xml.contains("v&lt;2&gt;"));
        let parsed = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert_eq!(parsed, tags);
    }

    // ── URL-encoded tags ────────────────────────────────────────────

    #[test]
    fn parse_url_encoded_tags_basic() {
        let tags = parse_url_encoded_tags("key1=value1&key2=value2").unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], ("key1".to_string(), "value1".to_string()));
        assert_eq!(tags[1], ("key2".to_string(), "value2".to_string()));
    }

    #[test]
    fn parse_url_encoded_tags_empty() {
        let tags = parse_url_encoded_tags("").unwrap();
        assert!(tags.is_empty());
    }

    #[test]
    fn parse_url_encoded_tags_percent_encoded() {
        let tags = parse_url_encoded_tags("k%201=v%201").unwrap();
        assert_eq!(tags[0], ("k 1".to_string(), "v 1".to_string()));
    }

    #[test]
    fn parse_url_encoded_tags_plus_as_space() {
        let tags = parse_url_encoded_tags("k+1=v+1").unwrap();
        assert_eq!(tags[0], ("k 1".to_string(), "v 1".to_string()));
    }

    #[test]
    fn parse_url_encoded_tags_too_many() {
        let input: String = (0..11)
            .map(|i| format!("k{i}=v{i}"))
            .collect::<Vec<_>>()
            .join("&");
        let err = parse_url_encoded_tags(&input).unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn parse_url_encoded_tags_empty_value() {
        let tags = parse_url_encoded_tags("k=").unwrap();
        assert_eq!(tags[0], ("k".to_string(), String::new()));
    }

    #[test]
    fn parse_url_encoded_tags_multibyte_utf8() {
        // é = U+00E9 = 0xC3 0xA9 in UTF-8
        let tags = parse_url_encoded_tags("caf%C3%A9=cr%C3%A8me").unwrap();
        assert_eq!(tags[0].0, "café");
        assert_eq!(tags[0].1, "crème");
    }

    #[test]
    fn parse_url_encoded_tags_invalid_utf8() {
        // 0xFF is not valid in any UTF-8 sequence
        let err = parse_url_encoded_tags("k=%FF").unwrap_err();
        assert!(matches!(err, ServerError::InvalidTag { .. }));
    }

    #[test]
    fn count_tags_basic() {
        let xml = get_tagging_xml(&[
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]);
        let tags = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn count_tags_empty() {
        let xml = get_tagging_xml(&[]);
        let tags = parse_tagging_xml(xml.as_bytes(), 10).unwrap();
        assert_eq!(tags.len(), 0);
    }

    // ── PublicAccessBlock XML ──────────────────────────────────────────

    #[test]
    fn parse_public_access_block_all_true() {
        let xml = b"<PublicAccessBlockConfiguration>\
            <BlockPublicAcls>true</BlockPublicAcls>\
            <IgnorePublicAcls>true</IgnorePublicAcls>\
            <BlockPublicPolicy>true</BlockPublicPolicy>\
            <RestrictPublicBuckets>true</RestrictPublicBuckets>\
            </PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(config.block_public_acls);
        assert!(config.ignore_public_acls);
        assert!(config.block_public_policy);
        assert!(config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_all_false() {
        let xml = b"<PublicAccessBlockConfiguration>\
            <BlockPublicAcls>false</BlockPublicAcls>\
            <IgnorePublicAcls>false</IgnorePublicAcls>\
            <BlockPublicPolicy>false</BlockPublicPolicy>\
            <RestrictPublicBuckets>false</RestrictPublicBuckets>\
            </PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(!config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_missing_elements_default_false() {
        let xml = b"<PublicAccessBlockConfiguration>\
            <BlockPublicAcls>true</BlockPublicAcls>\
            </PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_empty() {
        let xml = b"<PublicAccessBlockConfiguration></PublicAccessBlockConfiguration>";
        let config = parse_public_access_block_xml(xml).unwrap();
        assert!(!config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
    }

    #[test]
    fn parse_public_access_block_missing_wrapper() {
        let xml = b"<BlockPublicAcls>true</BlockPublicAcls>";
        assert!(parse_public_access_block_xml(xml).is_err());
    }

    #[test]
    fn public_access_block_xml_round_trip() {
        let config = PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: false,
        };
        let xml = get_public_access_block_xml(&config);
        let parsed = parse_public_access_block_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, config);
    }

    // ── Ownership controls XML tests ────────────────────────────────

    #[test]
    fn ownership_controls_xml_round_trip_enforced() {
        let xml = get_ownership_controls_xml(&BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
        });
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(
            parsed,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
            }
        );
    }

    #[test]
    fn ownership_controls_xml_round_trip_preferred() {
        let xml = get_ownership_controls_xml(&BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
        });
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(
            parsed,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
            }
        );
    }

    #[test]
    fn ownership_controls_xml_round_trip_object_writer() {
        let xml = get_ownership_controls_xml(&BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::ObjectWriter,
        });
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(
            parsed,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::ObjectWriter,
            }
        );
    }

    #[test]
    fn parse_ownership_controls_xml_invalid_value() {
        let xml = b"<OwnershipControls><Rule><ObjectOwnership>Invalid</ObjectOwnership></Rule></OwnershipControls>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_rejects_whitespace_padded_value() {
        let xml = b"<OwnershipControls><Rule><ObjectOwnership> BucketOwnerEnforced </ObjectOwnership></Rule></OwnershipControls>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_missing_rule() {
        let xml = b"<OwnershipControls></OwnershipControls>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_missing_root() {
        let xml = b"<Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_invalid_utf8() {
        let xml: &[u8] = &[0xFF, 0xFE];
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn bucket_abac_xml_round_trip_enabled() {
        let xml = get_bucket_abac_xml(true);
        let parsed = parse_bucket_abac_xml(xml.as_bytes()).unwrap();
        assert!(parsed);
    }

    #[test]
    fn bucket_abac_xml_round_trip_disabled() {
        let xml = get_bucket_abac_xml(false);
        let parsed = parse_bucket_abac_xml(xml.as_bytes()).unwrap();
        assert!(!parsed);
    }

    #[test]
    fn parse_bucket_abac_xml_invalid_value() {
        let xml = b"<AbacStatus><Status>Invalid</Status></AbacStatus>";
        assert!(parse_bucket_abac_xml(xml).is_err());
    }

    #[test]
    fn parse_bucket_abac_xml_missing_root() {
        let xml = b"<Status>Enabled</Status>";
        assert!(parse_bucket_abac_xml(xml).is_err());
    }

    // ── GetObjectAttributes XML tests ───────────────────────────────

    #[test]
    fn get_object_attributes_all() {
        let xml = get_object_attributes_xml(
            &["ETag", "Checksum", "StorageClass", "ObjectSize"],
            "\"abc123\"",
            1024,
            &[("x-amz-checksum-sha256", "base64hash==")],
            None,
            None,
        );
        assert!(xml.contains(
            "<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        ));
        assert!(xml.contains("<ETag>abc123</ETag>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(xml.contains("<ObjectSize>1024</ObjectSize>"));
        assert!(xml.contains("<Checksum><ChecksumSHA256>base64hash==</ChecksumSHA256></Checksum>"));
        assert!(xml.contains("</GetObjectAttributesResponse>"));
    }

    #[test]
    fn get_object_attributes_etag_only() {
        let xml = get_object_attributes_xml(&["ETag"], "\"abcdef\"", 0, &[], None, None);
        assert!(xml.contains("<ETag>abcdef</ETag>"));
        assert!(!xml.contains("<StorageClass>"));
        assert!(!xml.contains("<ObjectSize>"));
        assert!(!xml.contains("<Checksum>"));
    }

    #[test]
    fn get_object_attributes_size_only() {
        let xml = get_object_attributes_xml(&["ObjectSize"], "\"x\"", 42, &[], None, None);
        assert!(xml.contains("<ObjectSize>42</ObjectSize>"));
        assert!(!xml.contains("<ETag>"));
    }

    #[test]
    fn get_object_attributes_no_checksum_entries() {
        let xml = get_object_attributes_xml(&["Checksum"], "\"x\"", 0, &[], None, None);
        // Checksum element should be omitted when there are no checksum entries
        assert!(!xml.contains("<Checksum>"));
    }

    #[test]
    fn get_object_attributes_multiple_checksums() {
        let xml = get_object_attributes_xml(
            &["Checksum"],
            "\"x\"",
            0,
            &[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-sha256", "BBBBBB=="),
            ],
            None,
            None,
        );
        assert!(xml.contains("<ChecksumCRC32>AAAAAA==</ChecksumCRC32>"));
        assert!(xml.contains("<ChecksumSHA256>BBBBBB==</ChecksumSHA256>"));
    }

    #[test]
    fn get_object_attributes_object_parts_omitted() {
        let xml = get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], None, None);
        // ObjectParts should be omitted for non-multipart objects
        assert!(!xml.contains("<ObjectParts>"));
    }

    #[test]
    fn get_object_attributes_object_parts_rendered() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        let parts_info = ObjectPartsInfo {
            total_parts_count: 3,
            has_detail: true,
            parts: vec![
                ObjectPartEntry {
                    part_number: 1,
                    size: 5242880,
                    checksum: None,
                },
                ObjectPartEntry {
                    part_number: 2,
                    size: 1024,
                    checksum: None,
                },
            ],
            is_truncated: true,
            next_part_number_marker: Some(2),
            max_parts: 2,
            part_number_marker: 0,
        };
        let xml =
            get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], Some(&parts_info), None);
        assert!(xml.contains("<ObjectParts>"));
        assert!(xml.contains("<PartsCount>3</PartsCount>"));
        assert!(xml.contains("<PartNumberMarker>0</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>2</MaxParts>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextPartNumberMarker>2</NextPartNumberMarker>"));
        assert!(xml.contains("<Part><PartNumber>1</PartNumber><Size>5242880</Size></Part>"));
        assert!(xml.contains("<Part><PartNumber>2</PartNumber><Size>1024</Size></Part>"));
        assert!(xml.contains("</ObjectParts>"));
    }

    #[test]
    fn get_object_attributes_uses_aws_element_order() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        let parts_info = ObjectPartsInfo {
            total_parts_count: 2,
            has_detail: true,
            parts: vec![
                ObjectPartEntry {
                    part_number: 1,
                    size: 5242880,
                    checksum: Some("QoZTGg==".to_string()),
                },
                ObjectPartEntry {
                    part_number: 2,
                    size: 22,
                    checksum: Some("d7wqew==".to_string()),
                },
            ],
            is_truncated: false,
            next_part_number_marker: Some(2),
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml = get_object_attributes_xml(
            &["Checksum", "ObjectParts", "ObjectSize", "StorageClass"],
            "\"x\"",
            5242902,
            &[
                ("x-amz-checksum-crc32", "1wbLhg==-2"),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
            Some(&parts_info),
            Some(ChecksumAlgorithm::Crc32),
        );
        assert_eq!(
            xml,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<Checksum><ChecksumCRC32>1wbLhg==</ChecksumCRC32><ChecksumType>COMPOSITE</ChecksumType></Checksum>\
<ObjectParts><PartsCount>2</PartsCount><PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>2</NextPartNumberMarker><MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>5242880</Size><ChecksumCRC32>QoZTGg==</ChecksumCRC32></Part><Part><PartNumber>2</PartNumber><Size>22</Size><ChecksumCRC32>d7wqew==</ChecksumCRC32></Part></ObjectParts>\
<StorageClass>STANDARD</StorageClass><ObjectSize>5242902</ObjectSize></GetObjectAttributesResponse>"
        );
    }

    #[test]
    fn get_object_attributes_object_parts_not_truncated() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        let parts_info = ObjectPartsInfo {
            total_parts_count: 1,
            has_detail: true,
            parts: vec![ObjectPartEntry {
                part_number: 1,
                size: 100,
                checksum: None,
            }],
            is_truncated: false,
            next_part_number_marker: None,
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml =
            get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], Some(&parts_info), None);
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<NextPartNumberMarker>"));
        assert!(xml.contains("<PartsCount>1</PartsCount>"));
    }

    #[test]
    fn get_object_attributes_object_parts_no_detail() {
        use crate::coordinator::ObjectPartsInfo;
        let parts_info = ObjectPartsInfo {
            total_parts_count: 3,
            has_detail: false,
            parts: Vec::new(),
            is_truncated: false,
            next_part_number_marker: None,
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml =
            get_object_attributes_xml(&["ObjectParts"], "\"x\"", 0, &[], Some(&parts_info), None);
        assert!(xml.contains("<ObjectParts><PartsCount>3</PartsCount></ObjectParts>"));
        assert!(!xml.contains("<IsTruncated>"));
        assert!(!xml.contains("<PartNumberMarker>"));
        assert!(!xml.contains("<MaxParts>"));
        assert!(!xml.contains("<Part>"));
    }

    #[test]
    fn valid_object_attributes() {
        assert!(is_valid_object_attribute("ETag"));
        assert!(is_valid_object_attribute("Checksum"));
        assert!(is_valid_object_attribute("ObjectParts"));
        assert!(is_valid_object_attribute("StorageClass"));
        assert!(is_valid_object_attribute("ObjectSize"));
        assert!(!is_valid_object_attribute("etag"));
        assert!(!is_valid_object_attribute("Size"));
        assert!(!is_valid_object_attribute(""));
    }

    // ── Multipart XML tests ─────────────────────────────────────────

    #[test]
    fn parse_complete_multipart_basic() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag></Part>\
            <Part><PartNumber>2</PartNumber><ETag>\"def\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"abc\"");
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[1].etag, "\"def\"");
    }

    #[test]
    fn parse_complete_multipart_with_whitespace() {
        let xml = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
            <CompleteMultipartUpload>\n\
            <Part>\n\
              <PartNumber> 1 </PartNumber>\n\
              <ETag> \"etag1\" </ETag>\n\
            </Part>\n\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"etag1\"");
    }

    #[test]
    fn parse_complete_multipart_etag_without_quotes() {
        // Unquoted ETags are preserved as-is (unusual but valid)
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>abc123</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts[0].etag, "abc123");
    }

    #[test]
    fn parse_complete_multipart_empty_body() {
        let xml = b"<CompleteMultipartUpload></CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_invalid_utf8() {
        let xml = &[0xFF, 0xFE, 0x00];
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_missing_part_number() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><ETag>\"abc\"</ETag></Part>\
            </CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn parse_complete_multipart_missing_etag() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber></Part>\
            </CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    fn initiate_multipart_upload_xml_format() {
        let xml = initiate_multipart_upload_xml("mybucket", "mykey", "upload123", None, None);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<UploadId>upload123</UploadId>"));
        assert!(xml.contains("InitiateMultipartUploadResult"));
        // No checksum elements when not set.
        assert!(!xml.contains("ChecksumAlgorithm"));
        assert!(!xml.contains("ChecksumType"));
    }

    #[test]
    fn complete_multipart_upload_xml_format() {
        let xml =
            complete_multipart_upload_xml("mybucket", "mykey", "\"etag123\"", None, None, None);
        assert!(xml.contains("<Location>http://s3.amazonaws.com/mybucket/mykey</Location>"));
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<ETag>&quot;etag123&quot;</ETag>"));
        assert!(xml.contains("CompleteMultipartUploadResult"));
    }

    #[test]
    fn complete_multipart_upload_xml_location_encodes_key() {
        let xml =
            complete_multipart_upload_xml("mybucket", "path/to/my key", "\"e\"", None, None, None);
        assert!(xml.contains("mybucket/path/to/my%20key"));
    }

    #[test]
    fn complete_multipart_upload_xml_location_encodes_literal_percent() {
        // A key containing literal %20 should encode the % as %25
        let xml =
            complete_multipart_upload_xml("mybucket", "key%20name", "\"e\"", None, None, None);
        assert!(xml.contains("mybucket/key%2520name"));
    }

    #[test]
    fn complete_multipart_upload_xml_with_checksum() {
        let xml = complete_multipart_upload_xml(
            "mybucket",
            "mykey",
            "\"etag\"",
            Some(ChecksumAlgorithm::Sha256),
            Some(ChecksumType::FullObject),
            Some("abc123=="),
        );
        assert!(
            xml.contains("<ChecksumSHA256>abc123==</ChecksumSHA256>"),
            "missing checksum element: {xml}"
        );
        assert!(
            xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "missing checksum type: {xml}"
        );
    }

    #[test]
    fn parse_complete_multipart_with_part_checksums() {
        let crc32_b64 = "AAAAAA==";
        let body = b"\
            <CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag>\
            <ChecksumCRC32>AAAAAA==</ChecksumCRC32></Part>\
            <Part><PartNumber>2</PartNumber><ETag>\"e2\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(body).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0].checksum,
            Some(ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, crc32_b64).unwrap())
        );
        assert_eq!(parts[1].checksum, None);
    }

    #[test]
    fn parse_complete_multipart_multiple_checksum_elements_rejected() {
        use base64::Engine;

        let crc32_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 4]);
        let sha256_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag>\
             <ChecksumCRC32>{crc32_b64}</ChecksumCRC32>\
             <ChecksumSHA256>{sha256_b64}</ChecksumSHA256></Part>\
             </CompleteMultipartUpload>"
        );
        let err = parse_complete_multipart_upload_xml(body.as_bytes()).unwrap_err();
        assert!(
            matches!(err, ServerError::MalformedXML { .. }),
            "expected MalformedXML, got {err:?}"
        );
    }

    #[test]
    fn parse_complete_multipart_duplicate_same_checksum_element_rejected() {
        let body = b"\
            <CompleteMultipartUpload>\
            <Part><PartNumber>1</PartNumber><ETag>\"e1\"</ETag>\
            <ChecksumCRC32>AAAAAA==</ChecksumCRC32>\
            <ChecksumCRC32>AAAAAA==</ChecksumCRC32></Part>\
            </CompleteMultipartUpload>";
        let err = parse_complete_multipart_upload_xml(body).unwrap_err();
        assert!(
            matches!(err, ServerError::MalformedXML { .. }),
            "expected MalformedXML, got {err:?}"
        );
    }

    #[test]
    fn uri_encode_key_encodes_all_special_chars() {
        assert_eq!(uri_encode_key("a/b"), "a/b");
        assert_eq!(uri_encode_key("hello world"), "hello%20world");
        assert_eq!(uri_encode_key("100%"), "100%25");
        assert_eq!(uri_encode_key("a-b_c.d~e"), "a-b_c.d~e");
    }

    #[test]
    fn initiate_xml_escapes_special_chars() {
        let xml = initiate_multipart_upload_xml("my&bucket", "key<>", "id\"1", None, None);
        assert!(xml.contains("my&amp;bucket"));
        assert!(xml.contains("key&lt;&gt;"));
        assert!(xml.contains("id&quot;1"));
    }

    #[test]
    fn initiate_xml_includes_checksum_fields() {
        let xml = initiate_multipart_upload_xml("b", "k", "u", Some("CRC32"), Some("FULL_OBJECT"));
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
    }

    #[test]
    fn initiate_xml_checksum_algorithm_only() {
        let xml = initiate_multipart_upload_xml("b", "k", "u", Some("SHA256"), None);
        assert!(xml.contains("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>"));
        assert!(!xml.contains("ChecksumType"));
    }

    // ── ListMultipartUploads XML tests ───────────────────────────────

    #[test]
    fn list_multipart_uploads_xml_empty() {
        let result = RenderedListMultipartUploadsResult {
            uploads: vec![],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(!xml.contains("<Prefix"));
        assert!(xml.contains("<KeyMarker></KeyMarker>"));
        assert!(xml.contains("<UploadIdMarker></UploadIdMarker>"));
        assert!(xml.contains("<MaxUploads>1000</MaxUploads>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Upload>"));
        assert!(!xml.contains("<NextKeyMarker>"));
        assert!(!xml.contains("<NextUploadIdMarker>"));
    }

    #[test]
    fn list_multipart_uploads_xml_with_entries() {
        let result = RenderedListMultipartUploadsResult {
            uploads: vec![
                RenderedMultipartUploadEntry {
                    key: "file1.txt".to_string(),
                    upload_id: "id1".to_string(),
                    initiated: 1700000000000,
                    owner: RenderedCanonicalUser {
                        canonical_id: CanonicalUserId::from_principal("owner-1"),
                        display_name: None,
                    },
                    initiator: RenderedCanonicalUser {
                        canonical_id: CanonicalUserId::from_principal("owner-1"),
                        display_name: Some("owner-1".to_string()),
                    },
                    checksum_algorithm: None,
                    checksum_type: None,
                },
                RenderedMultipartUploadEntry {
                    key: "file2.txt".to_string(),
                    upload_id: "id2".to_string(),
                    initiated: 1700000001000,
                    owner: RenderedCanonicalUser {
                        canonical_id: CanonicalUserId::from_principal("owner-2"),
                        display_name: None,
                    },
                    initiator: RenderedCanonicalUser {
                        canonical_id: CanonicalUserId::from_principal("writer-2"),
                        display_name: Some("writer-2".to_string()),
                    },
                    checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
                    checksum_type: Some(checksum::ChecksumType::Composite),
                },
            ],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml =
            list_multipart_uploads_xml("mybucket", Some("file"), None, None, None, 1000, &result);
        assert!(xml.contains("<Prefix>file</Prefix>"));
        assert!(xml.contains("<Key>file1.txt</Key>"));
        assert!(xml.contains("<UploadId>id1</UploadId>"));
        assert!(xml.contains("<Key>file2.txt</Key>"));
        assert!(xml.contains("<UploadId>id2</UploadId>"));
        assert!(xml.contains("<Initiated>"));
        assert!(xml.contains("<Owner><ID>"));
        assert!(xml.contains("<Initiator><ID>"));
        assert!(xml.contains("<DisplayName>owner-1</DisplayName>"));
        assert!(xml.contains("<DisplayName>writer-2</DisplayName>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(xml.contains("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>COMPOSITE</ChecksumType>"));
    }

    #[test]
    fn list_multipart_uploads_xml_truncated() {
        let result = RenderedListMultipartUploadsResult {
            uploads: vec![RenderedMultipartUploadEntry {
                key: "key1".to_string(),
                upload_id: "uid1".to_string(),
                initiated: 0,
                owner: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner-1"),
                    display_name: None,
                },
                initiator: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner-1"),
                    display_name: Some("owner-1".to_string()),
                },
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: true,
            next_key_marker: Some("key1".to_string()),
            next_upload_id_marker: Some("uid1".to_string()),
        };
        let xml = list_multipart_uploads_xml(
            "mybucket",
            None,
            Some("marker"),
            Some("uid-marker"),
            None,
            1,
            &result,
        );
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<KeyMarker>marker</KeyMarker>"));
        assert!(xml.contains("<UploadIdMarker>uid-marker</UploadIdMarker>"));
        assert!(xml.contains("<NextKeyMarker>key1</NextKeyMarker>"));
        assert!(xml.contains("<NextUploadIdMarker>uid1</NextUploadIdMarker>"));
        assert!(xml.contains("<MaxUploads>1</MaxUploads>"));
    }

    #[test]
    fn list_multipart_uploads_xml_escapes_keys() {
        let result = RenderedListMultipartUploadsResult {
            uploads: vec![RenderedMultipartUploadEntry {
                key: "key&<>".to_string(),
                upload_id: "id\"'".to_string(),
                initiated: 0,
                owner: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner&<>"),
                    display_name: None,
                },
                initiator: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner&<>"),
                    display_name: Some("owner&<>".to_string()),
                },
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Key>key&amp;&lt;&gt;</Key>"));
        assert!(xml.contains("<UploadId>id&quot;'</UploadId>"));
        assert!(xml.contains("<Owner><ID>"));
        assert!(xml.contains("<DisplayName>owner&amp;&lt;&gt;</DisplayName>"));
    }

    #[test]
    fn list_multipart_uploads_xml_url_encodes_key_fields() {
        let result = RenderedListMultipartUploadsResult {
            uploads: vec![RenderedMultipartUploadEntry {
                key: "key <>&\"+".to_string(),
                upload_id: "upload-marker".to_string(),
                initiated: 0,
                owner: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner-1"),
                    display_name: None,
                },
                initiator: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner-1"),
                    display_name: Some("owner-1".to_string()),
                },
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: true,
            next_key_marker: Some("next <>&\"+".to_string()),
            next_upload_id_marker: Some("upload/id+marker".to_string()),
        };
        let xml = list_multipart_uploads_xml(
            "mybucket",
            Some("prefix <>&\"+"),
            Some("marker <>&\"+"),
            Some("upload/id+marker"),
            Some("url"),
            1,
            &result,
        );
        assert!(xml.contains("<EncodingType>url</EncodingType>"));
        assert!(xml.contains("<Prefix>prefix+%3C%3E%26%22%2B</Prefix>"));
        assert!(xml.contains("<KeyMarker>marker+%3C%3E%26%22%2B</KeyMarker>"));
        assert!(xml.contains("<NextKeyMarker>next+%3C%3E%26%22%2B</NextKeyMarker>"));
        assert!(xml.contains("<Key>key+%3C%3E%26%22%2B</Key>"));
        assert!(xml.contains("<UploadIdMarker>upload/id+marker</UploadIdMarker>"));
        assert!(xml.contains("<NextUploadIdMarker>upload/id+marker</NextUploadIdMarker>"));
    }

    // ── ListParts XML tests ──────────────────────────────────────────

    #[test]
    fn list_parts_xml_empty() {
        let result = ListPartsResult {
            parts: vec![],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: None,
            checksum_type: None,
            lifecycle_abort: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<UploadId>uid1</UploadId>"));
        assert!(xml.contains("<PartNumberMarker>0</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>1000</MaxParts>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Part>"));
    }

    #[test]
    fn list_parts_xml_with_entries() {
        use crate::coordinator::PartEntry;
        let result = ListPartsResult {
            parts: vec![
                PartEntry {
                    part_number: 1,
                    size: 5242880,
                    etag: "\"abc\"".to_string(),
                    last_modified: 1700000000000,
                    checksum: None,
                },
                PartEntry {
                    part_number: 2,
                    size: 1024,
                    etag: "\"def\"".to_string(),
                    last_modified: 1700000001000,
                    checksum: None,
                },
            ],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: None,
            checksum_type: None,
            lifecycle_abort: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<Size>5242880</Size>"));
        assert!(xml.contains("<ETag>&quot;abc&quot;</ETag>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
        assert!(xml.contains("<Size>1024</Size>"));
        assert!(xml.contains("<LastModified>"));
    }

    #[test]
    fn list_parts_xml_truncated_with_marker() {
        use crate::coordinator::PartEntry;
        let result = ListPartsResult {
            parts: vec![PartEntry {
                part_number: 3,
                size: 100,
                etag: "\"e\"".to_string(),
                last_modified: 0,
                checksum: None,
            }],
            is_truncated: true,
            next_part_number_marker: Some(3),
            checksum_algorithm: None,
            checksum_type: None,
            lifecycle_abort: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", Some(2), 1, &result);
        assert!(xml.contains("<PartNumberMarker>2</PartNumberMarker>"));
        assert!(xml.contains("<MaxParts>1</MaxParts>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextPartNumberMarker>3</NextPartNumberMarker>"));
    }

    // ── Step 5: ListParts with checksums ───────────────────────────────

    #[test]
    fn list_parts_xml_with_checksum_algorithm_and_parts() {
        use crate::coordinator::PartEntry;
        use checksum::ChecksumAlgorithm;
        let result = ListPartsResult {
            parts: vec![
                PartEntry {
                    part_number: 1,
                    size: 5242880,
                    etag: "\"abc\"".to_string(),
                    last_modified: 1700000000000,
                    checksum: Some("AAAAAA==".to_string()),
                },
                PartEntry {
                    part_number: 2,
                    size: 1024,
                    etag: "\"def\"".to_string(),
                    last_modified: 1700000001000,
                    checksum: Some("BBBBBB==".to_string()),
                },
            ],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
            checksum_type: None,
            lifecycle_abort: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        assert!(xml.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumCRC32>AAAAAA==</ChecksumCRC32>"));
        assert!(xml.contains("<ChecksumCRC32>BBBBBB==</ChecksumCRC32>"));
    }

    #[test]
    fn list_parts_xml_checksum_omitted_when_no_algorithm() {
        use crate::coordinator::PartEntry;
        let result = ListPartsResult {
            parts: vec![PartEntry {
                part_number: 1,
                size: 100,
                etag: "\"e\"".to_string(),
                last_modified: 0,
                checksum: Some("AAAAAA==".to_string()),
            }],
            is_truncated: false,
            next_part_number_marker: None,
            checksum_algorithm: None,
            checksum_type: None,
            lifecycle_abort: None,
        };
        let xml = list_parts_xml("mybucket", "mykey", "uid1", None, 1000, &result);
        // Without checksum_algorithm, per-part checksum elements should not be rendered
        assert!(!xml.contains("<ChecksumCRC32>"));
        assert!(!xml.contains("<ChecksumAlgorithm>"));
    }

    // ── Step 5: GetObjectAttributes with per-part checksums ──────────

    #[test]
    fn get_object_attributes_object_parts_with_checksums() {
        use crate::coordinator::{ObjectPartEntry, ObjectPartsInfo};
        use checksum::ChecksumAlgorithm;
        let parts_info = ObjectPartsInfo {
            total_parts_count: 2,
            has_detail: true,
            parts: vec![
                ObjectPartEntry {
                    part_number: 1,
                    size: 5242880,
                    checksum: Some("AAAAAA==".to_string()),
                },
                ObjectPartEntry {
                    part_number: 2,
                    size: 1024,
                    checksum: Some("BBBBBB==".to_string()),
                },
            ],
            is_truncated: false,
            next_part_number_marker: None,
            max_parts: 1000,
            part_number_marker: 0,
        };
        let xml = get_object_attributes_xml(
            &["ObjectParts"],
            "\"x\"",
            0,
            &[],
            Some(&parts_info),
            Some(ChecksumAlgorithm::Sha256),
        );
        assert!(xml.contains("<ChecksumSHA256>AAAAAA==</ChecksumSHA256>"));
        assert!(xml.contains("<ChecksumSHA256>BBBBBB==</ChecksumSHA256>"));
    }

    #[test]
    fn get_object_attributes_checksum_type_rendered() {
        let xml = get_object_attributes_xml(
            &["Checksum"],
            "\"x\"",
            0,
            &[
                ("x-amz-checksum-crc32", "AAAAAA=="),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
            None,
            None,
        );
        assert!(xml.contains("<Checksum>"));
        assert!(xml.contains("<ChecksumCRC32>AAAAAA==</ChecksumCRC32>"));
        assert!(xml.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"));
        assert!(xml.contains("</Checksum>"));
    }

    #[test]
    fn get_object_attributes_strips_composite_suffix() {
        let xml = get_object_attributes_xml(
            &["Checksum"],
            "\"x\"",
            0,
            &[
                (
                    "x-amz-checksum-sha256",
                    "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
                ),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
            None,
            None,
        );
        assert!(xml.contains(
            "<ChecksumSHA256>uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=</ChecksumSHA256>"
        ));
        assert!(!xml.contains("-3</ChecksumSHA256>"));
        assert!(xml.contains("<ChecksumType>COMPOSITE</ChecksumType>"));
    }

    #[test]
    fn strip_composite_suffix_works() {
        assert_eq!(strip_composite_suffix("abc=-3"), "abc=");
        assert_eq!(strip_composite_suffix("abc=-123"), "abc=");
        assert_eq!(strip_composite_suffix("abc="), "abc=");
        assert_eq!(strip_composite_suffix("plain"), "plain");
        // Empty after dash is not a suffix
        assert_eq!(strip_composite_suffix("abc=-"), "abc=-");
    }

    // ── Multipart error XML coverage ─────────────────────────────────

    #[test]
    fn error_xml_no_such_upload() {
        let xml = no_such_upload_error_xml("abc", "req-1", "host-1");
        assert!(xml.contains("<Code>NoSuchUpload</Code>"));
        assert!(xml.contains(
            "<Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>"
        ));
        assert!(xml.contains("<UploadId>abc</UploadId>"));
        assert!(xml.contains("<HostId>host-1</HostId>"));
    }

    #[test]
    fn error_xml_invalid_part() {
        let xml = complete_multipart_invalid_part_error_xml(
            "upload-1",
            3,
            "\"etag-1\"",
            "req-1",
            "host-1",
        );
        assert!(xml.contains("<Code>InvalidPart</Code>"));
        assert!(xml.contains("<UploadId>upload-1</UploadId>"));
        assert!(xml.contains("<PartNumber>3</PartNumber>"));
        assert!(xml.contains("<ETag>etag-1</ETag>"));
    }

    #[test]
    fn error_xml_invalid_part_order() {
        let xml = complete_multipart_invalid_part_order_error_xml("upload-1", "req-1", "host-1");
        assert!(xml.contains("<Code>InvalidPartOrder</Code>"));
        assert!(xml.contains("<UploadId>upload-1</UploadId>"));
    }

    #[test]
    fn error_xml_entity_too_small() {
        let xml = complete_multipart_entity_too_small_error_xml(
            100,
            5242880,
            1,
            "\"etag-1\"",
            "req-1",
            "host-1",
        );
        assert!(xml.contains("<Code>EntityTooSmall</Code>"));
        assert!(xml.contains("<ProposedSize>100</ProposedSize>"));
        assert!(xml.contains("<MinSizeAllowed>5242880</MinSizeAllowed>"));
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<ETag>etag-1</ETag>"));
    }

    #[test]
    fn generic_error_xml_entity_too_small() {
        let xml = error_xml(
            "EntityTooSmall",
            "entity too small: part 1 is 100 bytes (min 5242880)",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>EntityTooSmall</Code>"));
        assert!(xml.contains("5242880"));
    }

    // ── Parse CompleteMultipartUpload edge cases ─────────────────────

    #[test]
    #[allow(clippy::format_push_string)]
    fn parse_complete_multipart_many_parts() {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for i in 1..=100 {
            xml.push_str(&format!(
                "<Part><PartNumber>{i}</PartNumber><ETag>\"etag{i}\"</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let parts = parse_complete_multipart_upload_xml(xml.as_bytes()).unwrap();
        assert_eq!(parts.len(), 100);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[99].part_number, 100);
        assert_eq!(parts[49].etag, "\"etag50\"");
    }

    #[test]
    fn parse_complete_multipart_with_xml_declaration_and_namespace() {
        let xml = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"a\"");
    }

    #[test]
    fn parse_complete_multipart_invalid_part_number_zero() {
        // part_number=0 parses fine; validation is in coordinator
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>0</PartNumber><ETag>\"a\"</ETag></Part>\
            </CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload_xml(xml).unwrap();
        assert_eq!(parts[0].part_number, 0);
    }

    #[test]
    fn parse_complete_multipart_negative_part_number() {
        let xml = b"<CompleteMultipartUpload>\
            <Part><PartNumber>-1</PartNumber><ETag>\"a\"</ETag></Part>\
            </CompleteMultipartUpload>";
        assert!(parse_complete_multipart_upload_xml(xml).is_err());
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn ceph_cleanup_workflow() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        create_test_bucket(&coord, "test-bucket");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                object: test_object_request("test-bucket", "dir/file1.txt"),
                data: b"hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: server_core::coordinator::WriteEncryptionRequest::none(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                object: test_object_request("test-bucket", "dir/file2.txt"),
                data: b"world",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: server_core::coordinator::WriteEncryptionRequest::none(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                object: test_object_request("test-bucket", "root.txt"),
                data: b"root",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: server_core::coordinator::WriteEncryptionRequest::none(),
            },
        )
        .unwrap();

        let versions_result = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: test_bucket_request("test-bucket"),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1000,
            })
            .unwrap();
        assert_eq!(versions_result.versions.len(), 3);

        let versions_xml =
            list_object_versions_xml("test-bucket", None, None, None, 1000, &versions_result);
        assert!(versions_xml.contains("<Key>dir/file1.txt</Key>"));
        assert!(versions_xml.contains("<Key>dir/file2.txt</Key>"));
        assert!(versions_xml.contains("<Key>root.txt</Key>"));
        for _ in 0..3 {
            assert!(versions_xml.contains("<VersionId>null</VersionId>"));
        }

        let list_result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: test_bucket_request("test-bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap();
        assert_eq!(list_result.objects.len(), 3);

        let mut delete_xml = String::from("<Delete>");
        for obj in &list_result.objects {
            delete_xml.push_str(&format!("<Object><Key>{}</Key></Object>", obj.key));
        }
        delete_xml.push_str("</Delete>");

        let (xml_entries, quiet) = parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(xml_entries.len(), 3);
        assert!(!quiet);
        let entries: Vec<DeleteEntry> = xml_entries
            .iter()
            .map(|e| DeleteEntry {
                key: e.key.clone(),
                version_id: None,
                cond: DeleteCondition::None,
            })
            .collect();

        let delete_result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: test_bucket_request("test-bucket"),
                entries: &entries,
                bypass_governance: false,
            })
            .unwrap();
        assert_eq!(delete_result.deleted.len(), 3);
        assert!(delete_result.errors.is_empty());

        let list_after = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: test_bucket_request("test-bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap();
        assert!(list_after.objects.is_empty());
        coord
            .delete_bucket(&crate::coordinator::BucketRequest::new(
                storage::BucketName::try_from("test-bucket".to_string()).unwrap(),
                test_requester(),
                None,
            ))
            .unwrap();
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn ceph_cleanup_workflow_paginated() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        create_test_bucket(&coord, "bucket");
        for i in 0..5 {
            let key = format!("key-{i:02}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                    object_lock: Default::default(),
                    object: test_object_request("bucket", &key),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                    encryption: server_core::coordinator::WriteEncryptionRequest::none(),
                },
            )
            .unwrap();
        }

        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: test_bucket_request("bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.clone().unwrap();

        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: test_bucket_request("bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: Some(&token),
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        let token2 = page2.next_continuation_token.clone().unwrap();

        let page3 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: test_bucket_request("bucket"),
                prefix: None,
                delimiter: None,
                continuation_token: Some(&token2),
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);

        let all_keys: Vec<String> = page1
            .objects
            .iter()
            .chain(page2.objects.iter())
            .chain(page3.objects.iter())
            .map(|o| o.key.clone())
            .collect();
        assert_eq!(all_keys.len(), 5);

        let mut delete_xml = String::from("<Delete><Quiet>true</Quiet>");
        for key in &all_keys {
            delete_xml.push_str(&format!("<Object><Key>{key}</Key></Object>"));
        }
        delete_xml.push_str("</Delete>");

        let (xml_entries, quiet) = parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(xml_entries.len(), 5);
        assert!(quiet);
        let entries: Vec<DeleteEntry> = xml_entries
            .iter()
            .map(|e| DeleteEntry {
                key: e.key.clone(),
                version_id: None,
                cond: DeleteCondition::None,
            })
            .collect();

        let delete_result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: test_bucket_request("bucket"),
                entries: &entries,
                bypass_governance: false,
            })
            .unwrap();
        assert_eq!(delete_result.deleted.len(), 5);
        assert!(delete_result.errors.is_empty());

        let result_xml =
            delete_objects_result_xml(&delete_result.deleted, &delete_result.errors, quiet);
        assert!(!result_xml.contains("<Deleted>"));
        assert!(result_xml.contains("DeleteResult"));

        coord
            .delete_bucket(&crate::coordinator::BucketRequest::new(
                storage::BucketName::try_from("bucket".to_string()).unwrap(),
                test_requester(),
                None,
            ))
            .unwrap();
    }

    #[test]
    fn parse_versioning_config_rejects_over_1k_document() {
        let xml = format!(
            "<VersioningConfiguration>{}<Status>Enabled</Status></VersioningConfiguration>",
            " ".repeat(1025)
        );
        assert!(matches!(
            parse_versioning_config_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 1024
            })
        ));
    }

    #[test]
    fn parse_ownership_controls_rejects_over_2k_document() {
        let xml = format!(
            "<OwnershipControls>{}<Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
            " ".repeat(2049)
        );
        assert!(matches!(
            parse_ownership_controls_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 2048
            })
        ));
    }

    #[test]
    fn parse_delete_objects_rejects_over_2048000_document() {
        let xml = format!(
            "<Delete>{}<Object><Key>k</Key></Object></Delete>",
            " ".repeat(2_048_001)
        );
        assert!(matches!(
            parse_delete_objects_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 2_048_000
            })
        ));
    }

    #[test]
    fn parse_lifecycle_configuration_rejects_over_2m_document() {
        let xml = format!(
            "<LifecycleConfiguration>{}<Rule><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
            " ".repeat(2_097_153)
        );
        assert!(matches!(
            parse_bucket_lifecycle_configuration_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 2_097_152
            })
        ));
    }

    #[test]
    fn parse_tagging_xml_rejects_over_160k_document() {
        let xml = format!(
            "<Tagging><TagSet><Tag><Key>a</Key><Value>{}</Value></Tag></TagSet></Tagging>",
            "v".repeat(163_841)
        );
        assert!(matches!(
            parse_tagging_xml(xml.as_bytes(), 50),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 163_840
            })
        ));
    }

    #[test]
    fn parse_public_access_block_rejects_over_2m_document() {
        let xml = format!(
            "<PublicAccessBlockConfiguration>{}<BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>",
            " ".repeat(2_097_153)
        );
        assert!(matches!(
            parse_public_access_block_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 2_097_152
            })
        ));
    }

    #[test]
    fn parse_object_lock_configuration_rejects_over_2m_document() {
        let xml = format!(
            "<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}<ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
            " ".repeat(2_097_153)
        );
        assert!(matches!(
            parse_bucket_object_lock_configuration_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 2_097_152
            })
        ));
    }

    #[test]
    fn parse_complete_multipart_rejects_over_2621440_document() {
        let xml = format!(
            "<CompleteMultipartUpload>{}<Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>",
            " ".repeat(2_621_441)
        );
        assert!(matches!(
            parse_complete_multipart_upload_xml(xml.as_bytes()),
            Err(ServerError::MaxMessageLengthExceeded {
                max_message_length_bytes: 2_621_440
            })
        ));
    }
}
