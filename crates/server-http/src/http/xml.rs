/// Hand-formatted XML for S3 responses with lightweight request XML parsing.
use crate::coordinator::{
    BucketAcl, BucketObjectLockConfigurationUpdate, BucketSummary, ChecksumClaim, CompletePart,
    DeleteError, DeletedObject, ListObjectVersionsResult, ListObjectsResult, ListPartsResult,
    ObjectPartsInfo,
};
use crate::error::ServerError;
use auth::canonical::uri_encode_path;
use checksum::ChecksumAlgorithm;
use quick_xml::{escape::unescape, events::Event, Reader};
#[cfg(test)]
use s3_types::VersionId;
use s3_types::{
    AclGrant, AclGrantee, AclGrants, AclPermission, BucketObjectLockConfig, BucketVersioningState,
    CanonicalUserId, LegalHoldStatus, ObjectLockDefaultRetention, ObjectLockMode, ObjectRetention,
    RetentionPeriod,
};
use storage::{BucketEncryptionConfig, BucketLifecycleConfiguration};

use super::response::format_version_id;

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
        xml.push_str(&xml_escape(&bucket.name));
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
    xml.push_str("</ID></");
    xml.push_str(element);
    xml.push('>');
}

/// Format a `GetBucketAcl`/`GetObjectAcl` XML response.
#[must_use]
pub fn acl_xml(
    owner_display_name: &str,
    owner_canonical_id: &CanonicalUserId,
    grants: &[RenderedAclGrant],
) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Owner><ID>",
    );
    xml.push_str(&xml_escape(owner_canonical_id.as_str()));
    xml.push_str("</ID><DisplayName>");
    xml.push_str(&xml_escape(owner_display_name));
    xml.push_str("</DisplayName></Owner><AccessControlList>");
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
            if let Some(display_name) = &grant.display_name {
                xml.push_str("<DisplayName>");
                xml.push_str(&xml_escape(display_name));
                xml.push_str("</DisplayName>");
            }
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

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.local_name().as_ref() {
                b"Grantee" => {
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
                b"ID" => current_text_field = Some(TextField::GranteeId),
                b"URI" => current_text_field = Some(TextField::GranteeUri),
                b"Permission" => current_text_field = Some(TextField::Permission),
                _ => {}
            },
            Ok(Event::Empty(e)) if e.local_name().as_ref() == b"Grantee" => {
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

    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(p));
        xml.push_str("</Prefix>");
    } else {
        xml.push_str("<Prefix/>");
    }

    if let Some(d) = delimiter {
        xml.push_str("<Delimiter>");
        xml.push_str(&xml_escape(d));
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
        xml.push_str(&xml_escape(s));
        xml.push_str("</StartAfter>");
    }

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    xml.push_str("<KeyCount>");
    xml.push_str(&(result.objects.len() + result.common_prefixes.len()).to_string());
    xml.push_str("</KeyCount>");

    if let Some(ref token) = result.next_continuation_token {
        xml.push_str("<NextContinuationToken>");
        xml.push_str(&xml_escape(token));
        xml.push_str("</NextContinuationToken>");
    }

    let owner_id = result.owner_canonical_id.as_str();
    let owner_name = result.owner_principal.as_str();

    for obj in &result.objects {
        xml.push_str("<Contents>");
        xml.push_str("<Key>");
        xml.push_str(&xml_escape(&encode_value(&obj.key, encoding_type)));
        xml.push_str("</Key>");
        xml.push_str("<LastModified>");
        xml.push_str(&format_timestamp(obj.last_modified));
        xml.push_str("</LastModified>");
        xml.push_str("<ETag>");
        xml.push_str(&xml_escape(&obj.etag));
        xml.push_str("</ETag>");
        xml.push_str("<Size>");
        xml.push_str(&obj.size.to_string());
        xml.push_str("</Size>");
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        if fetch_owner {
            xml.push_str("<Owner><ID>");
            xml.push_str(&xml_escape(owner_id));
            xml.push_str("</ID><DisplayName>");
            xml.push_str(&xml_escape(owner_name));
            xml.push_str("</DisplayName></Owner>");
        }
        xml.push_str("</Contents>");
    }

    for prefix in &result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        xml.push_str(&xml_escape(&encode_value(prefix, encoding_type)));
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

    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(p));
        xml.push_str("</Prefix>");
    } else {
        xml.push_str("<Prefix/>");
    }

    if let Some(m) = marker {
        xml.push_str("<Marker>");
        xml.push_str(&xml_escape(m));
        xml.push_str("</Marker>");
    } else {
        xml.push_str("<Marker/>");
    }

    if let Some(d) = delimiter {
        xml.push_str("<Delimiter>");
        xml.push_str(&xml_escape(d));
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

    if result.is_truncated {
        if let Some(ref token) = result.next_continuation_token {
            xml.push_str("<NextMarker>");
            xml.push_str(&xml_escape(token));
            xml.push_str("</NextMarker>");
        }
    }

    let owner_id = result.owner_canonical_id.as_str();
    let owner_name = result.owner_principal.as_str();

    for obj in &result.objects {
        xml.push_str("<Contents>");
        xml.push_str("<Key>");
        xml.push_str(&xml_escape(&encode_value(&obj.key, encoding_type)));
        xml.push_str("</Key>");
        xml.push_str("<LastModified>");
        xml.push_str(&format_timestamp(obj.last_modified));
        xml.push_str("</LastModified>");
        xml.push_str("<ETag>");
        xml.push_str(&xml_escape(&obj.etag));
        xml.push_str("</ETag>");
        xml.push_str("<Size>");
        xml.push_str(&obj.size.to_string());
        xml.push_str("</Size>");
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("<Owner><ID>");
        xml.push_str(&xml_escape(owner_id));
        xml.push_str("</ID><DisplayName>");
        xml.push_str(&xml_escape(owner_name));
        xml.push_str("</DisplayName></Owner>");
        xml.push_str("</Contents>");
    }

    for prefix in &result.common_prefixes {
        xml.push_str("<CommonPrefixes><Prefix>");
        xml.push_str(&xml_escape(&encode_value(prefix, encoding_type)));
        xml.push_str("</Prefix></CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// An entry in a `DeleteObjects` request.
#[derive(Debug)]
pub struct DeleteObjectEntry {
    pub key: String,
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
            let vid = super::response::format_version_id(d.version_id);
            xml.push_str("<Deleted><Key>");
            xml.push_str(&xml_escape(&d.key));
            xml.push_str("</Key><VersionId>");
            xml.push_str(&xml_escape(&vid));
            xml.push_str("</VersionId>");
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
    if apply_default_seen {
        match sse_algorithm.as_deref().map(str::trim) {
            Some("AES256") => {}
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
    }

    let encryption_types: Vec<&str> = encryption_types
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect();

    let sse_c_blocked = match encryption_types.as_slice() {
        [] | ["NONE"] => false,
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

    Ok(BucketEncryptionConfig { sse_c_blocked })
}

/// Format a `GetBucketEncryption` XML response.
#[must_use]
pub fn get_bucket_encryption_xml(config: BucketEncryptionConfig) -> String {
    let encryption_type = if config.sse_c_blocked {
        "SSE-C"
    } else {
        "NONE"
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule>\
         <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault>\
         <BlockedEncryptionTypes><EncryptionType>{}</EncryptionType></BlockedEncryptionTypes>\
         </Rule>\
         </ServerSideEncryptionConfiguration>",
        encryption_type
    )
}

pub fn parse_bucket_lifecycle_configuration_xml(
    data: &[u8],
) -> Result<BucketLifecycleConfiguration, ServerError> {
    storage::parse_lifecycle_configuration_xml(data).map_err(|error| match error {
        storage::LifecycleConfigError::MalformedXml { reason } => {
            ServerError::MalformedXML { reason }
        }
        storage::LifecycleConfigError::InvalidArgument { reason } => {
            ServerError::InvalidArgument { reason }
        }
        storage::LifecycleConfigError::NotImplemented { feature } => {
            ServerError::NotImplemented { feature }
        }
    })
}

#[must_use]
pub fn get_bucket_lifecycle_configuration_xml(config: &BucketLifecycleConfiguration) -> String {
    storage::render_lifecycle_configuration_xml(config)
}

/// Format a `ListVersionsResult` XML response.
#[must_use]
pub fn list_object_versions_xml(
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
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

    if let Some(p) = prefix {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(p));
        xml.push_str("</Prefix>");
    } else {
        xml.push_str("<Prefix/>");
    }

    if let Some(km) = key_marker {
        xml.push_str("<KeyMarker>");
        xml.push_str(&xml_escape(km));
        xml.push_str("</KeyMarker>");
    } else {
        xml.push_str("<KeyMarker/>");
    }

    xml.push_str("<VersionIdMarker/>");

    xml.push_str("<MaxKeys>");
    xml.push_str(&max_keys.to_string());
    xml.push_str("</MaxKeys>");

    xml.push_str("<IsTruncated>");
    xml.push_str(if result.is_truncated { "true" } else { "false" });
    xml.push_str("</IsTruncated>");

    if let Some(ref nkm) = result.next_key_marker {
        xml.push_str("<NextKeyMarker>");
        xml.push_str(&xml_escape(nkm));
        xml.push_str("</NextKeyMarker>");
    }

    if let Some(nvm) = result.next_version_id_marker {
        xml.push_str("<NextVersionIdMarker>");
        xml.push_str(&format_version_id(nvm));
        xml.push_str("</NextVersionIdMarker>");
    }

    let owner_id = result.owner_canonical_id.as_str();

    for entry in &result.versions {
        let vid = format_version_id(entry.version_id);
        let is_latest = if entry.is_latest { "true" } else { "false" };

        if entry.is_delete_marker {
            xml.push_str("<DeleteMarker>");
            xml.push_str("<Key>");
            xml.push_str(&xml_escape(&entry.key));
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
            xml.push_str(&xml_escape(&entry.key));
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
            xml.push_str("<Size>");
            xml.push_str(&entry.size.to_string());
            xml.push_str("</Size>");
            xml.push_str("<StorageClass>STANDARD</StorageClass>");
            xml.push_str("<Owner><ID>");
            xml.push_str(&xml_escape(owner_id));
            xml.push_str("</ID></Owner>");
            xml.push_str("</Version>");
        }
    }

    xml.push_str("</ListVersionsResult>");
    xml
}

fn encode_value(value: &str, encoding_type: Option<&str>) -> String {
    match encoding_type {
        Some("url") => uri_encode_path(value),
        _ => value.to_string(),
    }
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
            '\'' => out.push_str("&apos;"),
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
                            Err(malformed_cors_xml(
                                "CORS configuration must contain at most 100 rules",
                            ))
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
         <CORSConfiguration>",
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

        for header in &rule.allowed_headers {
            xml.push_str("<AllowedHeader>");
            xml.push_str(&xml_escape(header));
            xml.push_str("</AllowedHeader>");
        }

        for header in &rule.expose_headers {
            xml.push_str("<ExposeHeader>");
            xml.push_str(&xml_escape(header));
            xml.push_str("</ExposeHeader>");
        }

        if let Some(max_age) = rule.max_age_seconds {
            xml.push_str("<MaxAgeSeconds>");
            xml.push_str(&max_age.to_string());
            xml.push_str("</MaxAgeSeconds>");
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

/// Format a `CopyObjectResult` XML response.
#[must_use]
pub fn copy_object_result_xml(etag: &str, last_modified: u64) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyObjectResult>\
         <ETag>{}</ETag>\
         <LastModified>{}</LastModified>\
         </CopyObjectResult>",
        xml_escape(etag),
        format_timestamp(last_modified),
    )
}

/// Format a `CopyPartResult` XML response.
#[must_use]
pub fn copy_part_result_xml(etag: &str, last_modified: u64) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CopyPartResult>\
         <ETag>{}</ETag>\
         <LastModified>{}</LastModified>\
         </CopyPartResult>",
        xml_escape(etag),
        format_timestamp(last_modified),
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

fn date_to_days(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: u32) -> Option<u32> {
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    })
}

pub(crate) fn format_object_lock_timestamp(unix_seconds: u64) -> String {
    format_timestamp(unix_seconds.saturating_mul(1000))
}

fn parse_object_lock_timestamp_secs_with<F>(raw: &str, invalid: F) -> Result<u64, ServerError>
where
    F: Fn() -> ServerError,
{
    let raw = raw.trim();
    let datetime = raw.strip_suffix('Z').ok_or_else(&invalid)?;
    let (date, time) = datetime.split_once('T').ok_or_else(&invalid)?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts
        .next()
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let month: u32 = date_parts
        .next()
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let day: u32 = date_parts
        .next()
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if date_parts.next().is_some() {
        return Err(invalid());
    }

    let (hms, fractional) = time
        .split_once('.')
        .map_or((time, None), |(h, f)| (h, Some(f)));
    if fractional.is_some_and(|part| !part.chars().all(|c| c.is_ascii_digit())) {
        return Err(invalid());
    }
    let mut time_parts = hms.split(':');
    let hour: u32 = time_parts
        .next()
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let minute: u32 = time_parts
        .next()
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let second: u32 = time_parts
        .next()
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if time_parts.next().is_some() {
        return Err(invalid());
    }

    let max_day = days_in_month(year, month).ok_or_else(&invalid)?;
    if day == 0 || day > max_day || hour > 23 || minute > 59 || second > 59 {
        return Err(invalid());
    }

    let days = date_to_days(year, month, day);
    if days < 0 {
        return Err(invalid());
    }
    let secs = days
        .checked_mul(86_400)
        .and_then(|v| v.checked_add(i64::from(hour) * 3_600))
        .and_then(|v| v.checked_add(i64::from(minute) * 60))
        .and_then(|v| v.checked_add(i64::from(second)))
        .ok_or_else(&invalid)?;
    u64::try_from(secs).map_err(|_| invalid())
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
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Start,
        InTagging,
        InTagSet,
        InTag,
        InKey,
        InValue,
        Done,
    }

    let mut reader = Reader::from_reader(data);
    let mut buf = Vec::new();
    let mut state = State::Start;
    let mut tags = Vec::new();
    let mut seen_keys = std::collections::HashSet::new();
    let mut current_key: Option<String> = None;
    let mut current_value: Option<String> = None;
    let mut current_text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match (state, e.name().as_ref()) {
                (State::Start, b"Tagging") => state = State::InTagging,
                (State::InTagging, b"TagSet") => state = State::InTagSet,
                (State::InTagSet, b"Tag") => {
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
                _ => {
                    return Err(malformed_tagging_xml("unexpected element in tagging XML"));
                }
            },
            Ok(Event::Empty(e)) => match (state, e.name().as_ref()) {
                (State::InTagging, b"TagSet") => {}
                (State::InTagSet, b"Tag") => {
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
                (State::InTagging, b"Tagging") => state = State::Done,
                (State::InTagSet, b"TagSet") => state = State::InTagging,
                (State::InTag, b"Tag") => {
                    let key = current_key.take().ok_or(ServerError::InvalidTag {
                        reason: "missing <Key> element in <Tag>".to_string(),
                    })?;
                    let value = current_value.take().unwrap_or_default();

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
                            reason: format!(
                                "tag value must be 0-256 characters, got {value_chars}"
                            ),
                        });
                    }

                    if !seen_keys.insert(key.clone()) {
                        return Err(ServerError::InvalidTag {
                            reason: format!("duplicate tag key: {key}"),
                        });
                    }

                    tags.push((key, value));
                    if tags.len() > max_tags {
                        return Err(ServerError::InvalidTag {
                            reason: format!(
                                "tags cannot be greater than {}, got {}",
                                max_tags,
                                tags.len()
                            ),
                        });
                    }

                    state = State::InTagSet;
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
                    _ => {
                        return Err(malformed_tagging_xml("unexpected text in tagging XML"));
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = std::str::from_utf8(t.as_ref())
                    .map_err(|_| malformed_tagging_xml("invalid UTF-8 in tagging XML body"))?;
                match state {
                    State::InKey | State::InValue => current_text.push_str(text),
                    _ if text.trim().is_empty() => {}
                    _ => {
                        return Err(malformed_tagging_xml("unexpected CDATA in tagging XML"));
                    }
                }
            }
            Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_)) => {}
            Ok(Event::Eof) => {
                return match state {
                    State::Done => Ok(tags),
                    State::Start => Err(malformed_tagging_xml(
                        "missing <Tagging> element in tagging XML",
                    )),
                    State::InTagging => Err(malformed_tagging_xml(
                        "missing <TagSet> element in tagging XML",
                    )),
                    _ => Err(malformed_tagging_xml("unexpected end of tagging XML")),
                };
            }
            Err(_) => {
                return Err(malformed_tagging_xml("malformed tagging XML"));
            }
        }
        buf.clear();
    }
}

/// Serialize a list of (key, value) tag pairs into S3 tagging XML.
#[must_use]
pub fn get_tagging_xml(tags: &[(String, String)]) -> String {
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

/// Public access block configuration for a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAccessBlockConfig {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
    pub restrict_public_buckets: bool,
}

/// Parse a `<PublicAccessBlockConfiguration>` XML request body.
///
/// Missing boolean elements default to `false`.
pub fn parse_public_access_block_xml(data: &[u8]) -> Result<PublicAccessBlockConfig, ServerError> {
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
pub fn parse_ownership_controls_xml(data: &[u8]) -> Result<String, ServerError> {
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
                    State::Done => match object_ownership.as_deref().map(str::trim) {
                        Some(
                            value @ ("BucketOwnerEnforced"
                            | "BucketOwnerPreferred"
                            | "ObjectWriter"),
                        ) => Ok(value.to_string()),
                        Some(value) => Err(ServerError::InvalidArgument {
                            reason: format!("invalid ObjectOwnership value: {value}"),
                        }),
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
pub fn get_ownership_controls_xml(object_ownership: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <OwnershipControls xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ObjectOwnership>{}</ObjectOwnership></Rule>\
         </OwnershipControls>",
        xml_escape(object_ownership),
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
         <GetObjectAttributesResponse>",
    );

    for &attr in requested {
        match attr {
            "ETag" => {
                // Strip surrounding quotes from etag
                let unquoted = etag.trim_matches('"');
                xml.push_str("<ETag>");
                xml.push_str(&xml_escape(unquoted));
                xml.push_str("</ETag>");
            }
            "Checksum" => {
                if !checksum_entries.is_empty() {
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
            }
            "StorageClass" => {
                xml.push_str("<StorageClass>STANDARD</StorageClass>");
            }
            "ObjectSize" => {
                xml.push_str("<ObjectSize>");
                xml.push_str(&size.to_string());
                xml.push_str("</ObjectSize>");
            }
            "ObjectParts" => {
                if let Some(parts_info) = object_parts {
                    xml.push_str("<ObjectParts>");
                    xml.push_str("<PartsCount>");
                    xml.push_str(&parts_info.total_parts_count.to_string());
                    xml.push_str("</PartsCount>");
                    if parts_info.has_detail {
                        xml.push_str("<PartNumberMarker>");
                        xml.push_str(&parts_info.part_number_marker.to_string());
                        xml.push_str("</PartNumberMarker>");
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
                        if let Some(next) = parts_info.next_part_number_marker {
                            xml.push_str("<NextPartNumberMarker>");
                            xml.push_str(&next.to_string());
                            xml.push_str("</NextPartNumberMarker>");
                        }
                        for part in &parts_info.parts {
                            xml.push_str("<Part>");
                            xml.push_str("<PartNumber>");
                            xml.push_str(&part.part_number.to_string());
                            xml.push_str("</PartNumber>");
                            xml.push_str("<Size>");
                            xml.push_str(&part.size.to_string());
                            xml.push_str("</Size>");
                            if let (Some(algo), Some(ref val)) =
                                (checksum_algorithm, &part.checksum)
                            {
                                let elem = algo.xml_element_name();
                                xml.push_str(&format!("<{elem}>{}</{elem}>", xml_escape(val)));
                            }
                            xml.push_str("</Part>");
                        }
                    }
                    xml.push_str("</ObjectParts>");
                }
            }
            _ => {}
        }
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
            format!("<{elem}>{}</{elem}>", xml_escape(val))
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
pub fn list_multipart_uploads_xml(
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
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
        xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(p)));
    } else {
        xml.push_str("<Prefix/>");
    }
    if let Some(km) = key_marker {
        xml.push_str(&format!("<KeyMarker>{}</KeyMarker>", xml_escape(km)));
    } else {
        xml.push_str("<KeyMarker/>");
    }
    if let Some(um) = upload_id_marker {
        xml.push_str(&format!(
            "<UploadIdMarker>{}</UploadIdMarker>",
            xml_escape(um)
        ));
    } else {
        xml.push_str("<UploadIdMarker/>");
    }
    xml.push_str(&format!("<MaxUploads>{max_uploads}</MaxUploads>"));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    if let Some(ref nkm) = result.next_key_marker {
        xml.push_str(&format!(
            "<NextKeyMarker>{}</NextKeyMarker>",
            xml_escape(nkm)
        ));
    }
    if let Some(ref num) = result.next_upload_id_marker {
        xml.push_str(&format!(
            "<NextUploadIdMarker>{}</NextUploadIdMarker>",
            xml_escape(num)
        ));
    }
    for upload in &result.uploads {
        xml.push_str("<Upload>");
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
        xml.push_str(&format!(
            "<Initiated>{}</Initiated>",
            format_timestamp(upload.initiated)
        ));
        append_canonical_owner_xml(&mut xml, "Initiator", &upload.initiator);
        xml.push_str(&format!("<Key>{}</Key>", xml_escape(&upload.key)));
        append_canonical_owner_xml(&mut xml, "Owner", &upload.owner);
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str(&format!(
            "<UploadId>{}</UploadId>",
            xml_escape(&upload.upload_id)
        ));
        xml.push_str("</Upload>");
    }
    xml.push_str("</ListMultipartUploadsResult>");
    xml
}

/// Format a `ListPartsResult` XML response.
#[must_use]
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
                (State::InPart, name) => {
                    if checksum_algorithm_for_element(name).is_some() {
                    } else {
                        return Err(malformed_complete_multipart_xml());
                    }
                }
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
    use server_core::system_metadata::SystemMetadata;
    use std::sync::Arc;
    use storage::SharedStorageNode;

    fn setup_coordinator(dir: &std::path::Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        Coordinator::new(storage_node, ec_config, "us-east-1".to_string(), None).unwrap()
    }

    const NO_WRITE: &WriteCondition = &WriteCondition::None;
    const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

    fn test_requester() -> Requester {
        test_helpers::requester("default-owner")
    }

    fn test_bucket_request(name: &str) -> crate::coordinator::BucketRequest<'_> {
        crate::coordinator::BucketRequest::new(name, test_requester(), None)
    }

    fn test_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
    ) -> crate::coordinator::ObjectRequest<'a> {
        crate::coordinator::ObjectRequest::new(bucket, key, test_requester(), None)
    }

    fn create_test_bucket(coord: &Coordinator, name: &str) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name,
                requester: test_requester(),
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
            name: "test-bucket".to_string(),
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
            encryption: BucketEncryptionConfig::default(),
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
        assert!(xml.contains("<DisplayName>owner</DisplayName>"));
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
    fn list_objects_xml_format() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "my-key".to_string(),
                size: 42,
                etag: "\"abc123\"".to_string(),
                last_modified: 1685000000000,
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
            "<Owner><ID>{}</ID><DisplayName>owner</DisplayName></Owner>",
            result.owner_canonical_id.as_str()
        )));
    }

    #[test]
    fn list_objects_xml_with_prefix_delimiter_and_truncation() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "photos/cat.jpg".to_string(),
                size: 100,
                etag: "\"aabbccdd\"".to_string(),
                last_modified: 1685000000000,
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
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextContinuationToken>photos/cat.jpg</NextContinuationToken>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>photos/2024/</Prefix></CommonPrefixes>"));
    }

    #[test]
    fn list_objects_xml_key_count_includes_prefixes() {
        let result = ListObjectsResult {
            objects: vec![ListEntry {
                key: "root.txt".to_string(),
                size: 10,
                etag: "\"abc\"".to_string(),
                last_modified: 0,
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
        // With prefix=None → should produce <Prefix/>
        let xml = list_objects_v2_xml("bucket", None, None, None, None, None, false, 1000, &result);
        assert!(xml.contains("<Prefix/>"));
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
            }],
            common_prefixes: vec![],
            is_truncated: false,
            next_continuation_token: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v1_xml("bucket", None, None, None, None, 1000, &result);
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<Marker/>"));
        assert!(xml.contains("ListBucketResult"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID><DisplayName>owner</DisplayName></Owner>",
            result.owner_canonical_id.as_str()
        )));
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
        assert!(xml.contains("<Prefix/>"));
        assert!(xml.contains("<Marker/>"));
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
            }],
            common_prefixes: vec![],
            is_truncated: true,
            next_continuation_token: Some("key2".to_string()),
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_objects_v1_xml("bucket", None, None, Some("key1"), None, 1, &result);
        assert!(xml.contains("<Marker>key1</Marker>"));
        assert!(xml.contains("<NextMarker>key2</NextMarker>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
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
        assert_eq!(
            xml_escape("a&b<c>d\"e'f"),
            "a&amp;b&lt;c&gt;d&quot;e&apos;f"
        );
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
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_object_versions_xml("bucket", None, None, 1000, &result);
        assert!(xml.contains("ListVersionsResult"));
        assert!(xml.contains("<Version>"));
        assert!(xml.contains("<Key>my-key</Key>"));
        assert!(xml.contains("<VersionId>null</VersionId>"));
        assert!(xml.contains("<IsLatest>true</IsLatest>"));
        assert!(xml.contains("<Size>42</Size>"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID></Owner>",
            result.owner_canonical_id.as_str()
        )));
        assert!(!xml.contains("<DisplayName>"));
        assert!(xml.contains("<KeyMarker/>"));
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
        let xml = list_object_versions_xml("bucket", None, None, 1000, &result);
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
        let xml = list_object_versions_xml("bucket", Some("photos/"), Some("key1"), 100, &result);
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
            }],
            is_truncated: false,
            next_key_marker: None,
            next_version_id_marker: None,
            owner_principal: "owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal("owner"),
        };
        let xml = list_object_versions_xml("bucket", None, None, 1000, &result);
        assert!(xml.contains("<DeleteMarker>"));
        assert!(xml.contains(&format!(
            "<Owner><ID>{}</ID></Owner>",
            result.owner_canonical_id.as_str()
        )));
        assert!(!xml.contains("<DisplayName>"));
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
        let xml = copy_object_result_xml("\"abcdef1234567890\"", 1705321845000);
        assert!(xml.contains("<?xml"));
        assert!(xml.contains("<CopyObjectResult>"));
        assert!(xml.contains("<ETag>&quot;abcdef1234567890&quot;</ETag>"));
        assert!(xml.contains("<LastModified>2024-01-15T12:30:45.000Z</LastModified>"));
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
              <RetainUntilDate>2026-04-01T00:00:00.000Z</RetainUntilDate>
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
            sse_c_blocked: true,
        };
        let xml = get_bucket_encryption_xml(blocked);
        let parsed = parse_bucket_encryption_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, blocked);

        let unblocked_xml = get_bucket_encryption_xml(BucketEncryptionConfig {
            sse_c_blocked: false,
        });
        assert!(unblocked_xml.contains("<EncryptionType>NONE</EncryptionType>"));
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
        assert!(xml.contains("<CORSConfiguration>"));
        assert!(xml.contains("<AllowedOrigin>http://example.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedMethod>GET</AllowedMethod>"));
        assert!(xml.contains("<AllowedMethod>PUT</AllowedMethod>"));
        assert!(xml.contains("<AllowedHeader>*</AllowedHeader>"));
        assert!(xml.contains("<ExposeHeader>x-amz-request-id</ExposeHeader>"));
        assert!(xml.contains("<MaxAgeSeconds>3600</MaxAgeSeconds>"));

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
        let xml = get_ownership_controls_xml("BucketOwnerEnforced");
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, "BucketOwnerEnforced");
    }

    #[test]
    fn ownership_controls_xml_round_trip_preferred() {
        let xml = get_ownership_controls_xml("BucketOwnerPreferred");
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, "BucketOwnerPreferred");
    }

    #[test]
    fn ownership_controls_xml_round_trip_object_writer() {
        let xml = get_ownership_controls_xml("ObjectWriter");
        let parsed = parse_ownership_controls_xml(xml.as_bytes()).unwrap();
        assert_eq!(parsed, "ObjectWriter");
    }

    #[test]
    fn parse_ownership_controls_xml_invalid_value() {
        let xml = b"<OwnershipControls><Rule><ObjectOwnership>Invalid</ObjectOwnership></Rule></OwnershipControls>";
        assert!(parse_ownership_controls_xml(xml).is_err());
    }

    #[test]
    fn parse_ownership_controls_xml_trims_value() {
        let xml = b"<OwnershipControls><Rule><ObjectOwnership> BucketOwnerEnforced </ObjectOwnership></Rule></OwnershipControls>";
        let parsed = parse_ownership_controls_xml(xml).unwrap();
        assert_eq!(parsed, "BucketOwnerEnforced");
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
        assert!(xml.contains("<GetObjectAttributesResponse>"));
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
        let xml = complete_multipart_upload_xml("mybucket", "mykey", "\"etag123\"", None, None);
        assert!(xml.contains("<Location>http://s3.amazonaws.com/mybucket/mykey</Location>"));
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Key>mykey</Key>"));
        assert!(xml.contains("<ETag>&quot;etag123&quot;</ETag>"));
        assert!(xml.contains("CompleteMultipartUploadResult"));
    }

    #[test]
    fn complete_multipart_upload_xml_location_encodes_key() {
        let xml = complete_multipart_upload_xml("mybucket", "path/to/my key", "\"e\"", None, None);
        assert!(xml.contains("mybucket/path/to/my%20key"));
    }

    #[test]
    fn complete_multipart_upload_xml_location_encodes_literal_percent() {
        // A key containing literal %20 should encode the % as %25
        let xml = complete_multipart_upload_xml("mybucket", "key%20name", "\"e\"", None, None);
        assert!(xml.contains("mybucket/key%2520name"));
    }

    #[test]
    fn complete_multipart_upload_xml_with_checksum() {
        let xml = complete_multipart_upload_xml(
            "mybucket",
            "mykey",
            "\"etag\"",
            Some(ChecksumAlgorithm::Sha256),
            Some("abc123=="),
        );
        assert!(
            xml.contains("<ChecksumSHA256>abc123==</ChecksumSHA256>"),
            "missing checksum element: {xml}"
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
        let xml = list_multipart_uploads_xml("mybucket", None, None, None, 1000, &result);
        assert!(xml.contains("<Bucket>mybucket</Bucket>"));
        assert!(xml.contains("<Prefix/>"));
        assert!(xml.contains("<KeyMarker/>"));
        assert!(xml.contains("<UploadIdMarker/>"));
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
                    },
                    initiator: RenderedCanonicalUser {
                        canonical_id: CanonicalUserId::from_principal("owner-1"),
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
                    },
                    initiator: RenderedCanonicalUser {
                        canonical_id: CanonicalUserId::from_principal("writer-2"),
                    },
                    checksum_algorithm: Some(ChecksumAlgorithm::Sha256),
                    checksum_type: Some(checksum::ChecksumType::Composite),
                },
            ],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", Some("file"), None, None, 1000, &result);
        assert!(xml.contains("<Prefix>file</Prefix>"));
        assert!(xml.contains("<Key>file1.txt</Key>"));
        assert!(xml.contains("<UploadId>id1</UploadId>"));
        assert!(xml.contains("<Key>file2.txt</Key>"));
        assert!(xml.contains("<UploadId>id2</UploadId>"));
        assert!(xml.contains("<Initiated>"));
        assert!(xml.contains("<Owner><ID>"));
        assert!(xml.contains("<Initiator><ID>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(xml.contains("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>"));
        assert!(xml.contains("<ChecksumType>COMPOSITE</ChecksumType>"));
        assert!(!xml.contains("<DisplayName>"));
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
                },
                initiator: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner-1"),
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
                },
                initiator: RenderedCanonicalUser {
                    canonical_id: CanonicalUserId::from_principal("owner&<>"),
                },
                checksum_algorithm: None,
                checksum_type: None,
            }],
            is_truncated: false,
            next_key_marker: None,
            next_upload_id_marker: None,
        };
        let xml = list_multipart_uploads_xml("mybucket", None, None, None, 1000, &result);
        assert!(xml.contains("<Key>key&amp;&lt;&gt;</Key>"));
        assert!(xml.contains("<UploadId>id&quot;&apos;</UploadId>"));
        assert!(xml.contains("<Owner><ID>"));
        assert!(!xml.contains("<DisplayName>"));
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
        let xml = error_xml(
            "NoSuchUpload",
            "no such upload: abc",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>NoSuchUpload</Code>"));
        assert!(xml.contains("<Message>no such upload: abc</Message>"));
    }

    #[test]
    fn error_xml_invalid_part() {
        let xml = error_xml(
            "InvalidPart",
            "invalid part: part 3",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>InvalidPart</Code>"));
    }

    #[test]
    fn error_xml_invalid_part_order() {
        let xml = error_xml(
            "InvalidPartOrder",
            "invalid part order",
            "/bucket/key",
            "req-1",
        );
        assert!(xml.contains("<Code>InvalidPartOrder</Code>"));
    }

    #[test]
    fn error_xml_entity_too_small() {
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
    fn ceph_cleanup_workflow() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        create_test_bucket(&coord, "test-bucket");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                sse_customer: None,
                policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                object: test_object_request("test-bucket", "dir/file1.txt"),
                data: b"hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                sse_customer: None,
                policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                object: test_object_request("test-bucket", "dir/file2.txt"),
                data: b"world",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                sse_customer: None,
                policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                object: test_object_request("test-bucket", "root.txt"),
                data: b"root",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
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
            list_object_versions_xml("test-bucket", None, None, 1000, &versions_result);
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
                key: &e.key,
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
            .delete_bucket(&crate::coordinator::BucketRequest {
                name: "test-bucket",
                requester: test_requester(),
                expected_bucket_owner: None,
            })
            .unwrap();
    }

    #[test]
    fn ceph_cleanup_workflow_paginated() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        create_test_bucket(&coord, "bucket");
        for i in 0..5 {
            let key = format!("key-{i:02}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    sse_customer: None,
                    policy_context: server_core::coordinator::PutObjectPolicyContext::default(),
                    object_lock: Default::default(),
                    object: test_object_request("bucket", &key),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
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
                key: &e.key,
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
            .delete_bucket(&crate::coordinator::BucketRequest {
                name: "bucket",
                requester: test_requester(),
                expected_bucket_owner: None,
            })
            .unwrap();
    }
}
