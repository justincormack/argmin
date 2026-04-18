use super::*;

impl Requester {
    #[must_use]
    pub fn from_auth(auth: &auth::AuthContext) -> Self {
        Self {
            account: auth.account.clone(),
            authorization_profile: auth.authorization_profile,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub const fn anonymous() -> Self {
        Self {
            account: None,
            authorization_profile: auth::AuthorizationProfile::Standard,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn authenticated(account: AccountIdentity) -> Self {
        Self {
            account: Some(account),
            authorization_profile: auth::AuthorizationProfile::Standard,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_account(account: Option<&AccountIdentity>) -> Self {
        Self {
            account: account.cloned(),
            authorization_profile: auth::AuthorizationProfile::Standard,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn authenticated_owner_account_admin(account: AccountIdentity) -> Self {
        Self {
            account: Some(account),
            authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_account_owner_account_admin(account: Option<&AccountIdentity>) -> Self {
        Self {
            account: account.cloned(),
            authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn authenticated_with_profile(
        account: AccountIdentity,
        authorization_profile: auth::AuthorizationProfile,
    ) -> Self {
        Self {
            account: Some(account),
            authorization_profile,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_account_with_profile(
        account: Option<&AccountIdentity>,
        authorization_profile: auth::AuthorizationProfile,
    ) -> Self {
        Self {
            account: account.cloned(),
            authorization_profile,
        }
    }

    #[must_use]
    pub fn account(&self) -> Option<&AccountIdentity> {
        self.account.as_ref()
    }

    #[must_use]
    pub fn principal_opt(&self) -> Option<&str> {
        self.account().map(AccountIdentity::principal)
    }

    #[must_use]
    pub fn canonical_user_id(&self) -> Option<&CanonicalUserId> {
        self.account().map(AccountIdentity::canonical_user_id)
    }

    #[must_use]
    pub fn is_anonymous(&self) -> bool {
        self.account.is_none()
    }

    #[must_use]
    pub const fn authorization_profile(&self) -> auth::AuthorizationProfile {
        self.authorization_profile
    }
}

impl PutObjectAcl<'_> {
    pub(super) const fn is_public(self) -> bool {
        matches!(
            self,
            Self::PublicRead | Self::PublicReadWrite | Self::AuthenticatedRead
        )
    }

    pub(super) const fn is_supported_with_bucket_owner_enforced(self) -> bool {
        matches!(
            self,
            Self::None | Self::Private | Self::BucketOwnerRead | Self::BucketOwnerFullControl
        )
    }

    pub const fn policy_condition_value(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Private => Some("private"),
            Self::PublicRead => Some("public-read"),
            Self::PublicReadWrite => Some("public-read-write"),
            Self::AuthenticatedRead => Some("authenticated-read"),
            Self::AwsExecRead => Some("aws-exec-read"),
            Self::BucketOwnerRead => Some("bucket-owner-read"),
            Self::BucketOwnerFullControl => Some("bucket-owner-full-control"),
            Self::Invalid(_) => None,
        }
    }
}

impl<'a> From<PutObjectAcl<'a>> for PutObjectWriteAcl<'a> {
    fn from(value: PutObjectAcl<'a>) -> Self {
        match value {
            PutObjectAcl::None => Self::None,
            other => Self::Canned(other),
        }
    }
}

impl PutObjectWriteAcl<'_> {
    pub const fn policy_condition_value(&self) -> Option<&'static str> {
        match self {
            Self::None | Self::Grants(_) => None,
            Self::Canned(acl) => acl.policy_condition_value(),
        }
    }
}

pub(super) fn authorization_policy_context_for_put_object_write_acl<'a>(
    operation: &str,
    acl: &PutObjectWriteAcl<'_>,
    policy_context: PutObjectPolicyContext<'a>,
) -> Result<PutObjectPolicyContext<'a>, ServerError> {
    match acl {
        PutObjectWriteAcl::None => {
            if policy_context.canned_acl.is_some() {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} policy context cannot include canned ACL"),
                });
            }
            if parse_acl_grants_from_policy_context(policy_context)?.is_some() {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} policy context cannot include grant headers"),
                });
            }
            Ok(PutObjectPolicyContext::default())
        }
        PutObjectWriteAcl::Canned(acl) => {
            if policy_context.grant_read.is_some()
                || policy_context.grant_write.is_some()
                || policy_context.grant_read_acp.is_some()
                || policy_context.grant_write_acp.is_some()
                || policy_context.grant_full_control.is_some()
            {
                return Err(ServerError::InvalidArgument {
                    reason: format!(
                        "{operation} canned ACL policy context cannot include grant headers"
                    ),
                });
            }
            let expected_canned_acl = acl.policy_condition_value();
            if policy_context.canned_acl.is_some()
                && policy_context.canned_acl != expected_canned_acl
            {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} canned ACL policy context mismatch"),
                });
            }
            Ok(
                PutObjectPolicyContext::default()
                    .with_default_canned_acl(policy_context.canned_acl),
            )
        }
        PutObjectWriteAcl::Grants(acl_grants) => {
            if policy_context.canned_acl.is_some() {
                return Err(ServerError::InvalidArgument {
                    reason: format!("{operation} grant policy context cannot include canned ACL"),
                });
            }
            let header_grants = parse_acl_grants_from_policy_context(policy_context)?;
            if let Some(header_grants) = header_grants {
                if &header_grants != acl_grants {
                    return Err(ServerError::InvalidArgument {
                        reason: format!("{operation} grant policy context mismatch"),
                    });
                }
                Ok(PutObjectPolicyContext::default().with_acl_grant_headers(
                    policy_context.grant_read,
                    policy_context.grant_write,
                    policy_context.grant_read_acp,
                    policy_context.grant_write_acp,
                    policy_context.grant_full_control,
                ))
            } else {
                Ok(PutObjectPolicyContext::default())
            }
        }
    }
}

impl BucketAcl {
    pub(super) const fn is_public(self) -> bool {
        matches!(
            self,
            Self::PublicRead | Self::PublicReadWrite | Self::AuthenticatedRead
        )
    }

    pub const fn policy_condition_value(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AuthenticatedRead => "authenticated-read",
        }
    }
}

impl CreateBucketAcl {
    pub(super) const fn is_explicit(&self) -> bool {
        !matches!(self, Self::DefaultPrivate)
    }
}

impl<'a> PutBucketAclRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self.policy_context;
        if policy_context.copy_source.is_some()
            || policy_context.metadata_directive.is_some()
            || policy_context.managed_encryption.is_some()
            || policy_context.sse_customer_algorithm.is_some()
            || policy_context.request_object_tags_xml.is_some()
        {
            return Err(ServerError::InvalidArgument {
                reason: "PutBucketAcl policy context contains unsupported fields".to_string(),
            });
        }

        match &self.acl {
            PutBucketAclInput::Canned(acl) => {
                if policy_context.grant_read.is_some()
                    || policy_context.grant_write.is_some()
                    || policy_context.grant_read_acp.is_some()
                    || policy_context.grant_write_acp.is_some()
                    || policy_context.grant_full_control.is_some()
                {
                    return Err(ServerError::InvalidArgument {
                        reason:
                            "PutBucketAcl canned ACL policy context cannot include grant headers"
                                .to_string(),
                    });
                }
                let expected_canned_acl = Some(acl.policy_condition_value());
                if policy_context.canned_acl.is_some()
                    && policy_context.canned_acl != expected_canned_acl
                {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutBucketAcl canned ACL policy context mismatch".to_string(),
                    });
                }
                Ok(PutObjectPolicyContext::default()
                    .with_default_canned_acl(policy_context.canned_acl))
            }
            PutBucketAclInput::Grants(acl_grants) => {
                if policy_context.canned_acl.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutBucketAcl grant policy context cannot include canned ACL"
                            .to_string(),
                    });
                }
                let header_grants = parse_acl_grants_from_policy_context(policy_context)?;
                if let Some(header_grants) = header_grants {
                    if &header_grants != acl_grants {
                        return Err(ServerError::InvalidArgument {
                            reason: "PutBucketAcl grant policy context mismatch".to_string(),
                        });
                    }
                    Ok(PutObjectPolicyContext::default().with_acl_grant_headers(
                        policy_context.grant_read,
                        policy_context.grant_write,
                        policy_context.grant_read_acp,
                        policy_context.grant_write_acp,
                        policy_context.grant_full_control,
                    ))
                } else {
                    Ok(PutObjectPolicyContext::default())
                }
            }
        }
    }
}

impl PutObjectAclInput<'_> {
    pub const fn policy_condition_value(&self) -> Option<&'static str> {
        match self {
            Self::Canned(acl) => acl.policy_condition_value(),
            Self::Grants(_) => None,
        }
    }
}

impl<'a> PutObjectAclRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self.policy_context;
        if policy_context.copy_source.is_some()
            || policy_context.metadata_directive.is_some()
            || policy_context.managed_encryption.is_some()
            || policy_context.sse_customer_algorithm.is_some()
            || policy_context.request_object_tags_xml.is_some()
        {
            return Err(ServerError::InvalidArgument {
                reason: "PutObjectAcl policy context contains unsupported fields".to_string(),
            });
        }

        match &self.acl {
            PutObjectAclInput::Canned(acl) => {
                if policy_context.grant_read.is_some()
                    || policy_context.grant_write.is_some()
                    || policy_context.grant_read_acp.is_some()
                    || policy_context.grant_write_acp.is_some()
                    || policy_context.grant_full_control.is_some()
                {
                    return Err(ServerError::InvalidArgument {
                        reason:
                            "PutObjectAcl canned ACL policy context cannot include grant headers"
                                .to_string(),
                    });
                }
                let expected_canned_acl = acl.policy_condition_value();
                if policy_context.canned_acl.is_some()
                    && policy_context.canned_acl != expected_canned_acl
                {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutObjectAcl canned ACL policy context mismatch".to_string(),
                    });
                }
                Ok(PutObjectPolicyContext::default()
                    .with_default_canned_acl(policy_context.canned_acl))
            }
            PutObjectAclInput::Grants(acl_grants) => {
                if policy_context.canned_acl.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "PutObjectAcl grant policy context cannot include canned ACL"
                            .to_string(),
                    });
                }
                let header_grants = parse_acl_grants_from_policy_context(policy_context)?;
                if let Some(header_grants) = header_grants {
                    if &header_grants != acl_grants {
                        return Err(ServerError::InvalidArgument {
                            reason: "PutObjectAcl grant policy context mismatch".to_string(),
                        });
                    }
                    Ok(PutObjectPolicyContext::default().with_acl_grant_headers(
                        policy_context.grant_read,
                        policy_context.grant_write,
                        policy_context.grant_read_acp,
                        policy_context.grant_write_acp,
                        policy_context.grant_full_control,
                    ))
                } else {
                    Ok(PutObjectPolicyContext::default())
                }
            }
        }
    }
}

fn parse_put_object_acl_grant_header_value(
    value: &str,
    permission: AclPermission,
) -> Result<Vec<AclGrant>, ServerError> {
    let mut grants = Vec::new();
    let mut remaining = value.trim();
    if remaining.is_empty() {
        return Err(ServerError::InvalidArgument {
            reason: "empty ACL grant header value".to_string(),
        });
    }

    while !remaining.is_empty() {
        let (grantee_kind, rest) =
            remaining
                .split_once('=')
                .ok_or_else(|| ServerError::InvalidArgument {
                    reason: format!("invalid ACL grant header entry: {remaining}"),
                })?;
        let grantee_kind = grantee_kind.trim();
        let rest = rest.trim_start();
        let quoted = rest
            .strip_prefix('"')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        let quote_end = quoted
            .find('"')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        let grantee_value = &quoted[..quote_end];
        let next = &quoted[quote_end + 1..];
        if grantee_value.is_empty() {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            });
        }
        let grantee =
            match grantee_kind {
                "id" => AclGrantee::CanonicalUser(CanonicalUserId::new(grantee_value).ok_or_else(
                    || ServerError::InvalidArgument {
                        reason: "invalid canonical user ID in ACL grant header".to_string(),
                    },
                )?),
                "uri" => AclGrantee::parse_group_uri(grantee_value).ok_or_else(|| {
                    ServerError::InvalidArgument {
                        reason: format!("unsupported ACL group URI: {grantee_value}"),
                    }
                })?,
                other => {
                    return Err(ServerError::InvalidArgument {
                        reason: format!("unsupported ACL grant header grantee: {other}"),
                    });
                }
            };
        grants.push(AclGrant::new(grantee, permission));

        remaining = next.trim_start();
        if remaining.is_empty() {
            break;
        }
        remaining = remaining
            .strip_prefix(',')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {value}"),
            });
        }
    }

    Ok(grants)
}

fn extend_put_object_acl_grants_from_header(
    grants: &mut Vec<AclGrant>,
    value: Option<&str>,
    permission: AclPermission,
) -> Result<(), ServerError> {
    if let Some(value) = value {
        grants.extend(parse_put_object_acl_grant_header_value(value, permission)?);
    }
    Ok(())
}

fn parse_acl_grants_from_policy_context(
    policy_context: PutObjectPolicyContext<'_>,
) -> Result<Option<AclGrants>, ServerError> {
    let mut grants = Vec::new();
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_read,
        AclPermission::Read,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_write,
        AclPermission::Write,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_read_acp,
        AclPermission::ReadAcp,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_write_acp,
        AclPermission::WriteAcp,
    )?;
    extend_put_object_acl_grants_from_header(
        &mut grants,
        policy_context.grant_full_control,
        AclPermission::FullControl,
    )?;
    if grants.is_empty() {
        Ok(None)
    } else {
        Ok(Some(AclGrants::new(grants)))
    }
}

impl<'a> PutObjectRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        authorization_policy_context_for_put_object_write_acl(
            "PutObject",
            &self.acl,
            self.policy_context,
        )
    }

    pub(super) fn effective_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self
            .encryption
            .with_policy_context(self.authorization_policy_context()?);
        if policy_context.request_object_tags_xml.is_some() {
            Ok(policy_context)
        } else {
            Ok(policy_context.with_request_object_tags_xml(self.tags))
        }
    }
}

impl<'a> CreateMultipartUploadRequest<'a> {
    pub(super) fn authorization_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        authorization_policy_context_for_put_object_write_acl(
            "CreateMultipartUpload",
            &self.acl,
            self.policy_context,
        )
    }

    pub(super) fn effective_policy_context(
        &self,
    ) -> Result<PutObjectPolicyContext<'a>, ServerError> {
        let policy_context = self
            .encryption
            .with_policy_context(self.authorization_policy_context()?);
        if policy_context.request_object_tags_xml.is_some() {
            Ok(policy_context)
        } else {
            Ok(policy_context.with_request_object_tags_xml(self.tags))
        }
    }
}
