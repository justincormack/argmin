use s3_types::{
    AclGrants, LegalHoldStatus, ObjectLockDefaultRetention, ObjectLockMode, ObjectRetention,
    RetentionPeriod, StoredLegalHoldStatus, VersionId,
};
use storage::{BucketName, ObjectKey, ObjectLockState};

use super::authz::BucketPolicyAccess;
use super::{
    BucketSummary, Coordinator, GetObjectAclResult, ObjectVersionRequest, PutObjectAclInput,
    PutObjectAclRequest, PutObjectLegalHoldRequest, PutObjectRetentionRequest,
    PutObjectTagsRequest, TRACE_TARGET,
};
use crate::error::ServerError;

struct ObjectMetadataPolicyContext {
    bucket_info: BucketSummary,
    bucket_policy: Option<std::sync::Arc<auth::BucketPolicy>>,
    bucket_tags: Option<Vec<(String, String)>>,
}

impl Coordinator {
    fn load_object_metadata_policy_context(
        &self,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ObjectMetadataPolicyContext, ServerError> {
        let bucket =
            self.load_bucket_handle_for_object_policy_read(bucket, expected_bucket_owner)?;
        let bucket_info = bucket.bucket().clone();
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        Ok(ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        })
    }

    fn map_object_metadata_access_error(
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        can_discover_missing: bool,
        error: storage::ObjectPgActionError,
    ) -> ServerError {
        Self::map_object_read_snapshot_error(bucket, key, version_id, can_discover_missing, error)
    }

    pub(super) fn ensure_object_lock_bucket(bucket: &BucketSummary) -> Result<(), ServerError> {
        if bucket.object_lock.enabled {
            Ok(())
        } else {
            Err(ServerError::InvalidRequest {
                reason: "Bucket is missing Object Lock Configuration".to_string(),
            })
        }
    }

    fn requested_object_lock_present(state: ObjectLockState) -> bool {
        state.retention.is_some() || state.legal_hold != StoredLegalHoldStatus::NotSet
    }

    fn is_leap_year(year: i64) -> bool {
        (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
    }

    fn days_in_month(year: i64, month: u32) -> Option<u32> {
        Some(match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if Self::is_leap_year(year) => 29,
            2 => 28,
            _ => return None,
        })
    }

    fn days_to_date(days: i64) -> (i64, u32, u32) {
        let z = days + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let mut year = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = mp + if mp < 10 { 3 } else { -9 };
        if month <= 2 {
            year += 1;
        }
        (year, month as u32, day as u32)
    }

    fn date_to_days(year: i64, month: u32, day: u32) -> i64 {
        let adjust = if month <= 2 { 1 } else { 0 };
        let y = year - adjust;
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let month_i = i64::from(month);
        let day_i = i64::from(day);
        let doy = (153 * (month_i + if month > 2 { -3 } else { 9 }) + 2) / 5 + day_i - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }

    pub(super) fn current_unix_seconds() -> Result<u64, ServerError> {
        if let Some(now_millis) = storage::clock::override_time_millis() {
            return Ok(now_millis / 1000);
        }
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .map_err(|_| ServerError::InternalError {
                reason: "system clock is before the Unix epoch".to_string(),
            })
    }

    fn default_retention_deadline(
        default_retention: ObjectLockDefaultRetention,
        created_at_unix_seconds: u64,
    ) -> Result<u64, ServerError> {
        match default_retention.period {
            RetentionPeriod::Days(days) => created_at_unix_seconds
                .checked_add(u64::from(days.get()) * 86_400)
                .ok_or_else(|| ServerError::InternalError {
                    reason: "default Object Lock retention overflowed".to_string(),
                }),
            RetentionPeriod::Years(years) => {
                let days = created_at_unix_seconds / 86_400;
                let seconds_of_day = created_at_unix_seconds % 86_400;
                let (year, month, day) = Self::days_to_date(days as i64);
                let target_year = year.checked_add(i64::from(years.get())).ok_or_else(|| {
                    ServerError::InternalError {
                        reason: "default Object Lock retention overflowed".to_string(),
                    }
                })?;
                let target_day =
                    day.min(Self::days_in_month(target_year, month).ok_or_else(|| {
                        ServerError::InternalError {
                            reason: "invalid month while applying default Object Lock retention"
                                .to_string(),
                        }
                    })?);
                let target_days = Self::date_to_days(target_year, month, target_day);
                let target_days =
                    u64::try_from(target_days).map_err(|_| ServerError::InternalError {
                        reason: "default Object Lock retention underflowed".to_string(),
                    })?;
                target_days
                    .checked_mul(86_400)
                    .and_then(|seconds| seconds.checked_add(seconds_of_day))
                    .ok_or_else(|| ServerError::InternalError {
                        reason: "default Object Lock retention overflowed".to_string(),
                    })
            }
        }
    }

    pub(super) fn validate_requested_object_lock_state(
        bucket: &BucketSummary,
        requested: ObjectLockState,
    ) -> Result<(), ServerError> {
        if !bucket.object_lock.enabled && Self::requested_object_lock_present(requested) {
            return Err(ServerError::InvalidRequest {
                reason: "Bucket is missing Object Lock Configuration".to_string(),
            });
        }
        if let Some(retention) = requested.retention {
            if retention.retain_until_unix_seconds <= Self::current_unix_seconds()? {
                return Err(ServerError::InvalidArgument {
                    reason: "The retain until date must be in the future!".to_string(),
                });
            }
        }
        Ok(())
    }

    pub(super) fn resolve_new_object_lock_state(
        bucket: &BucketSummary,
        requested: ObjectLockState,
    ) -> Result<ObjectLockState, ServerError> {
        Self::validate_requested_object_lock_state(bucket, requested)?;
        if !bucket.object_lock.enabled {
            return Ok(ObjectLockState::default());
        }

        let mut resolved = requested;
        if resolved.retention.is_none() {
            if let Some(default_retention) = bucket.object_lock.default_retention {
                resolved.retention = Some(ObjectRetention {
                    mode: default_retention.mode,
                    retain_until_unix_seconds: Self::default_retention_deadline(
                        default_retention,
                        Self::current_unix_seconds()?,
                    )?,
                });
            }
        }
        Ok(resolved)
    }

    pub(super) fn validate_retention_update(
        current: Option<ObjectRetention>,
        requested: ObjectRetention,
        bypass_governance_requested: bool,
        can_bypass_governance: bool,
    ) -> Result<(), ServerError> {
        let Some(current) = current else {
            return Ok(());
        };

        match current.mode {
            ObjectLockMode::Governance => {
                let shortens =
                    requested.retain_until_unix_seconds < current.retain_until_unix_seconds;
                let changes_mode = requested.mode != current.mode;
                if shortens || changes_mode {
                    if bypass_governance_requested && can_bypass_governance {
                        return Ok(());
                    }
                    return Err(ServerError::AccessDenied);
                }
                Ok(())
            }
            ObjectLockMode::Compliance => {
                if requested.mode != ObjectLockMode::Compliance {
                    return Err(ServerError::AccessDenied);
                }
                if requested.retain_until_unix_seconds < current.retain_until_unix_seconds {
                    return Err(ServerError::AccessDenied);
                }
                Ok(())
            }
        }
    }

    pub(super) fn validate_delete_against_object_lock(
        object_lock: ObjectLockState,
        bypass_governance_requested: bool,
        can_bypass_governance: bool,
        now_unix_seconds: u64,
    ) -> Result<(), ServerError> {
        if object_lock.legal_hold == StoredLegalHoldStatus::On {
            return Err(ServerError::AccessDenied);
        }

        let Some(retention) = object_lock.retention else {
            return Ok(());
        };
        if retention.retain_until_unix_seconds <= now_unix_seconds {
            return Ok(());
        }

        match retention.mode {
            ObjectLockMode::Governance if bypass_governance_requested && can_bypass_governance => {
                Ok(())
            }
            ObjectLockMode::Governance | ObjectLockMode::Compliance => {
                Err(ServerError::AccessDenied)
            }
        }
    }

    pub fn put_object_tags(&self, req: &PutObjectTagsRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_object_tags",
            "bucket={:?} key={:?} bytes={}",
            req.object.bucket_name(),
            req.object.key(),
            req.tags.len()
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.object.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        self.storage_node
            .put_object_tags_if(bucket, key, req.object.version_id, req.tags, |stored| {
                if !self.requester_can_manage_object_tags_with_bucket_policy(
                    BucketPolicyAccess {
                        requester: req.object.requester(),
                        bucket: &bucket_info,
                        bucket_tags: bucket_tags.as_deref(),
                        policy: bucket_policy.as_deref(),
                    },
                    stored,
                    Self::put_object_tagging_policy_action(req.object.version_id),
                    Some(req.tags),
                )? {
                    return Err(ServerError::AccessDenied);
                }
                if stored.is_delete_marker() {
                    return Err(ServerError::MethodNotAllowed);
                }
                Ok(stored.version_id())
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.object.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok(())
    }

    pub fn put_object_retention(
        &self,
        req: &PutObjectRetentionRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_object_retention",
            "bucket={:?} key={:?} version_id={:?} mode={:?} bypass={}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id,
            req.retention.mode,
            req.bypass_governance
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.object.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        self.storage_node
            .put_object_retention_if(
                bucket,
                key,
                req.object.version_id,
                req.retention,
                |stored| {
                    if !self.requester_can_manage_object_lock_with_bucket_policy(
                        req.object.requester(),
                        &bucket_info,
                        bucket_tags.as_deref(),
                        stored,
                        auth::PolicyAction::PutObjectRetention,
                        bucket_policy.as_deref(),
                    )? {
                        return Err(ServerError::AccessDenied);
                    }
                    Self::ensure_object_lock_bucket(&bucket_info)?;
                    let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                    let can_bypass_governance = self
                        .requester_can_bypass_governance_retention_with_bucket_policy(
                            req.object.requester(),
                            &bucket_info,
                            bucket_tags.as_deref(),
                            stored,
                            bucket_policy.as_deref(),
                        )?;
                    Self::validate_retention_update(
                        live.object_lock.retention,
                        req.retention,
                        req.bypass_governance,
                        can_bypass_governance,
                    )?;
                    Ok(live.version_id)
                },
            )
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.object.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok(())
    }

    pub fn get_object_retention(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<Option<ObjectRetention>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_retention",
            "bucket={:?} key={:?} version_id={:?}",
            req.object.bucket_name(),
            req.object.key,
            req.version_id
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        self.storage_node
            .get_object_retention_if(bucket, key, req.version_id, |stored| {
                if !self.requester_can_manage_object_lock_with_bucket_policy(
                    req.object.requester(),
                    &bucket_info,
                    bucket_tags.as_deref(),
                    stored,
                    auth::PolicyAction::GetObjectRetention,
                    bucket_policy.as_deref(),
                )? {
                    return Err(ServerError::AccessDenied);
                }
                Self::ensure_object_lock_bucket(&bucket_info)?;
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                Ok(live.object_lock.retention)
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })?
    }

    pub fn put_object_legal_hold(
        &self,
        req: &PutObjectLegalHoldRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_object_legal_hold",
            "bucket={:?} key={:?} version_id={:?} status={:?}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id,
            req.legal_hold
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.object.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        let legal_hold = StoredLegalHoldStatus::from_legal_hold_status(Some(req.legal_hold));
        self.storage_node
            .put_object_legal_hold_if(bucket, key, req.object.version_id, legal_hold, |stored| {
                if !self.requester_can_manage_object_lock_with_bucket_policy(
                    req.object.requester(),
                    &bucket_info,
                    bucket_tags.as_deref(),
                    stored,
                    auth::PolicyAction::PutObjectLegalHold,
                    bucket_policy.as_deref(),
                )? {
                    return Err(ServerError::AccessDenied);
                }
                Self::ensure_object_lock_bucket(&bucket_info)?;
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                Ok(live.version_id)
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.object.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok(())
    }

    pub fn get_object_legal_hold(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<Option<LegalHoldStatus>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_legal_hold",
            "bucket={:?} key={:?} version_id={:?}",
            req.object.bucket_name(),
            req.object.key,
            req.version_id
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        self.storage_node
            .get_object_legal_hold_if(bucket, key, req.version_id, |stored| {
                if !self.requester_can_manage_object_lock_with_bucket_policy(
                    req.object.requester(),
                    &bucket_info,
                    bucket_tags.as_deref(),
                    stored,
                    auth::PolicyAction::GetObjectLegalHold,
                    bucket_policy.as_deref(),
                )? {
                    return Err(ServerError::AccessDenied);
                }
                Self::ensure_object_lock_bucket(&bucket_info)?;
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                Ok(live.object_lock.legal_hold.as_legal_hold_status())
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })?
    }

    pub fn get_object_tags(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_tags",
            "bucket={:?} key={:?}",
            req.object.bucket_name(),
            req.object.key
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        self.storage_node
            .get_object_tags_if(bucket, key, req.version_id, |stored| {
                if !self.requester_can_manage_object_tags_with_bucket_policy(
                    BucketPolicyAccess {
                        requester: req.object.requester(),
                        bucket: &bucket_info,
                        bucket_tags: bucket_tags.as_deref(),
                        policy: bucket_policy.as_deref(),
                    },
                    stored,
                    Self::get_object_tagging_policy_action(req.version_id),
                    None,
                )? {
                    return Err(ServerError::AccessDenied);
                }
                if stored.is_delete_marker() {
                    return Err(ServerError::MethodNotAllowed);
                }
                Ok(stored.version_id())
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })?
    }

    pub fn delete_object_tags(&self, req: &ObjectVersionRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_object_tags",
            "bucket={:?} key={:?}",
            req.object.bucket_name(),
            req.object.key
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_bucket_owner_account_admin(req.object.requester(), &bucket_info);
        self.storage_node
            .delete_object_tags_if(bucket, key, req.version_id, |stored| {
                if !self.requester_can_manage_object_tags_with_bucket_policy(
                    BucketPolicyAccess {
                        requester: req.object.requester(),
                        bucket: &bucket_info,
                        bucket_tags: bucket_tags.as_deref(),
                        policy: bucket_policy.as_deref(),
                    },
                    stored,
                    Self::delete_object_tagging_policy_action(req.version_id),
                    None,
                )? {
                    return Err(ServerError::AccessDenied);
                }
                if stored.is_delete_marker() {
                    return Err(ServerError::MethodNotAllowed);
                }
                Ok(stored.version_id())
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok(())
    }

    pub fn get_object_acl(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<GetObjectAclResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_acl",
            "bucket={:?} key={:?} version_id={:?}",
            req.object.bucket_name(),
            req.object.key,
            req.version_id
        );
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_discover_missing_object_acl(req.object.requester(), &bucket_info);
        self.storage_node
            .load_object_if(bucket, key, req.version_id, |stored| {
                if !self.requester_can_read_object_acl_with_bucket_policy(
                    req.object.requester(),
                    &bucket_info,
                    bucket_tags.as_deref(),
                    stored,
                    Self::get_object_acl_policy_action(req.version_id),
                    bucket_policy.as_deref(),
                )? {
                    return Err(ServerError::AccessDenied);
                }
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                let result =
                    if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
                        let owner = Self::bucket_owner_identity(&bucket_info);
                        GetObjectAclResult {
                            owner_principal: owner.principal,
                            owner_canonical_id: owner.canonical_id.clone(),
                            acl_grants: AclGrants::new(vec![s3_types::AclGrant::new(
                                s3_types::AclGrantee::CanonicalUser(owner.canonical_id),
                                s3_types::AclPermission::FullControl,
                            )]),
                            version_id: live.version_id,
                        }
                    } else {
                        GetObjectAclResult {
                            owner_principal: live.owner.principal.clone(),
                            owner_canonical_id: live.owner.canonical_id.clone(),
                            acl_grants: live.acl_grants.clone(),
                            version_id: live.version_id,
                        }
                    };
                Ok(result)
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })?
    }

    pub fn put_object_acl(&self, req: &PutObjectAclRequest<'_>) -> Result<VersionId, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_object_acl",
            "bucket={:?} key={:?} version_id={:?} acl_kind={}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id,
            match &req.acl {
                PutObjectAclInput::Canned(_) => "canned",
                PutObjectAclInput::Grants(_) => "grants",
            }
        );
        if req.object.requester().is_anonymous() {
            return Err(ServerError::AnonymousApiAccessDenied);
        }
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let ObjectMetadataPolicyContext {
            bucket_info,
            bucket_policy,
            bucket_tags,
        } = self.load_object_metadata_policy_context(bucket, req.object.expected_bucket_owner())?;
        let can_discover_missing =
            Self::requester_can_discover_missing_object_acl(req.object.requester(), &bucket_info);
        let policy_context = req.authorization_policy_context()?;
        self.storage_node
            .put_object_acl_if(bucket, key, req.object.version_id, |stored| {
                if !self.requester_can_write_object_acl_with_bucket_policy(
                    BucketPolicyAccess {
                        requester: req.object.requester(),
                        bucket: &bucket_info,
                        bucket_tags: bucket_tags.as_deref(),
                        policy: bucket_policy.as_deref(),
                    },
                    stored,
                    Self::put_object_acl_policy_action(req.object.version_id),
                    policy_context,
                )? {
                    return Err(ServerError::AccessDenied);
                }
                if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                let acl_grants = match &req.acl {
                    PutObjectAclInput::Canned(acl) => {
                        Self::ensure_put_object_acl_supported(&bucket_info, *acl)?;
                        Self::object_acl_grants_for_write(&bucket_info, &live.owner, *acl)
                    }
                    PutObjectAclInput::Grants(acl_grants) => {
                        Self::ensure_supported_object_acl_grants(acl_grants)?;
                        acl_grants.clone()
                    }
                };
                let public_read = Self::acl_grants_public_read(&acl_grants);
                if Self::blocks_public_acls(bucket_info.public_access_block.as_ref())
                    && (Self::acl_grants_grant_public_read(&acl_grants)
                        || Self::acl_grants_grant_public_write(&acl_grants))
                {
                    return Err(ServerError::AccessDenied);
                }
                Ok((live.version_id, acl_grants, public_read))
            })
            .map_err(|error| {
                Self::map_object_metadata_access_error(
                    bucket,
                    key,
                    req.object.version_id,
                    can_discover_missing,
                    error,
                )
            })?
    }
}
