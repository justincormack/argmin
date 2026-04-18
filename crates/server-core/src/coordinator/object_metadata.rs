use s3_types::{
    AclGrants, LegalHoldStatus, ObjectLockDefaultRetention, ObjectLockMode, ObjectRetention,
    RetentionPeriod, StoredLegalHoldStatus, VersionId,
};
use storage::{BucketName, ObjectKey, ObjectLockState};

use super::{
    AuthorizedPutObjectAclUpdate, BucketSummary, Coordinator, GetObjectAclResult,
    ObjectVersionRequest, PutObjectAclInput, PutObjectAclRequest, PutObjectLegalHoldRequest,
    PutObjectRetentionRequest, PutObjectTagsRequest, TRACE_TARGET,
};
use crate::error::ServerError;

impl Coordinator {
    fn persist_locked_object_acl(
        bucket: &BucketName,
        key: &ObjectKey,
        meta_pg: &storage::PgStore,
        version_id: VersionId,
        acl_grants: AclGrants,
        public_read: bool,
    ) -> Result<VersionId, ServerError> {
        storage::PgMetadataStore::put_object_acl(
            meta_pg,
            bucket,
            key,
            version_id,
            &acl_grants,
            public_read,
        )
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            storage::MetadataError::MethodNotAllowedOnDeleteMarker => ServerError::MethodNotAllowed,
            other => ServerError::Metadata(other),
        })?;
        Ok(version_id)
    }

    fn apply_authorized_object_acl_update(
        &self,
        authorized: &AuthorizedPutObjectAclUpdate<'_>,
    ) -> Result<VersionId, ServerError> {
        Self::persist_locked_object_acl(
            &authorized.bucket,
            &authorized.key,
            authorized.pgs.meta(),
            authorized.version_id,
            authorized.acl_grants.clone(),
            authorized.public_read,
        )
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
        let authorized = self.authorize_put_object_tags(req)?;
        storage::PgMetadataStore::put_object_tags(
            authorized.pgs.meta(),
            &authorized.bucket,
            &authorized.key,
            authorized.version_id,
            req.tags,
        )
        .map_err(ServerError::Metadata)
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
        let authorized = self.authorize_put_object_retention(req)?;
        storage::PgMetadataStore::put_object_retention(
            authorized.pgs.meta(),
            &authorized.bucket,
            &authorized.key,
            authorized.version_id,
            authorized.retention,
        )
        .map_err(|e| match e {
            storage::MetadataError::MethodNotAllowedOnDeleteMarker => ServerError::MethodNotAllowed,
            other => ServerError::Metadata(other),
        })
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
        let authorized = self.authorize_get_object_retention(req)?;
        Ok(authorized.retention)
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
        let authorized = self.authorize_put_object_legal_hold(req)?;
        storage::PgMetadataStore::put_object_legal_hold(
            authorized.pgs.meta(),
            &authorized.bucket,
            &authorized.key,
            authorized.version_id,
            authorized.legal_hold,
        )
        .map_err(|e| match e {
            storage::MetadataError::MethodNotAllowedOnDeleteMarker => ServerError::MethodNotAllowed,
            other => ServerError::Metadata(other),
        })
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
        let authorized = self.authorize_get_object_legal_hold(req)?;
        Ok(authorized.legal_hold)
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
        let authorized = self.authorize_get_object_tags(req)?;
        storage::PgMetadataStore::get_object_tags(
            authorized.pgs.meta(),
            &authorized.bucket,
            &authorized.key,
            authorized.version_id,
        )
        .map_err(ServerError::Metadata)
    }

    pub fn delete_object_tags(&self, req: &ObjectVersionRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_object_tags",
            "bucket={:?} key={:?}",
            req.object.bucket_name(),
            req.object.key
        );
        let authorized = self.authorize_delete_object_tags(req)?;
        storage::PgMetadataStore::delete_object_tags(
            authorized.pgs.meta(),
            &authorized.bucket,
            &authorized.key,
            authorized.version_id,
        )
        .map_err(ServerError::Metadata)
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
        let authorized = self.authorize_get_object_acl(req)?;
        Ok(authorized.result)
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
        let authorized = self.authorize_put_object_acl(req)?;
        self.apply_authorized_object_acl_update(&authorized)
    }
}
