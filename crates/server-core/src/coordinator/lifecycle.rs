use std::collections::HashMap;

use s3_types::{
    BucketLifecycleConfiguration, LifecycleDate, LifecycleExpiration, LifecycleRule,
    LifecycleRuleStatus, VersionId,
};
use storage::StoredObject;

use super::{
    bucket_handles::{LoadedBucketHandle, LoadedBucketValue},
    BucketSummary, Coordinator, DeleteMarkerLifecycleExpiration, LifecycleAbortHeaders,
    LifecycleExpirationHeader, NoncurrentLifecycleExpiration,
};
use crate::error::ServerError;

impl Coordinator {
    #[cfg(test)]
    pub(super) fn requested_version_is_current_live(
        bucket: &str,
        key: &str,
        requested_version_id: Option<VersionId>,
        resolved_version_id: VersionId,
    ) -> Result<bool, ServerError> {
        let _ = (bucket, key, resolved_version_id);
        Ok(requested_version_id.is_none())
    }

    pub(super) fn cached_bucket_lifecycle(
        &self,
        bucket: &BucketSummary,
    ) -> Result<Option<BucketLifecycleConfiguration>, ServerError> {
        if !bucket.bucket_lifecycle_present {
            return Ok(None);
        }

        let authorized = self.authorize_load_bucket_lifecycle_for(&bucket.name);
        let raw_config = self.load_authorized_bucket_subresource(&authorized)?;
        match raw_config {
            Some(config_xml) => s3_types::parse_lifecycle_configuration_xml(config_xml.as_bytes())
                .map(Some)
                .map_err(
                    |error| ServerError::InternalError {
                        reason: format!(
                            "stored lifecycle configuration for {} failed to parse at request time: {error}",
                            bucket.name
                        ),
                    },
                ),
            None => Ok(None),
        }
    }

    pub(super) fn cached_bucket_lifecycle_for_loaded_handle(
        &self,
        bucket: &LoadedBucketHandle,
    ) -> Result<Option<BucketLifecycleConfiguration>, ServerError> {
        let bucket_summary = bucket.bucket();
        if !bucket_summary.bucket_lifecycle_present {
            return Ok(None);
        }

        match bucket.lifecycle() {
            LoadedBucketValue::Loaded(raw_config) => {
                s3_types::parse_lifecycle_configuration_xml(raw_config.as_bytes())
                    .map(Some)
                    .map_err(|error| ServerError::InternalError {
                        reason: format!(
                            "stored lifecycle configuration for {} failed to parse at request time: {error}",
                            bucket_summary.name,
                        ),
                    })
            }
            LoadedBucketValue::Missing | LoadedBucketValue::NotRequested => Ok(None),
        }
    }

    pub(super) fn current_object_lifecycle_expiration(
        &self,
        bucket: &BucketSummary,
        key: &str,
        tags_xml: Option<&str>,
        size: u64,
        last_modified: u64,
    ) -> Result<Option<LifecycleExpirationHeader>, ServerError> {
        let Some(config) = self.cached_bucket_lifecycle(bucket)? else {
            return Ok(None);
        };
        let tags = match tags_xml {
            Some(tags_xml) => Self::parse_serialized_tag_set(tags_xml)?,
            None => Vec::new(),
        };
        Ok(Self::evaluate_current_object_lifecycle_expiration(
            &config,
            key,
            &tags,
            size,
            last_modified,
        ))
    }

    pub(super) fn current_object_write_lifecycle_expiration(
        &self,
        bucket: &BucketSummary,
        key: &str,
        tags_xml: Option<&str>,
        size: u64,
        last_modified: u64,
    ) -> Result<Option<LifecycleExpirationHeader>, ServerError> {
        let Some(config) = self.cached_bucket_lifecycle(bucket)? else {
            return Ok(None);
        };
        let tags = match tags_xml {
            Some(tags_xml) => Self::parse_serialized_tag_set(tags_xml)?,
            None => Vec::new(),
        };
        Ok(Self::evaluate_current_object_lifecycle_expiration(
            &config,
            key,
            &tags,
            size,
            last_modified,
        ))
    }

    pub(super) fn current_object_write_lifecycle_expiration_for_loaded_bucket(
        &self,
        bucket: &LoadedBucketHandle,
        key: &str,
        tags_xml: Option<&str>,
        size: u64,
        last_modified: u64,
    ) -> Result<Option<LifecycleExpirationHeader>, ServerError> {
        let Some(config) = self.cached_bucket_lifecycle_for_loaded_handle(bucket)? else {
            return Ok(None);
        };
        let tags = match tags_xml {
            Some(tags_xml) => Self::parse_serialized_tag_set(tags_xml)?,
            None => Vec::new(),
        };
        Ok(Self::evaluate_current_object_lifecycle_expiration(
            &config,
            key,
            &tags,
            size,
            last_modified,
        ))
    }

    pub(super) fn multipart_lifecycle_abort_headers(
        &self,
        bucket: &BucketSummary,
        key: &str,
        initiated_at: u64,
    ) -> Result<Option<LifecycleAbortHeaders>, ServerError> {
        let Some(config) = self.cached_bucket_lifecycle(bucket)? else {
            return Ok(None);
        };
        Ok(Self::evaluate_multipart_lifecycle_abort_headers(
            &config,
            key,
            initiated_at,
        ))
    }

    pub(super) fn evaluate_current_object_lifecycle_expiration(
        config: &BucketLifecycleConfiguration,
        key: &str,
        tags: &[(String, String)],
        size: u64,
        last_modified: u64,
    ) -> Option<LifecycleExpirationHeader> {
        let mut best = None;

        for rule in &config.rules {
            if rule.status != LifecycleRuleStatus::Enabled
                || !rule.filter.matches_object(key, tags, size)
            {
                continue;
            }
            let Some(expiration) = &rule.expiration else {
                continue;
            };
            let expiry_time_millis = match expiration {
                LifecycleExpiration::Days(days) => {
                    Self::lifecycle_day_based_deadline(last_modified, days.get())?
                }
                LifecycleExpiration::Date(date) => Self::lifecycle_date_deadline(*date)?,
                LifecycleExpiration::ExpiredObjectDeleteMarker => continue,
            };
            let candidate = LifecycleExpirationHeader {
                expiry_time_millis,
                rule_id: rule.id.clone(),
            };
            if best
                .as_ref()
                .is_none_or(|current: &LifecycleExpirationHeader| {
                    candidate.expiry_time_millis < current.expiry_time_millis
                })
            {
                best = Some(candidate);
            }
        }

        best
    }

    pub(super) fn evaluate_due_noncurrent_version_expirations(
        config: &BucketLifecycleConfiguration,
        versions: &[StoredObject],
        now_millis: u64,
    ) -> Result<Vec<NoncurrentLifecycleExpiration>, ServerError> {
        #[derive(Debug)]
        struct NoncurrentVersionCandidate {
            version_id: VersionId,
            tags: Vec<(String, String)>,
            size: u64,
            became_noncurrent_at: u64,
        }

        if versions.len() <= 1 {
            return Ok(Vec::new());
        }

        let key = versions[0].key().as_str();
        let mut candidates = Vec::new();
        for stored in versions.iter().skip(1) {
            let Some(record) = stored.as_live() else {
                continue;
            };
            let Some(became_noncurrent_at) = record.became_noncurrent_at else {
                continue;
            };
            let tags = match record.tags.as_deref() {
                Some(tags_xml) => Self::parse_serialized_tag_set(tags_xml)?,
                None => Vec::new(),
            };
            candidates.push(NoncurrentVersionCandidate {
                version_id: record.version_id,
                tags,
                size: record.size,
                became_noncurrent_at,
            });
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let mut due_by_version: HashMap<VersionId, u64> = HashMap::new();
        for rule in &config.rules {
            if rule.status != LifecycleRuleStatus::Enabled {
                continue;
            }
            let Some(noncurrent) = &rule.noncurrent_version_expiration else {
                continue;
            };

            let mut newer_matching_noncurrent_versions = 0u32;
            for candidate in &candidates {
                if !rule
                    .filter
                    .matches_object(key, &candidate.tags, candidate.size)
                {
                    continue;
                }

                let Some(expiry_time_millis) = Self::lifecycle_day_based_deadline(
                    candidate.became_noncurrent_at,
                    noncurrent.noncurrent_days.get(),
                ) else {
                    continue;
                };
                let retain_newer = noncurrent
                    .newer_noncurrent_versions
                    .map_or(0, std::num::NonZeroU32::get);

                if newer_matching_noncurrent_versions >= retain_newer
                    && expiry_time_millis <= now_millis
                {
                    due_by_version
                        .entry(candidate.version_id)
                        .and_modify(|current| {
                            *current = (*current).min(expiry_time_millis);
                        })
                        .or_insert(expiry_time_millis);
                }

                // The retention count applies to newer noncurrent versions that
                // are in scope for the same rule.
                newer_matching_noncurrent_versions += 1;
            }
        }

        let mut due = Vec::new();
        for candidate in candidates {
            if let Some(expiry_time_millis) = due_by_version.remove(&candidate.version_id) {
                due.push(NoncurrentLifecycleExpiration {
                    version_id: candidate.version_id,
                    expiry_time_millis,
                });
            }
        }
        Ok(due)
    }

    pub(super) fn evaluate_due_expired_delete_marker(
        config: &BucketLifecycleConfiguration,
        versions: &[StoredObject],
        now_millis: u64,
    ) -> Option<DeleteMarkerLifecycleExpiration> {
        let StoredObject::DeleteMarker(marker) = versions.first()? else {
            return None;
        };
        if versions.len() != 1 {
            return None;
        }

        let mut best = None;
        for rule in &config.rules {
            if rule.status != LifecycleRuleStatus::Enabled
                || !Self::lifecycle_rule_matches_delete_marker(rule, marker.key.as_str())
            {
                continue;
            }

            let Some(expiration) = &rule.expiration else {
                continue;
            };
            let expiry_time_millis = match expiration {
                LifecycleExpiration::Days(days) => {
                    Self::lifecycle_day_based_deadline(marker.last_modified, days.get())?
                }
                LifecycleExpiration::Date(date) => Self::lifecycle_date_deadline(*date)?,
                LifecycleExpiration::ExpiredObjectDeleteMarker => 0,
            };

            if expiry_time_millis > now_millis {
                continue;
            }

            let candidate = DeleteMarkerLifecycleExpiration {
                version_id: marker.version_id,
                expiry_time_millis,
            };
            if best
                .as_ref()
                .is_none_or(|current: &DeleteMarkerLifecycleExpiration| {
                    candidate.expiry_time_millis < current.expiry_time_millis
                })
            {
                best = Some(candidate);
            }
        }

        best
    }

    pub(super) fn evaluate_multipart_lifecycle_abort_headers(
        config: &BucketLifecycleConfiguration,
        key: &str,
        initiated_at: u64,
    ) -> Option<LifecycleAbortHeaders> {
        let mut best = None;

        for rule in &config.rules {
            if rule.status != LifecycleRuleStatus::Enabled
                || !rule.filter.matches_multipart_upload(key)
            {
                continue;
            }
            let Some(abort) = &rule.abort_incomplete_multipart_upload else {
                continue;
            };
            let abort_time_millis = Self::lifecycle_day_based_deadline(
                initiated_at,
                abort.days_after_initiation.get(),
            )?;
            let candidate = LifecycleAbortHeaders {
                abort_time_millis,
                rule_id: rule.id.clone(),
            };
            if best.as_ref().is_none_or(|current: &LifecycleAbortHeaders| {
                candidate.abort_time_millis < current.abort_time_millis
            }) {
                best = Some(candidate);
            }
        }

        best
    }

    pub(super) fn lifecycle_rule_matches_delete_marker(rule: &LifecycleRule, key: &str) -> bool {
        !rule.filter.has_tag_filter()
            && !rule.filter.has_size_filter()
            && rule
                .filter
                .prefix
                .as_ref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }

    pub(super) fn lifecycle_day_based_deadline(start_millis: u64, days: u32) -> Option<u64> {
        let start_day = start_millis / 86_400_000;
        start_day
            .checked_add(u64::from(days))?
            .checked_add(1)?
            .checked_mul(86_400_000)
    }

    pub(super) fn lifecycle_date_deadline(date: LifecycleDate) -> Option<u64> {
        let days = u64::try_from(date.days_since_epoch()).ok()?;
        days.checked_mul(86_400_000)
    }
}
