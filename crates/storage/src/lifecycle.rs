use std::collections::HashSet;
use std::num::NonZeroU32;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LifecycleConfigError {
    #[error("malformed XML: {reason}")]
    MalformedXml { reason: String },

    #[error("invalid argument: {reason}")]
    InvalidArgument { reason: String },

    #[error("not implemented: {feature}")]
    NotImplemented { feature: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketLifecycleConfiguration {
    pub rules: Vec<LifecycleRule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleRuleStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleRuleFilter {
    pub prefix: Option<String>,
    pub tags: Vec<LifecycleTag>,
    pub object_size_greater_than: Option<u64>,
    pub object_size_less_than: Option<u64>,
    pub explicit_filter: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LifecycleDate {
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleExpiration {
    Days(NonZeroU32),
    Date(LifecycleDate),
    ExpiredObjectDeleteMarker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoncurrentVersionExpiration {
    pub noncurrent_days: NonZeroU32,
    pub newer_noncurrent_versions: Option<NonZeroU32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortIncompleteMultipartUpload {
    pub days_after_initiation: NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleRule {
    pub id: Option<String>,
    pub status: LifecycleRuleStatus,
    pub filter: LifecycleRuleFilter,
    pub expiration: Option<LifecycleExpiration>,
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

impl LifecycleRuleFilter {
    #[must_use]
    pub fn has_tag_filter(&self) -> bool {
        !self.tags.is_empty()
    }

    #[must_use]
    pub fn has_size_filter(&self) -> bool {
        self.object_size_greater_than.is_some() || self.object_size_less_than.is_some()
    }

    #[must_use]
    pub fn has_scope(&self) -> bool {
        self.explicit_filter
            || self.prefix.is_some()
            || !self.tags.is_empty()
            || self.object_size_greater_than.is_some()
            || self.object_size_less_than.is_some()
    }

    #[must_use]
    pub fn matches_object(&self, key: &str, tags: &[(String, String)], size: u64) -> bool {
        if self
            .prefix
            .as_ref()
            .is_some_and(|prefix| !key.starts_with(prefix))
        {
            return false;
        }
        if self
            .object_size_greater_than
            .is_some_and(|min_size| size <= min_size)
        {
            return false;
        }
        if self
            .object_size_less_than
            .is_some_and(|max_size| size >= max_size)
        {
            return false;
        }
        self.tags.iter().all(|expected| {
            tags.iter()
                .any(|(key, value)| key == &expected.key && value == &expected.value)
        })
    }

    #[must_use]
    pub fn matches_multipart_upload(&self, key: &str) -> bool {
        !self.has_tag_filter()
            && !self.has_size_filter()
            && self
                .prefix
                .as_ref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }

    fn add_tag(&mut self, tag: LifecycleTag) -> Result<(), LifecycleConfigError> {
        if self.tags.iter().any(|existing| existing.key == tag.key) {
            return Err(LifecycleConfigError::InvalidArgument {
                reason: format!("duplicate lifecycle tag filter key {}", tag.key),
            });
        }
        self.tags.push(tag);
        Ok(())
    }
}

impl LifecycleDate {
    #[must_use]
    pub fn format(self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    #[must_use]
    pub fn days_since_epoch(self) -> i64 {
        let month = i64::from(self.month);
        let day = i64::from(self.day);
        let adjust = if self.month <= 2 { 1 } else { 0 };
        let year = i64::from(self.year) - adjust;
        let era = if year >= 0 { year } else { year - 399 } / 400;
        let year_of_era = year - era * 400;
        let day_of_year = (153 * (month + if self.month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        era * 146_097 + day_of_era - 719_468
    }
}

pub fn parse_lifecycle_configuration_xml(
    data: &[u8],
) -> Result<BucketLifecycleConfiguration, LifecycleConfigError> {
    let root = parse_xml_document(data)?;
    if root.name != "LifecycleConfiguration" {
        return Err(LifecycleConfigError::MalformedXml {
            reason: "missing LifecycleConfiguration element".to_string(),
        });
    }
    root.ensure_no_text()?;

    let mut rules = Vec::new();
    for child in &root.children {
        if child.name != "Rule" {
            return Err(LifecycleConfigError::MalformedXml {
                reason: format!("unexpected <{}> in LifecycleConfiguration", child.name),
            });
        }
        rules.push(parse_rule(child)?);
    }

    validate_configuration(&rules)?;
    Ok(BucketLifecycleConfiguration { rules })
}

#[must_use]
pub fn render_lifecycle_configuration_xml(config: &BucketLifecycleConfiguration) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );

    for rule in &config.rules {
        xml.push_str("<Rule>");
        if let Some(id) = &rule.id {
            xml.push_str("<ID>");
            xml.push_str(&xml_escape(id));
            xml.push_str("</ID>");
        }
        render_filter(&mut xml, &rule.filter);
        xml.push_str("<Status>");
        xml.push_str(match rule.status {
            LifecycleRuleStatus::Enabled => "Enabled",
            LifecycleRuleStatus::Disabled => "Disabled",
        });
        xml.push_str("</Status>");
        if let Some(expiration) = &rule.expiration {
            render_expiration(&mut xml, expiration);
        }
        if let Some(noncurrent) = &rule.noncurrent_version_expiration {
            xml.push_str("<NoncurrentVersionExpiration><NoncurrentDays>");
            xml.push_str(&noncurrent.noncurrent_days.get().to_string());
            xml.push_str("</NoncurrentDays>");
            if let Some(newer) = noncurrent.newer_noncurrent_versions {
                xml.push_str("<NewerNoncurrentVersions>");
                xml.push_str(&newer.get().to_string());
                xml.push_str("</NewerNoncurrentVersions>");
            }
            xml.push_str("</NoncurrentVersionExpiration>");
        }
        if let Some(abort) = &rule.abort_incomplete_multipart_upload {
            xml.push_str("<AbortIncompleteMultipartUpload><DaysAfterInitiation>");
            xml.push_str(&abort.days_after_initiation.get().to_string());
            xml.push_str("</DaysAfterInitiation></AbortIncompleteMultipartUpload>");
        }
        xml.push_str("</Rule>");
    }

    xml.push_str("</LifecycleConfiguration>");
    xml
}

fn render_filter(xml: &mut String, filter: &LifecycleRuleFilter) {
    if !filter.explicit_filter && filter.tags.is_empty() && !filter.has_size_filter() {
        if let Some(prefix) = &filter.prefix {
            xml.push_str("<Prefix>");
            xml.push_str(&xml_escape(prefix));
            xml.push_str("</Prefix>");
        }
        return;
    }

    xml.push_str("<Filter>");
    let simple_prefix_only =
        filter.prefix.is_some() && filter.tags.is_empty() && !filter.has_size_filter();
    let simple_single_tag =
        filter.prefix.is_none() && filter.tags.len() == 1 && !filter.has_size_filter();
    let simple_single_gt = filter.prefix.is_none()
        && filter.tags.is_empty()
        && filter.object_size_greater_than.is_some()
        && filter.object_size_less_than.is_none();
    let simple_single_lt = filter.prefix.is_none()
        && filter.tags.is_empty()
        && filter.object_size_greater_than.is_none()
        && filter.object_size_less_than.is_some();

    if !filter.has_scope() || filter.explicit_filter && !filter.has_scope() {
        // Empty Filter applies to the whole bucket.
    } else if simple_prefix_only {
        xml.push_str("<Prefix>");
        xml.push_str(&xml_escape(filter.prefix.as_deref().unwrap_or("")));
        xml.push_str("</Prefix>");
    } else if simple_single_tag {
        render_tag(xml, &filter.tags[0]);
    } else if simple_single_gt {
        xml.push_str("<ObjectSizeGreaterThan>");
        xml.push_str(
            &filter
                .object_size_greater_than
                .unwrap_or_default()
                .to_string(),
        );
        xml.push_str("</ObjectSizeGreaterThan>");
    } else if simple_single_lt {
        xml.push_str("<ObjectSizeLessThan>");
        xml.push_str(&filter.object_size_less_than.unwrap_or_default().to_string());
        xml.push_str("</ObjectSizeLessThan>");
    } else {
        xml.push_str("<And>");
        if let Some(prefix) = &filter.prefix {
            xml.push_str("<Prefix>");
            xml.push_str(&xml_escape(prefix));
            xml.push_str("</Prefix>");
        }
        for tag in &filter.tags {
            render_tag(xml, tag);
        }
        if let Some(min_size) = filter.object_size_greater_than {
            xml.push_str("<ObjectSizeGreaterThan>");
            xml.push_str(&min_size.to_string());
            xml.push_str("</ObjectSizeGreaterThan>");
        }
        if let Some(max_size) = filter.object_size_less_than {
            xml.push_str("<ObjectSizeLessThan>");
            xml.push_str(&max_size.to_string());
            xml.push_str("</ObjectSizeLessThan>");
        }
        xml.push_str("</And>");
    }
    xml.push_str("</Filter>");
}

fn render_tag(xml: &mut String, tag: &LifecycleTag) {
    xml.push_str("<Tag><Key>");
    xml.push_str(&xml_escape(&tag.key));
    xml.push_str("</Key><Value>");
    xml.push_str(&xml_escape(&tag.value));
    xml.push_str("</Value></Tag>");
}

fn render_expiration(xml: &mut String, expiration: &LifecycleExpiration) {
    xml.push_str("<Expiration>");
    match expiration {
        LifecycleExpiration::Days(days) => {
            xml.push_str("<Days>");
            xml.push_str(&days.get().to_string());
            xml.push_str("</Days>");
        }
        LifecycleExpiration::Date(date) => {
            xml.push_str("<Date>");
            xml.push_str(&date.format());
            xml.push_str("T00:00:00.000Z</Date>");
        }
        LifecycleExpiration::ExpiredObjectDeleteMarker => {
            xml.push_str("<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>");
        }
    }
    xml.push_str("</Expiration>");
}

fn validate_configuration(rules: &[LifecycleRule]) -> Result<(), LifecycleConfigError> {
    if rules.is_empty() {
        return Err(LifecycleConfigError::InvalidArgument {
            reason: "lifecycle configuration must contain at least one rule".to_string(),
        });
    }
    if rules.len() > 1000 {
        return Err(LifecycleConfigError::InvalidArgument {
            reason: format!(
                "lifecycle configuration has {} rules, maximum is 1000",
                rules.len()
            ),
        });
    }

    let mut seen_ids = HashSet::new();
    for rule in rules {
        if let Some(id) = &rule.id {
            if id.len() > 255 {
                return Err(LifecycleConfigError::InvalidArgument {
                    reason: "lifecycle rule ID must be 255 characters or fewer".to_string(),
                });
            }
            if !seen_ids.insert(id.clone()) {
                return Err(LifecycleConfigError::InvalidArgument {
                    reason: format!("duplicate lifecycle rule ID {id}"),
                });
            }
        }

        if rule.expiration.is_none()
            && rule.noncurrent_version_expiration.is_none()
            && rule.abort_incomplete_multipart_upload.is_none()
        {
            return Err(LifecycleConfigError::InvalidArgument {
                reason: "lifecycle rule must contain at least one supported action".to_string(),
            });
        }

        if matches!(
            rule.expiration,
            Some(LifecycleExpiration::ExpiredObjectDeleteMarker)
        ) && rule.filter.has_tag_filter()
        {
            return Err(LifecycleConfigError::InvalidArgument {
                reason:
                    "ExpiredObjectDeleteMarker lifecycle rules do not support tag-based filters"
                        .to_string(),
            });
        }

        if rule.abort_incomplete_multipart_upload.is_some() && rule.filter.has_tag_filter() {
            return Err(LifecycleConfigError::InvalidArgument {
                reason:
                    "AbortIncompleteMultipartUpload lifecycle rules do not support tag-based filters"
                        .to_string(),
            });
        }

        if rule.abort_incomplete_multipart_upload.is_some() && rule.filter.has_size_filter() {
            return Err(LifecycleConfigError::InvalidArgument {
                reason:
                    "AbortIncompleteMultipartUpload lifecycle rules do not support object size filters"
                        .to_string(),
            });
        }

        if let Some(noncurrent) = &rule.noncurrent_version_expiration {
            if noncurrent.newer_noncurrent_versions.is_some() && !rule.filter.has_scope() {
                return Err(LifecycleConfigError::InvalidArgument {
                    reason: "NewerNoncurrentVersions requires an explicit lifecycle filter"
                        .to_string(),
                });
            }
        }

        if let (Some(min_size), Some(max_size)) = (
            rule.filter.object_size_greater_than,
            rule.filter.object_size_less_than,
        ) {
            if min_size >= max_size {
                return Err(LifecycleConfigError::InvalidArgument {
                    reason: "ObjectSizeGreaterThan must be less than ObjectSizeLessThan"
                        .to_string(),
                });
            }
        }
    }

    Ok(())
}

fn parse_rule(element: &XmlElement) -> Result<LifecycleRule, LifecycleConfigError> {
    element.ensure_no_text()?;
    let mut id = None;
    let mut status = None;
    let mut legacy_prefix = None;
    let mut filter = LifecycleRuleFilter::default();
    let mut expiration = None;
    let mut noncurrent_version_expiration = None;
    let mut abort_incomplete_multipart_upload = None;

    for child in &element.children {
        match child.name.as_str() {
            "ID" => {
                set_once(&mut id, child.trimmed_text()?.to_string(), "ID")?;
            }
            "Status" => {
                let parsed = match child.trimmed_text()? {
                    "Enabled" => LifecycleRuleStatus::Enabled,
                    "Disabled" => LifecycleRuleStatus::Disabled,
                    _ => {
                        return Err(LifecycleConfigError::MalformedXml {
                            reason: "lifecycle rule Status must be Enabled or Disabled".to_string(),
                        });
                    }
                };
                set_once(&mut status, parsed, "Status")?;
            }
            "Prefix" => {
                set_once(
                    &mut legacy_prefix,
                    child.trimmed_text()?.to_string(),
                    "Prefix",
                )?;
            }
            "Filter" => {
                if filter.explicit_filter {
                    return Err(LifecycleConfigError::MalformedXml {
                        reason: "duplicate Filter element in lifecycle rule".to_string(),
                    });
                }
                filter = parse_filter(child)?;
            }
            "Expiration" => {
                set_once(&mut expiration, parse_expiration(child)?, "Expiration")?;
            }
            "NoncurrentVersionExpiration" => {
                set_once(
                    &mut noncurrent_version_expiration,
                    parse_noncurrent_version_expiration(child)?,
                    "NoncurrentVersionExpiration",
                )?;
            }
            "AbortIncompleteMultipartUpload" => {
                set_once(
                    &mut abort_incomplete_multipart_upload,
                    parse_abort_incomplete_multipart_upload(child)?,
                    "AbortIncompleteMultipartUpload",
                )?;
            }
            "Transition"
            | "Transitions"
            | "NoncurrentVersionTransition"
            | "NoncurrentVersionTransitions" => {
                return Err(LifecycleConfigError::NotImplemented {
                    feature: "Lifecycle transition rules".to_string(),
                });
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in lifecycle rule"),
                });
            }
        }
    }

    if let Some(prefix) = legacy_prefix {
        if filter.explicit_filter {
            return Err(LifecycleConfigError::MalformedXml {
                reason: "lifecycle rule may not contain both Prefix and Filter".to_string(),
            });
        }
        filter.prefix = Some(prefix);
    }

    let status = status.ok_or_else(|| LifecycleConfigError::MalformedXml {
        reason: "lifecycle rule is missing Status".to_string(),
    })?;

    Ok(LifecycleRule {
        id,
        status,
        filter,
        expiration,
        noncurrent_version_expiration,
        abort_incomplete_multipart_upload,
    })
}

fn parse_filter(element: &XmlElement) -> Result<LifecycleRuleFilter, LifecycleConfigError> {
    element.ensure_no_text()?;
    let mut filter = LifecycleRuleFilter {
        explicit_filter: true,
        ..LifecycleRuleFilter::default()
    };
    let mut predicate = None;

    for child in &element.children {
        match child.name.as_str() {
            "Prefix" => {
                mark_filter_predicate(&mut predicate, "Prefix")?;
                set_once(
                    &mut filter.prefix,
                    child.trimmed_text()?.to_string(),
                    "Prefix",
                )?;
            }
            "Tag" => {
                mark_filter_predicate(&mut predicate, "Tag")?;
                filter.add_tag(parse_tag(child)?)?;
            }
            "And" => {
                mark_filter_predicate(&mut predicate, "And")?;
                parse_and(child, &mut filter)?;
            }
            "ObjectSizeGreaterThan" => {
                mark_filter_predicate(&mut predicate, "ObjectSizeGreaterThan")?;
                set_once(
                    &mut filter.object_size_greater_than,
                    parse_u64_text(child, "ObjectSizeGreaterThan")?,
                    "ObjectSizeGreaterThan",
                )?;
            }
            "ObjectSizeLessThan" => {
                mark_filter_predicate(&mut predicate, "ObjectSizeLessThan")?;
                set_once(
                    &mut filter.object_size_less_than,
                    parse_u64_text(child, "ObjectSizeLessThan")?,
                    "ObjectSizeLessThan",
                )?;
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in lifecycle filter"),
                });
            }
        }
    }

    Ok(filter)
}

fn parse_and(
    element: &XmlElement,
    filter: &mut LifecycleRuleFilter,
) -> Result<(), LifecycleConfigError> {
    element.ensure_no_text()?;
    for child in &element.children {
        match child.name.as_str() {
            "Prefix" => {
                set_once(
                    &mut filter.prefix,
                    child.trimmed_text()?.to_string(),
                    "Prefix",
                )?;
            }
            "Tag" => {
                filter.add_tag(parse_tag(child)?)?;
            }
            "ObjectSizeGreaterThan" => {
                set_once(
                    &mut filter.object_size_greater_than,
                    parse_u64_text(child, "ObjectSizeGreaterThan")?,
                    "ObjectSizeGreaterThan",
                )?;
            }
            "ObjectSizeLessThan" => {
                set_once(
                    &mut filter.object_size_less_than,
                    parse_u64_text(child, "ObjectSizeLessThan")?,
                    "ObjectSizeLessThan",
                )?;
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in lifecycle filter And"),
                });
            }
        }
    }
    Ok(())
}

fn parse_tag(element: &XmlElement) -> Result<LifecycleTag, LifecycleConfigError> {
    element.ensure_no_text()?;
    let mut key = None;
    let mut value = None;

    for child in &element.children {
        match child.name.as_str() {
            "Key" => {
                set_once(&mut key, child.trimmed_text()?.to_string(), "Key")?;
            }
            "Value" => {
                set_once(&mut value, child.trimmed_text()?.to_string(), "Value")?;
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in lifecycle Tag"),
                });
            }
        }
    }

    Ok(LifecycleTag {
        key: key.ok_or_else(|| LifecycleConfigError::MalformedXml {
            reason: format!("missing <Key> in <{}>", element.name),
        })?,
        value: value.ok_or_else(|| LifecycleConfigError::MalformedXml {
            reason: format!("missing <Value> in <{}>", element.name),
        })?,
    })
}

fn parse_expiration(element: &XmlElement) -> Result<LifecycleExpiration, LifecycleConfigError> {
    element.ensure_no_text()?;
    let mut days = None;
    let mut date = None;
    let mut delete_marker = None;

    for child in &element.children {
        match child.name.as_str() {
            "Days" => {
                set_once(&mut days, parse_non_zero_u32_text(child, "Days")?, "Days")?;
            }
            "Date" => {
                set_once(
                    &mut date,
                    parse_lifecycle_date(child.trimmed_text()?)?,
                    "Date",
                )?;
            }
            "ExpiredObjectDeleteMarker" => {
                set_once(
                    &mut delete_marker,
                    parse_bool_text(child.trimmed_text()?, "ExpiredObjectDeleteMarker")?,
                    "ExpiredObjectDeleteMarker",
                )?;
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in Expiration"),
                });
            }
        }
    }

    match (days, date, delete_marker) {
        (Some(_), Some(_), _) => Err(LifecycleConfigError::InvalidArgument {
            reason: "Expiration may not specify both Days and Date".to_string(),
        }),
        (Some(_), _, Some(true)) | (_, Some(_), Some(true)) => {
            Err(LifecycleConfigError::InvalidArgument {
                reason: "ExpiredObjectDeleteMarker may not be combined with Days or Date"
                    .to_string(),
            })
        }
        (Some(days), None, _) => Ok(LifecycleExpiration::Days(days)),
        (None, Some(date), _) => Ok(LifecycleExpiration::Date(date)),
        (None, None, Some(true)) => Ok(LifecycleExpiration::ExpiredObjectDeleteMarker),
        _ => Err(LifecycleConfigError::InvalidArgument {
            reason: "Expiration must contain Days, Date, or ExpiredObjectDeleteMarker".to_string(),
        }),
    }
}

fn parse_noncurrent_version_expiration(
    element: &XmlElement,
) -> Result<NoncurrentVersionExpiration, LifecycleConfigError> {
    element.ensure_no_text()?;
    let mut noncurrent_days = None;
    let mut newer_noncurrent_versions = None;

    for child in &element.children {
        match child.name.as_str() {
            "NoncurrentDays" => {
                set_once(
                    &mut noncurrent_days,
                    parse_non_zero_u32_text(child, "NoncurrentDays")?,
                    "NoncurrentDays",
                )?;
            }
            "NewerNoncurrentVersions" => {
                let value = parse_non_zero_u32_text(child, "NewerNoncurrentVersions")?;
                if value.get() > 100 {
                    return Err(LifecycleConfigError::InvalidArgument {
                        reason: "NewerNoncurrentVersions must be between 1 and 100".to_string(),
                    });
                }
                set_once(
                    &mut newer_noncurrent_versions,
                    value,
                    "NewerNoncurrentVersions",
                )?;
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in NoncurrentVersionExpiration"),
                });
            }
        }
    }

    Ok(NoncurrentVersionExpiration {
        noncurrent_days: noncurrent_days.ok_or_else(|| LifecycleConfigError::InvalidArgument {
            reason: "NoncurrentVersionExpiration must contain NoncurrentDays".to_string(),
        })?,
        newer_noncurrent_versions,
    })
}

fn parse_abort_incomplete_multipart_upload(
    element: &XmlElement,
) -> Result<AbortIncompleteMultipartUpload, LifecycleConfigError> {
    element.ensure_no_text()?;
    let mut days = None;
    for child in &element.children {
        match child.name.as_str() {
            "DaysAfterInitiation" => {
                set_once(
                    &mut days,
                    parse_non_zero_u32_text(child, "DaysAfterInitiation")?,
                    "DaysAfterInitiation",
                )?;
            }
            other => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{other}> in AbortIncompleteMultipartUpload"),
                });
            }
        }
    }

    Ok(AbortIncompleteMultipartUpload {
        days_after_initiation: days.ok_or_else(|| LifecycleConfigError::InvalidArgument {
            reason: "AbortIncompleteMultipartUpload must contain DaysAfterInitiation".to_string(),
        })?,
    })
}

fn parse_lifecycle_date(text: &str) -> Result<LifecycleDate, LifecycleConfigError> {
    let text = text.trim();
    let date_part = if text.len() == 10 {
        text
    } else {
        let (date_part, time_part) =
            text.split_once('T')
                .ok_or_else(|| LifecycleConfigError::InvalidArgument {
                    reason: format!("invalid lifecycle date {text}"),
                })?;
        if !matches_lifecycle_midnight_utc(time_part) {
            return Err(LifecycleConfigError::InvalidArgument {
                reason: format!("invalid lifecycle date {text}"),
            });
        }
        date_part
    };
    if date_part.len() != 10 || !date_part.is_ascii() {
        return Err(LifecycleConfigError::InvalidArgument {
            reason: format!("invalid lifecycle date {text}"),
        });
    }
    let year: i32 = date_part[0..4]
        .parse()
        .map_err(|_| LifecycleConfigError::InvalidArgument {
            reason: format!("invalid lifecycle date {text}"),
        })?;
    let month: u8 = date_part[5..7]
        .parse()
        .map_err(|_| LifecycleConfigError::InvalidArgument {
            reason: format!("invalid lifecycle date {text}"),
        })?;
    let day: u8 = date_part[8..10]
        .parse()
        .map_err(|_| LifecycleConfigError::InvalidArgument {
            reason: format!("invalid lifecycle date {text}"),
        })?;
    if &date_part[4..5] != "-" || &date_part[7..8] != "-" || !valid_calendar_date(year, month, day)
    {
        return Err(LifecycleConfigError::InvalidArgument {
            reason: format!("invalid lifecycle date {text}"),
        });
    }
    Ok(LifecycleDate { year, month, day })
}

fn matches_lifecycle_midnight_utc(time_part: &str) -> bool {
    let Some(time_part) = time_part.strip_suffix('Z') else {
        return false;
    };
    let (base_time, fraction) = match time_part.split_once('.') {
        Some((base_time, fraction)) => (base_time, Some(fraction)),
        None => (time_part, None),
    };
    if base_time != "00:00:00" {
        return false;
    }
    match fraction {
        Some(fraction) => !fraction.is_empty() && fraction.bytes().all(|byte| byte == b'0'),
        None => true,
    }
}

fn mark_filter_predicate(
    current: &mut Option<&'static str>,
    name: &'static str,
) -> Result<(), LifecycleConfigError> {
    if current.is_some() {
        return Err(LifecycleConfigError::MalformedXml {
            reason: "lifecycle Filter must contain exactly one predicate; combine multiple conditions inside And".to_string(),
        });
    }
    *current = Some(name);
    Ok(())
}

fn parse_bool_text(value: &str, field: &str) -> Result<bool, LifecycleConfigError> {
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(LifecycleConfigError::InvalidArgument {
            reason: format!("{field} must be true or false"),
        }),
    }
}

fn parse_u64_text(element: &XmlElement, field: &str) -> Result<u64, LifecycleConfigError> {
    element
        .trimmed_text()?
        .parse()
        .map_err(|_| LifecycleConfigError::InvalidArgument {
            reason: format!("{field} must be a non-negative integer"),
        })
}

fn parse_non_zero_u32_text(
    element: &XmlElement,
    field: &str,
) -> Result<NonZeroU32, LifecycleConfigError> {
    let value: u32 =
        element
            .trimmed_text()?
            .parse()
            .map_err(|_| LifecycleConfigError::InvalidArgument {
                reason: format!("{field} must be a positive integer"),
            })?;
    NonZeroU32::new(value).ok_or_else(|| LifecycleConfigError::InvalidArgument {
        reason: format!("{field} must be greater than zero"),
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T, field: &str) -> Result<(), LifecycleConfigError> {
    if slot.is_some() {
        return Err(LifecycleConfigError::MalformedXml {
            reason: format!("duplicate {field} element"),
        });
    }
    *slot = Some(value);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct XmlElement {
    name: String,
    children: Vec<XmlElement>,
    text: String,
}

impl XmlElement {
    fn ensure_no_text(&self) -> Result<(), LifecycleConfigError> {
        if !self.text.trim().is_empty() {
            return Err(LifecycleConfigError::MalformedXml {
                reason: format!("<{}> must not contain text content", self.name),
            });
        }
        Ok(())
    }

    fn trimmed_text(&self) -> Result<&str, LifecycleConfigError> {
        if !self.children.is_empty() {
            return Err(LifecycleConfigError::MalformedXml {
                reason: format!("<{}> must not contain child elements", self.name),
            });
        }
        Ok(self.text.trim())
    }
}

fn parse_xml_document(data: &[u8]) -> Result<XmlElement, LifecycleConfigError> {
    let input = std::str::from_utf8(data).map_err(|_| LifecycleConfigError::MalformedXml {
        reason: "XML body is not valid UTF-8".to_string(),
    })?;

    let bytes = input.as_bytes();
    let mut index = 0;
    let mut root = None;
    let mut stack: Vec<XmlElement> = Vec::new();

    while index < bytes.len() {
        if bytes[index] != b'<' {
            let next_tag = input[index..]
                .find('<')
                .map_or(bytes.len(), |offset| index + offset);
            let text = &input[index..next_tag];
            if !text.is_empty() {
                if let Some(current) = stack.last_mut() {
                    current.text.push_str(&xml_unescape(text)?);
                } else if !text.trim().is_empty() {
                    return Err(LifecycleConfigError::MalformedXml {
                        reason: "unexpected text outside root XML element".to_string(),
                    });
                }
            }
            index = next_tag;
            continue;
        }

        if input[index..].starts_with("<?") {
            let Some(end) = input[index + 2..].find("?>") else {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "unterminated XML declaration".to_string(),
                });
            };
            index += end + 4;
            continue;
        }
        if input[index..].starts_with("<!--") {
            let Some(end) = input[index + 4..].find("-->") else {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "unterminated XML comment".to_string(),
                });
            };
            index += end + 7;
            continue;
        }
        if input[index..].starts_with("<!") {
            return Err(LifecycleConfigError::MalformedXml {
                reason: "unsupported XML declaration".to_string(),
            });
        }

        let tag_end = find_tag_end(input, index + 1)?;
        let inner = &input[index + 1..tag_end];
        if let Some(stripped) = inner.strip_prefix('/') {
            let name = normalize_tag_name(stripped.trim().trim_end_matches('/'));
            let element = stack
                .pop()
                .ok_or_else(|| LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected closing </{name}>"),
                })?;
            if element.name != name {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("closing tag </{name}> does not match <{}>", element.name),
                });
            }
            if let Some(parent) = stack.last_mut() {
                parent.children.push(element);
            } else if root.is_none() {
                root = Some(element);
            } else {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "multiple root XML elements are not allowed".to_string(),
                });
            }
        } else {
            let self_closing = inner.trim_end().ends_with('/');
            let name = start_tag_name(inner)?;
            let element = XmlElement {
                name: name.to_string(),
                children: Vec::new(),
                text: String::new(),
            };
            if self_closing {
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(element);
                } else if root.is_none() {
                    root = Some(element);
                } else {
                    return Err(LifecycleConfigError::MalformedXml {
                        reason: "multiple root XML elements are not allowed".to_string(),
                    });
                }
            } else {
                stack.push(element);
            }
        }
        index = tag_end + 1;
    }

    if let Some(unclosed) = stack.last() {
        return Err(LifecycleConfigError::MalformedXml {
            reason: format!("unclosed <{}> element", unclosed.name),
        });
    }

    root.ok_or_else(|| LifecycleConfigError::MalformedXml {
        reason: "missing root XML element".to_string(),
    })
}

fn find_tag_end(input: &str, mut index: usize) -> Result<usize, LifecycleConfigError> {
    let bytes = input.as_bytes();
    let mut quote = None;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(expected) = quote {
            if byte == expected {
                quote = None;
            }
        } else {
            match byte {
                b'"' | b'\'' => quote = Some(byte),
                b'>' => return Ok(index),
                _ => {}
            }
        }
        index += 1;
    }
    Err(LifecycleConfigError::MalformedXml {
        reason: "unterminated XML tag".to_string(),
    })
}

fn start_tag_name(inner: &str) -> Result<&str, LifecycleConfigError> {
    let trimmed = inner.trim();
    let end = trimmed
        .find(|character: char| character.is_ascii_whitespace() || character == '/')
        .unwrap_or(trimmed.len());
    if end == 0 {
        return Err(LifecycleConfigError::MalformedXml {
            reason: "missing XML tag name".to_string(),
        });
    }
    Ok(normalize_tag_name(&trimmed[..end]))
}

fn normalize_tag_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

fn xml_unescape(text: &str) -> Result<String, LifecycleConfigError> {
    let mut output = String::with_capacity(text.len());
    let mut chars = text.chars();

    while let Some(character) = chars.next() {
        if character != '&' {
            output.push(character);
            continue;
        }

        let mut entity = String::new();
        loop {
            let Some(next) = chars.next() else {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "unterminated XML entity".to_string(),
                });
            };
            entity.push(next);
            if next == ';' {
                break;
            }
        }

        match entity.as_str() {
            "amp;" => output.push('&'),
            "lt;" => output.push('<'),
            "gt;" => output.push('>'),
            "quot;" => output.push('"'),
            "apos;" => output.push('\''),
            _ => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unsupported XML entity &{entity}"),
                });
            }
        }
    }

    Ok(output)
}

fn xml_escape(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(character),
        }
    }
    output
}

fn valid_calendar_date(year: i32, month: u8, day: u8) -> bool {
    if !(1..=12).contains(&month) || day == 0 {
        return false;
    }
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!(),
    };
    day <= days_in_month
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(xml: &str) -> BucketLifecycleConfiguration {
        let parsed = parse_lifecycle_configuration_xml(xml.as_bytes()).unwrap();
        let rendered = render_lifecycle_configuration_xml(&parsed);
        parse_lifecycle_configuration_xml(rendered.as_bytes()).unwrap()
    }

    #[test]
    fn parse_legacy_prefix_rule_round_trips() {
        let config = round_trip(
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>rule1</ID>\
                    <Prefix>logs/</Prefix>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>7</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        );
        assert_eq!(config.rules.len(), 1);
        let rule = &config.rules[0];
        assert_eq!(rule.id.as_deref(), Some("rule1"));
        assert_eq!(rule.filter.prefix.as_deref(), Some("logs/"));
        assert!(!rule.filter.explicit_filter);
        assert_eq!(
            render_lifecycle_configuration_xml(&config),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>rule1</ID><Prefix>logs/</Prefix><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>"
        );
    }

    #[test]
    fn parse_filter_and_accepts_prefix_tags_and_size() {
        let config = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter>\
                        <And>\
                            <Prefix>logs/</Prefix>\
                            <Tag><Key>env</Key><Value>prod</Value></Tag>\
                            <ObjectSizeGreaterThan>10</ObjectSizeGreaterThan>\
                        </And>\
                    </Filter>\
                    <Expiration><Days>3</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap();
        let rule = &config.rules[0];
        assert_eq!(rule.filter.prefix.as_deref(), Some("logs/"));
        assert_eq!(
            rule.filter.tags,
            vec![LifecycleTag {
                key: "env".to_string(),
                value: "prod".to_string()
            }]
        );
        assert_eq!(rule.filter.object_size_greater_than, Some(10));
        assert!(rule.filter.explicit_filter);
    }

    #[test]
    fn parse_empty_filter() {
        let config = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter/>\
                    <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap();
        assert!(config.rules[0].filter.explicit_filter);
        assert!(config.rules[0].filter.prefix.is_none());
        assert!(config.rules[0].filter.tags.is_empty());
    }

    #[test]
    fn rejects_rule_with_stray_text() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    junk\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_filter_with_stray_text() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter>junk</Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_tag_with_duplicate_key_elements() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter>\
                        <Tag>\
                            <Key>env</Key>\
                            <Key>env2</Key>\
                            <Value>prod</Value>\
                        </Tag>\
                    </Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_tag_with_duplicate_value_elements() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter>\
                        <Tag>\
                            <Key>env</Key>\
                            <Value>prod</Value>\
                            <Value>stage</Value>\
                        </Tag>\
                    </Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_rule_with_both_legacy_prefix_and_filter() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Prefix>logs/</Prefix>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_filter_with_multiple_top_level_predicates() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter>\
                        <Prefix>logs/</Prefix>\
                        <Tag><Key>env</Key><Value>prod</Value></Tag>\
                    </Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_nonstandard_tags_wrapper_in_filter() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter>\
                        <Tags>\
                            <Tag><Key>env</Key><Value>prod</Value></Tag>\
                        </Tags>\
                    </Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_duplicate_rule_ids() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><ID>same</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
                <Rule><ID>same</ID><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_invalid_status_as_malformed_xml() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><Status>enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_zero_days() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_invalid_date() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><Status>Enabled</Status><Expiration><Date>20200101</Date></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_non_midnight_expiration_date_timestamp() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><Status>Enabled</Status><Expiration><Date>2024-01-01T12:34:56Z</Date></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn accepts_midnight_expiration_date_timestamp_without_millis() {
        let config = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><Status>Enabled</Status><Expiration><Date>2024-01-01T00:00:00Z</Date></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap();
        assert_eq!(
            config.rules[0].expiration,
            Some(LifecycleExpiration::Date(LifecycleDate {
                year: 2024,
                month: 1,
                day: 1,
            }))
        );
    }

    #[test]
    fn rejects_expiration_date_with_garbage_suffix() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule><Status>Enabled</Status><Expiration><Date>2024-01-01Tgarbage</Date></Expiration></Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_expired_object_delete_marker_with_tag_filter() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><Tag><Key>env</Key><Value>prod</Value></Tag></Filter>\
                    <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_abort_rule_with_tag_filter() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><Tag><Key>env</Key><Value>prod</Value></Tag></Filter>\
                    <AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_abort_rule_with_size_filter() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan></Filter>\
                    <AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn rejects_newer_noncurrent_versions_without_filter() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <NoncurrentVersionExpiration>\
                        <NoncurrentDays>1</NoncurrentDays>\
                        <NewerNoncurrentVersions>2</NewerNoncurrentVersions>\
                    </NoncurrentVersionExpiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn filter_matches_object() {
        let filter = LifecycleRuleFilter {
            prefix: Some("logs/".to_string()),
            tags: vec![LifecycleTag {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            object_size_greater_than: Some(10),
            object_size_less_than: Some(100),
            explicit_filter: true,
        };
        let tags = vec![("env".to_string(), "prod".to_string())];
        assert!(filter.matches_object("logs/app.txt", &tags, 50));
        assert!(!filter.matches_object("tmp/app.txt", &tags, 50));
        assert!(!filter.matches_object("logs/app.txt", &tags, 5));
        assert!(!filter.matches_object("logs/app.txt", &tags, 100));
    }

    #[test]
    fn filter_matches_multipart_upload_requires_prefix_only() {
        let prefix_only = LifecycleRuleFilter {
            prefix: Some("logs/".to_string()),
            tags: Vec::new(),
            object_size_greater_than: None,
            object_size_less_than: None,
            explicit_filter: true,
        };
        assert!(prefix_only.matches_multipart_upload("logs/app"));

        let with_tags = LifecycleRuleFilter {
            prefix: Some("logs/".to_string()),
            tags: vec![LifecycleTag {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            object_size_greater_than: None,
            object_size_less_than: None,
            explicit_filter: true,
        };
        assert!(!with_tags.matches_multipart_upload("logs/app"));
    }
}
