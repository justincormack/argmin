use std::collections::HashSet;
use std::num::NonZeroU32;

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

use crate::{validate_tag_key_length, validate_tag_value_length};

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LifecycleConfigError {
    #[error("malformed XML: {reason}")]
    MalformedXml { reason: String },

    #[error("invalid request: {reason}")]
    InvalidRequest { reason: String },

    #[error("invalid request: {reason}")]
    LifecycleV2Required { reason: String },

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
    prefix: Option<String>,
    tags: Vec<LifecycleTag>,
    object_size_greater_than: Option<u64>,
    object_size_less_than: Option<u64>,
    explicit_filter: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LifecycleObjectSizeRange {
    greater_than: Option<u64>,
    less_than: Option<u64>,
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
    pub fn legacy_prefix(prefix: impl Into<String>) -> Result<Self, LifecycleConfigError> {
        let prefix = prefix.into();
        validate_prefix(&prefix)?;
        Ok(Self {
            prefix: Some(prefix),
            ..Self::default()
        })
    }

    #[must_use]
    pub fn explicit_empty() -> Self {
        Self {
            explicit_filter: true,
            ..Self::default()
        }
    }

    pub fn explicit_with_predicates(
        prefix: Option<String>,
        tags: Vec<LifecycleTag>,
        object_size_range: LifecycleObjectSizeRange,
    ) -> Result<Self, LifecycleConfigError> {
        if let Some(prefix) = &prefix {
            validate_prefix(prefix)?;
        }
        validate_tag_filters(&tags)?;
        Ok(Self {
            prefix,
            tags,
            object_size_greater_than: object_size_range.greater_than,
            object_size_less_than: object_size_range.less_than,
            explicit_filter: true,
        })
    }

    pub fn explicit_prefix(prefix: impl Into<String>) -> Result<Self, LifecycleConfigError> {
        Self::explicit_with_predicates(
            Some(prefix.into()),
            Vec::new(),
            LifecycleObjectSizeRange::default(),
        )
    }

    pub fn explicit_tag(tag: LifecycleTag) -> Result<Self, LifecycleConfigError> {
        Self::explicit_with_predicates(None, vec![tag], LifecycleObjectSizeRange::default())
    }

    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    #[must_use]
    pub fn tags(&self) -> &[LifecycleTag] {
        &self.tags
    }

    #[must_use]
    pub fn object_size_greater_than(&self) -> Option<u64> {
        self.object_size_greater_than
    }

    #[must_use]
    pub fn object_size_less_than(&self) -> Option<u64> {
        self.object_size_less_than
    }

    #[must_use]
    pub fn is_explicit_filter(&self) -> bool {
        self.explicit_filter
    }

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

impl LifecycleObjectSizeRange {
    pub fn new(
        greater_than: Option<u64>,
        less_than: Option<u64>,
    ) -> Result<Self, LifecycleConfigError> {
        validate_object_size_range(greater_than, less_than)?;
        Ok(Self {
            greater_than,
            less_than,
        })
    }

    pub fn greater_than(value: u64) -> Self {
        Self {
            greater_than: Some(value),
            less_than: None,
        }
    }

    pub fn less_than(value: u64) -> Self {
        Self {
            greater_than: None,
            less_than: Some(value),
        }
    }

    #[must_use]
    pub fn object_size_greater_than(self) -> Option<u64> {
        self.greater_than
    }

    #[must_use]
    pub fn object_size_less_than(self) -> Option<u64> {
        self.less_than
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
    let rules = LifecycleXmlParser::parse(data)?;
    validate_configuration(&rules)?;
    Ok(BucketLifecycleConfiguration { rules })
}

#[must_use]
pub fn render_lifecycle_configuration_xml(config: &BucketLifecycleConfiguration) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
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

    let has_concrete_predicate =
        filter.prefix.is_some() || !filter.tags.is_empty() || filter.has_size_filter();
    if !has_concrete_predicate {
        xml.push_str("<Filter/>");
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

    if simple_prefix_only {
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

    let has_explicit_filter = rules.iter().any(|rule| rule.filter.explicit_filter);
    let has_legacy_prefix = rules
        .iter()
        .any(|rule| !rule.filter.explicit_filter && rule.filter.prefix.is_some());
    if has_explicit_filter && has_legacy_prefix {
        return Err(LifecycleConfigError::InvalidRequest {
            reason:
                "Base level prefix cannot be used in Lifecycle V2, prefixes are only supported in the Filter."
                    .to_string(),
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
            return Err(LifecycleConfigError::InvalidRequest {
                reason: "ExpiredObjectDeleteMarker cannot be specified with Tags.".to_string(),
            });
        }

        if rule.abort_incomplete_multipart_upload.is_some() && rule.filter.has_tag_filter() {
            return Err(LifecycleConfigError::InvalidRequest {
                reason: "AbortIncompleteMultipartUpload cannot be specified with Tags.".to_string(),
            });
        }

        if rule.abort_incomplete_multipart_upload.is_some() && rule.filter.has_size_filter() {
            return Err(LifecycleConfigError::InvalidRequest {
                reason: "AbortIncompleteMultipartUpload cannot be specified with Object Size."
                    .to_string(),
            });
        }

        if let Some(noncurrent) = &rule.noncurrent_version_expiration {
            if noncurrent.newer_noncurrent_versions.is_some() && !rule.filter.explicit_filter {
                if rule.filter.has_scope() {
                    return Err(LifecycleConfigError::LifecycleV2Required {
                        reason: "NewerNoncurrentVersions element can only be used in Lifecycle V2."
                            .to_string(),
                    });
                }
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "NewerNoncurrentVersions requires an explicit lifecycle filter"
                        .to_string(),
                });
            }
        }

        validate_object_size_range(
            rule.filter.object_size_greater_than,
            rule.filter.object_size_less_than,
        )?;
    }

    Ok(())
}

fn validate_object_size_range(
    object_size_greater_than: Option<u64>,
    object_size_less_than: Option<u64>,
) -> Result<(), LifecycleConfigError> {
    if let (Some(min_size), Some(max_size)) = (object_size_greater_than, object_size_less_than) {
        if min_size >= max_size {
            return Err(LifecycleConfigError::InvalidRequest {
                reason:
                    "'ObjectSizeLessThan' has to be a value greater than 'ObjectSizeGreaterThan'."
                        .to_string(),
            });
        }
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), LifecycleConfigError> {
    if prefix.len() > 1024 {
        return Err(LifecycleConfigError::InvalidRequest {
            reason: "The maximum size of a prefix is 1024".to_string(),
        });
    }
    Ok(())
}

fn validate_tag_key(key: &str) -> Result<(), LifecycleConfigError> {
    validate_tag_key_length(key).map_err(|_| LifecycleConfigError::InvalidRequest {
        reason: "A Tag's Key must be a length between 1 and 128.".to_string(),
    })
}

fn validate_tag_value(value: &str) -> Result<(), LifecycleConfigError> {
    validate_tag_value_length(value).map_err(|_| LifecycleConfigError::InvalidRequest {
        reason: "A Tag's Value must be a length between 0 and 256.".to_string(),
    })
}

fn validate_tag_filters(tags: &[LifecycleTag]) -> Result<(), LifecycleConfigError> {
    let mut seen = HashSet::new();
    for tag in tags {
        validate_tag_key(&tag.key)?;
        validate_tag_value(&tag.value)?;
        if !seen.insert(tag.key.as_str()) {
            return Err(LifecycleConfigError::InvalidArgument {
                reason: format!("duplicate lifecycle tag filter key {}", tag.key),
            });
        }
    }
    Ok(())
}

const MAX_LIFECYCLE_XML_DEPTH: usize = 32;

struct LifecycleXmlParser {
    frames: Vec<LifecycleXmlFrame>,
    rules: Option<Vec<LifecycleRule>>,
}

enum LifecycleXmlFrame {
    Configuration { rules: Vec<LifecycleRule> },
    Rule(RuleBuilder),
    Filter(FilterBuilder),
    And(AndBuilder),
    Tag(TagBuilder),
    Expiration(ExpirationBuilder),
    NoncurrentVersionExpiration(NoncurrentVersionExpirationBuilder),
    AbortIncompleteMultipartUpload(AbortIncompleteMultipartUploadBuilder),
    Text { target: TextTarget, value: String },
}

#[derive(Default)]
struct RuleBuilder {
    id: Option<String>,
    status: Option<LifecycleRuleStatus>,
    legacy_prefix: Option<String>,
    filter: LifecycleRuleFilter,
    expiration: Option<LifecycleExpiration>,
    noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
    abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

struct FilterBuilder {
    filter: LifecycleRuleFilter,
    predicate: Option<&'static str>,
}

#[derive(Default)]
struct AndBuilder {
    filter: LifecycleRuleFilter,
}

#[derive(Default)]
struct TagBuilder {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Default)]
struct ExpirationBuilder {
    days: Option<NonZeroU32>,
    date: Option<LifecycleDate>,
    delete_marker: Option<bool>,
}

#[derive(Default)]
struct NoncurrentVersionExpirationBuilder {
    noncurrent_days: Option<NonZeroU32>,
    newer_noncurrent_versions: Option<NonZeroU32>,
}

#[derive(Default)]
struct AbortIncompleteMultipartUploadBuilder {
    days: Option<NonZeroU32>,
}

#[derive(Clone, Copy)]
enum TextTarget {
    RuleId,
    RuleStatus,
    RuleLegacyPrefix,
    FilterPrefix,
    FilterObjectSizeGreaterThan,
    FilterObjectSizeLessThan,
    AndPrefix,
    AndObjectSizeGreaterThan,
    AndObjectSizeLessThan,
    TagKey,
    TagValue,
    ExpirationDays,
    ExpirationDate,
    ExpirationDeleteMarker,
    NoncurrentDays,
    NewerNoncurrentVersions,
    DaysAfterInitiation,
}

enum CompletedLifecycleElement {
    Configuration(Vec<LifecycleRule>),
    Rule(LifecycleRule),
    Filter(LifecycleRuleFilter),
    And(LifecycleRuleFilter),
    Tag(LifecycleTag),
    Expiration(LifecycleExpiration),
    NoncurrentVersionExpiration(NoncurrentVersionExpiration),
    AbortIncompleteMultipartUpload(AbortIncompleteMultipartUpload),
    Text(TextTarget, String),
}

impl LifecycleXmlParser {
    fn parse(data: &[u8]) -> Result<Vec<LifecycleRule>, LifecycleConfigError> {
        std::str::from_utf8(data).map_err(|_| LifecycleConfigError::MalformedXml {
            reason: "XML body is not valid UTF-8".to_string(),
        })?;

        let mut parser = Self {
            frames: Vec::new(),
            rules: None,
        };
        let mut reader = Reader::from_reader(data);
        let mut buffer = Vec::new();

        loop {
            match reader.read_event_into(&mut buffer) {
                Ok(Event::Start(element)) => parser.open(&element)?,
                Ok(Event::Empty(element)) => {
                    parser.open(&element)?;
                    parser.close(local_element_name(element.local_name().as_ref())?)?;
                }
                Ok(Event::End(element)) => {
                    parser.close(local_element_name(element.local_name().as_ref())?)?;
                }
                Ok(event @ (Event::Text(_) | Event::GeneralRef(_))) => {
                    parser.text_event(event)?;
                }
                Ok(Event::Comment(_) | Event::Decl(_) | Event::PI(_)) => {}
                Ok(Event::CData(_) | Event::DocType(_)) => {
                    return Err(LifecycleConfigError::MalformedXml {
                        reason: "unsupported XML declaration".to_string(),
                    });
                }
                Ok(Event::Eof) => break,
                Err(error) => {
                    return Err(LifecycleConfigError::MalformedXml {
                        reason: format!("invalid lifecycle XML: {error}"),
                    });
                }
            }
            buffer.clear();
        }

        if let Some(unclosed) = parser.frames.last() {
            return Err(LifecycleConfigError::MalformedXml {
                reason: format!("unclosed <{}> element", unclosed.element_name()),
            });
        }
        parser
            .rules
            .ok_or_else(|| LifecycleConfigError::MalformedXml {
                reason: "missing root XML element".to_string(),
            })
    }

    fn open(&mut self, element: &BytesStart<'_>) -> Result<(), LifecycleConfigError> {
        validate_lifecycle_xml_attributes(element)?;
        if self.frames.len() >= MAX_LIFECYCLE_XML_DEPTH {
            return Err(LifecycleConfigError::MalformedXml {
                reason: format!(
                    "lifecycle XML nesting depth exceeds {MAX_LIFECYCLE_XML_DEPTH} elements"
                ),
            });
        }

        let local_name = element.local_name();
        let name = local_element_name(local_name.as_ref())?;
        let frame = match self.frames.last_mut() {
            None if self.rules.is_some() => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "multiple root XML elements are not allowed".to_string(),
                });
            }
            None if name == "LifecycleConfiguration" => {
                LifecycleXmlFrame::Configuration { rules: Vec::new() }
            }
            None => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "missing LifecycleConfiguration element".to_string(),
                });
            }
            Some(LifecycleXmlFrame::Configuration { .. }) if name == "Rule" => {
                LifecycleXmlFrame::Rule(RuleBuilder::default())
            }
            Some(LifecycleXmlFrame::Configuration { .. }) => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("unexpected <{name}> in LifecycleConfiguration"),
                });
            }
            Some(LifecycleXmlFrame::Rule(builder)) => builder.open_child(name)?,
            Some(LifecycleXmlFrame::Filter(builder)) => builder.open_child(name)?,
            Some(LifecycleXmlFrame::And(builder)) => builder.open_child(name)?,
            Some(LifecycleXmlFrame::Tag(_)) => match name {
                "Key" => LifecycleXmlFrame::text(TextTarget::TagKey),
                "Value" => LifecycleXmlFrame::text(TextTarget::TagValue),
                _ => return Err(unexpected_element(name, "lifecycle Tag")),
            },
            Some(LifecycleXmlFrame::Expiration(_)) => match name {
                "Days" => LifecycleXmlFrame::text(TextTarget::ExpirationDays),
                "Date" => LifecycleXmlFrame::text(TextTarget::ExpirationDate),
                "ExpiredObjectDeleteMarker" => {
                    LifecycleXmlFrame::text(TextTarget::ExpirationDeleteMarker)
                }
                _ => return Err(unexpected_element(name, "Expiration")),
            },
            Some(LifecycleXmlFrame::NoncurrentVersionExpiration(_)) => match name {
                "NoncurrentDays" => LifecycleXmlFrame::text(TextTarget::NoncurrentDays),
                "NewerNoncurrentVersions" => {
                    LifecycleXmlFrame::text(TextTarget::NewerNoncurrentVersions)
                }
                _ => return Err(unexpected_element(name, "NoncurrentVersionExpiration")),
            },
            Some(LifecycleXmlFrame::AbortIncompleteMultipartUpload(_)) => match name {
                "DaysAfterInitiation" => LifecycleXmlFrame::text(TextTarget::DaysAfterInitiation),
                _ => return Err(unexpected_element(name, "AbortIncompleteMultipartUpload")),
            },
            Some(LifecycleXmlFrame::Text { .. }) => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!(
                        "<{}> must not contain child elements",
                        self.frames
                            .last()
                            .map_or("text", LifecycleXmlFrame::element_name)
                    ),
                });
            }
        };
        self.frames.push(frame);
        Ok(())
    }

    fn close(&mut self, name: &str) -> Result<(), LifecycleConfigError> {
        let frame = self
            .frames
            .pop()
            .ok_or_else(|| LifecycleConfigError::MalformedXml {
                reason: format!("unexpected closing </{name}>"),
            })?;
        if frame.element_name() != name {
            return Err(LifecycleConfigError::MalformedXml {
                reason: format!(
                    "closing tag </{name}> does not match <{}>",
                    frame.element_name()
                ),
            });
        }
        let completed = frame.finish()?;
        if let Some(parent) = self.frames.last_mut() {
            parent.accept(completed)
        } else {
            let CompletedLifecycleElement::Configuration(rules) = completed else {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "missing LifecycleConfiguration element".to_string(),
                });
            };
            self.rules = Some(rules);
            Ok(())
        }
    }

    fn text(&mut self, raw: &[u8]) -> Result<(), LifecycleConfigError> {
        let raw = std::str::from_utf8(raw).map_err(|_| LifecycleConfigError::MalformedXml {
            reason: "XML body is not valid UTF-8".to_string(),
        })?;
        let value = xml_unescape(raw)?;
        match self.frames.last_mut() {
            Some(LifecycleXmlFrame::Text { value: text, .. }) => text.push_str(&value),
            Some(_) if value.trim().is_empty() => {}
            Some(frame) => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: format!("<{}> must not contain text content", frame.element_name()),
                });
            }
            None if value.trim().is_empty() => {}
            None => {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "unexpected text outside root XML element".to_string(),
                });
            }
        }
        Ok(())
    }

    fn text_event(&mut self, event: Event<'_>) -> Result<(), LifecycleConfigError> {
        match event {
            Event::Text(text) => self.text(text.as_ref()),
            Event::GeneralRef(reference) => {
                let mut escaped = Vec::with_capacity(reference.len() + 2);
                escaped.push(b'&');
                escaped.extend_from_slice(reference.as_ref());
                escaped.push(b';');
                self.text(&escaped)
            }
            _ => unreachable!("text_event only accepts text and reference events"),
        }
    }
}

impl LifecycleXmlFrame {
    fn text(target: TextTarget) -> Self {
        Self::Text {
            target,
            value: String::new(),
        }
    }

    fn element_name(&self) -> &'static str {
        match self {
            Self::Configuration { .. } => "LifecycleConfiguration",
            Self::Rule(_) => "Rule",
            Self::Filter(_) => "Filter",
            Self::And(_) => "And",
            Self::Tag(_) => "Tag",
            Self::Expiration(_) => "Expiration",
            Self::NoncurrentVersionExpiration(_) => "NoncurrentVersionExpiration",
            Self::AbortIncompleteMultipartUpload(_) => "AbortIncompleteMultipartUpload",
            Self::Text { target, .. } => target.element_name(),
        }
    }

    fn finish(self) -> Result<CompletedLifecycleElement, LifecycleConfigError> {
        match self {
            Self::Configuration { rules } => Ok(CompletedLifecycleElement::Configuration(rules)),
            Self::Rule(builder) => builder.finish().map(CompletedLifecycleElement::Rule),
            Self::Filter(builder) => Ok(CompletedLifecycleElement::Filter(builder.filter)),
            Self::And(builder) => Ok(CompletedLifecycleElement::And(builder.filter)),
            Self::Tag(builder) => builder.finish().map(CompletedLifecycleElement::Tag),
            Self::Expiration(builder) => {
                builder.finish().map(CompletedLifecycleElement::Expiration)
            }
            Self::NoncurrentVersionExpiration(builder) => builder
                .finish()
                .map(CompletedLifecycleElement::NoncurrentVersionExpiration),
            Self::AbortIncompleteMultipartUpload(builder) => builder
                .finish()
                .map(CompletedLifecycleElement::AbortIncompleteMultipartUpload),
            Self::Text { target, value } => Ok(CompletedLifecycleElement::Text(target, value)),
        }
    }

    fn accept(&mut self, completed: CompletedLifecycleElement) -> Result<(), LifecycleConfigError> {
        match (self, completed) {
            (Self::Configuration { rules }, CompletedLifecycleElement::Rule(rule)) => {
                rules.push(rule);
                Ok(())
            }
            (Self::Rule(builder), completed) => builder.accept(completed),
            (Self::Filter(builder), completed) => builder.accept(completed),
            (Self::And(builder), completed) => builder.accept(completed),
            (Self::Tag(builder), CompletedLifecycleElement::Text(target, value)) => {
                builder.accept(target, value)
            }
            (Self::Expiration(builder), CompletedLifecycleElement::Text(target, value)) => {
                builder.accept(target, &value)
            }
            (
                Self::NoncurrentVersionExpiration(builder),
                CompletedLifecycleElement::Text(target, value),
            ) => builder.accept(target, &value),
            (
                Self::AbortIncompleteMultipartUpload(builder),
                CompletedLifecycleElement::Text(target, value),
            ) => builder.accept(target, &value),
            (parent, child) => Err(LifecycleConfigError::MalformedXml {
                reason: format!(
                    "unexpected <{}> in <{}>",
                    child.element_name(),
                    parent.element_name()
                ),
            }),
        }
    }
}

impl CompletedLifecycleElement {
    fn element_name(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "LifecycleConfiguration",
            Self::Rule(_) => "Rule",
            Self::Filter(_) => "Filter",
            Self::And(_) => "And",
            Self::Tag(_) => "Tag",
            Self::Expiration(_) => "Expiration",
            Self::NoncurrentVersionExpiration(_) => "NoncurrentVersionExpiration",
            Self::AbortIncompleteMultipartUpload(_) => "AbortIncompleteMultipartUpload",
            Self::Text(target, _) => target.element_name(),
        }
    }
}

impl TextTarget {
    fn element_name(self) -> &'static str {
        match self {
            Self::RuleId => "ID",
            Self::RuleStatus => "Status",
            Self::RuleLegacyPrefix | Self::FilterPrefix | Self::AndPrefix => "Prefix",
            Self::FilterObjectSizeGreaterThan | Self::AndObjectSizeGreaterThan => {
                "ObjectSizeGreaterThan"
            }
            Self::FilterObjectSizeLessThan | Self::AndObjectSizeLessThan => "ObjectSizeLessThan",
            Self::TagKey => "Key",
            Self::TagValue => "Value",
            Self::ExpirationDays => "Days",
            Self::ExpirationDate => "Date",
            Self::ExpirationDeleteMarker => "ExpiredObjectDeleteMarker",
            Self::NoncurrentDays => "NoncurrentDays",
            Self::NewerNoncurrentVersions => "NewerNoncurrentVersions",
            Self::DaysAfterInitiation => "DaysAfterInitiation",
        }
    }
}

impl RuleBuilder {
    fn open_child(&mut self, name: &str) -> Result<LifecycleXmlFrame, LifecycleConfigError> {
        match name {
            "ID" => Ok(LifecycleXmlFrame::text(TextTarget::RuleId)),
            "Status" => Ok(LifecycleXmlFrame::text(TextTarget::RuleStatus)),
            "Prefix" => Ok(LifecycleXmlFrame::text(TextTarget::RuleLegacyPrefix)),
            "Filter" if self.filter.explicit_filter => Err(LifecycleConfigError::MalformedXml {
                reason: "duplicate Filter element in lifecycle rule".to_string(),
            }),
            "Filter" => Ok(LifecycleXmlFrame::Filter(FilterBuilder::new())),
            "Expiration" => Ok(LifecycleXmlFrame::Expiration(ExpirationBuilder::default())),
            "NoncurrentVersionExpiration" => Ok(LifecycleXmlFrame::NoncurrentVersionExpiration(
                NoncurrentVersionExpirationBuilder::default(),
            )),
            "AbortIncompleteMultipartUpload" => {
                Ok(LifecycleXmlFrame::AbortIncompleteMultipartUpload(
                    AbortIncompleteMultipartUploadBuilder::default(),
                ))
            }
            "Transition"
            | "Transitions"
            | "NoncurrentVersionTransition"
            | "NoncurrentVersionTransitions" => Err(LifecycleConfigError::NotImplemented {
                feature: "Lifecycle transition rules".to_string(),
            }),
            _ => Err(unexpected_element(name, "lifecycle rule")),
        }
    }

    fn accept(&mut self, completed: CompletedLifecycleElement) -> Result<(), LifecycleConfigError> {
        match completed {
            CompletedLifecycleElement::Text(TextTarget::RuleId, value) => {
                set_once(&mut self.id, value.trim().to_string(), "ID")
            }
            CompletedLifecycleElement::Text(TextTarget::RuleStatus, value) => {
                let status = match value.trim() {
                    "Enabled" => LifecycleRuleStatus::Enabled,
                    "Disabled" => LifecycleRuleStatus::Disabled,
                    _ => {
                        return Err(LifecycleConfigError::MalformedXml {
                            reason: "lifecycle rule Status must be Enabled or Disabled".to_string(),
                        });
                    }
                };
                set_once(&mut self.status, status, "Status")
            }
            CompletedLifecycleElement::Text(TextTarget::RuleLegacyPrefix, value) => {
                set_once(&mut self.legacy_prefix, value.trim().to_string(), "Prefix")
            }
            CompletedLifecycleElement::Filter(filter) => {
                self.filter = filter;
                Ok(())
            }
            CompletedLifecycleElement::Expiration(expiration) => {
                set_once(&mut self.expiration, expiration, "Expiration")
            }
            CompletedLifecycleElement::NoncurrentVersionExpiration(expiration) => set_once(
                &mut self.noncurrent_version_expiration,
                expiration,
                "NoncurrentVersionExpiration",
            ),
            CompletedLifecycleElement::AbortIncompleteMultipartUpload(abort) => set_once(
                &mut self.abort_incomplete_multipart_upload,
                abort,
                "AbortIncompleteMultipartUpload",
            ),
            other => Err(unexpected_element(other.element_name(), "lifecycle rule")),
        }
    }

    fn finish(mut self) -> Result<LifecycleRule, LifecycleConfigError> {
        if let Some(prefix) = self.legacy_prefix {
            if self.filter.explicit_filter {
                return Err(LifecycleConfigError::MalformedXml {
                    reason: "lifecycle rule may not contain both Prefix and Filter".to_string(),
                });
            }
            validate_prefix(&prefix)?;
            self.filter.prefix = Some(prefix);
        }
        let status = self
            .status
            .ok_or_else(|| LifecycleConfigError::MalformedXml {
                reason: "lifecycle rule is missing Status".to_string(),
            })?;
        Ok(LifecycleRule {
            id: self.id,
            status,
            filter: self.filter,
            expiration: self.expiration,
            noncurrent_version_expiration: self.noncurrent_version_expiration,
            abort_incomplete_multipart_upload: self.abort_incomplete_multipart_upload,
        })
    }
}

impl FilterBuilder {
    fn new() -> Self {
        Self {
            filter: LifecycleRuleFilter {
                explicit_filter: true,
                ..LifecycleRuleFilter::default()
            },
            predicate: None,
        }
    }

    fn open_child(&mut self, name: &str) -> Result<LifecycleXmlFrame, LifecycleConfigError> {
        let (predicate, frame) = match name {
            "Prefix" => ("Prefix", LifecycleXmlFrame::text(TextTarget::FilterPrefix)),
            "Tag" => ("Tag", LifecycleXmlFrame::Tag(TagBuilder::default())),
            "And" => ("And", LifecycleXmlFrame::And(AndBuilder::default())),
            "ObjectSizeGreaterThan" => (
                "ObjectSizeGreaterThan",
                LifecycleXmlFrame::text(TextTarget::FilterObjectSizeGreaterThan),
            ),
            "ObjectSizeLessThan" => (
                "ObjectSizeLessThan",
                LifecycleXmlFrame::text(TextTarget::FilterObjectSizeLessThan),
            ),
            _ => return Err(unexpected_element(name, "lifecycle filter")),
        };
        mark_filter_predicate(&mut self.predicate, predicate)?;
        Ok(frame)
    }

    fn accept(&mut self, completed: CompletedLifecycleElement) -> Result<(), LifecycleConfigError> {
        match completed {
            CompletedLifecycleElement::Text(TextTarget::FilterPrefix, value) => {
                let value = value.trim().to_string();
                validate_prefix(&value)?;
                set_once(&mut self.filter.prefix, value, "Prefix")
            }
            CompletedLifecycleElement::Text(TextTarget::FilterObjectSizeGreaterThan, value) => {
                set_once(
                    &mut self.filter.object_size_greater_than,
                    parse_u64_text(&value, "ObjectSizeGreaterThan")?,
                    "ObjectSizeGreaterThan",
                )
            }
            CompletedLifecycleElement::Text(TextTarget::FilterObjectSizeLessThan, value) => {
                set_once(
                    &mut self.filter.object_size_less_than,
                    parse_u64_text(&value, "ObjectSizeLessThan")?,
                    "ObjectSizeLessThan",
                )
            }
            CompletedLifecycleElement::Tag(tag) => self.filter.add_tag(tag),
            CompletedLifecycleElement::And(and) => {
                self.filter.prefix = and.prefix;
                self.filter.tags = and.tags;
                self.filter.object_size_greater_than = and.object_size_greater_than;
                self.filter.object_size_less_than = and.object_size_less_than;
                Ok(())
            }
            other => Err(unexpected_element(other.element_name(), "lifecycle filter")),
        }
    }
}

impl AndBuilder {
    fn open_child(&mut self, name: &str) -> Result<LifecycleXmlFrame, LifecycleConfigError> {
        match name {
            "Prefix" => Ok(LifecycleXmlFrame::text(TextTarget::AndPrefix)),
            "Tag" => Ok(LifecycleXmlFrame::Tag(TagBuilder::default())),
            "ObjectSizeGreaterThan" => Ok(LifecycleXmlFrame::text(
                TextTarget::AndObjectSizeGreaterThan,
            )),
            "ObjectSizeLessThan" => Ok(LifecycleXmlFrame::text(TextTarget::AndObjectSizeLessThan)),
            _ => Err(unexpected_element(name, "lifecycle filter And")),
        }
    }

    fn accept(&mut self, completed: CompletedLifecycleElement) -> Result<(), LifecycleConfigError> {
        match completed {
            CompletedLifecycleElement::Text(TextTarget::AndPrefix, value) => {
                let value = value.trim().to_string();
                validate_prefix(&value)?;
                set_once(&mut self.filter.prefix, value, "Prefix")
            }
            CompletedLifecycleElement::Text(TextTarget::AndObjectSizeGreaterThan, value) => {
                set_once(
                    &mut self.filter.object_size_greater_than,
                    parse_u64_text(&value, "ObjectSizeGreaterThan")?,
                    "ObjectSizeGreaterThan",
                )
            }
            CompletedLifecycleElement::Text(TextTarget::AndObjectSizeLessThan, value) => set_once(
                &mut self.filter.object_size_less_than,
                parse_u64_text(&value, "ObjectSizeLessThan")?,
                "ObjectSizeLessThan",
            ),
            CompletedLifecycleElement::Tag(tag) => self.filter.add_tag(tag),
            other => Err(unexpected_element(
                other.element_name(),
                "lifecycle filter And",
            )),
        }
    }
}

impl TagBuilder {
    fn accept(&mut self, target: TextTarget, value: String) -> Result<(), LifecycleConfigError> {
        match target {
            TextTarget::TagKey => set_once(&mut self.key, value.trim().to_string(), "Key"),
            TextTarget::TagValue => set_once(&mut self.value, value.trim().to_string(), "Value"),
            _ => Err(unexpected_element(target.element_name(), "lifecycle Tag")),
        }
    }

    fn finish(self) -> Result<LifecycleTag, LifecycleConfigError> {
        let key = self.key.ok_or_else(|| LifecycleConfigError::MalformedXml {
            reason: "missing <Key> in <Tag>".to_string(),
        })?;
        let value = self
            .value
            .ok_or_else(|| LifecycleConfigError::MalformedXml {
                reason: "missing <Value> in <Tag>".to_string(),
            })?;
        validate_tag_key(&key)?;
        validate_tag_value(&value)?;
        Ok(LifecycleTag { key, value })
    }
}

impl ExpirationBuilder {
    fn accept(&mut self, target: TextTarget, value: &str) -> Result<(), LifecycleConfigError> {
        match target {
            TextTarget::ExpirationDays => set_once(
                &mut self.days,
                parse_non_zero_u32_text(value, "Days")?,
                "Days",
            ),
            TextTarget::ExpirationDate => {
                set_once(&mut self.date, parse_lifecycle_date(value.trim())?, "Date")
            }
            TextTarget::ExpirationDeleteMarker => set_once(
                &mut self.delete_marker,
                parse_bool_text(value, "ExpiredObjectDeleteMarker")?,
                "ExpiredObjectDeleteMarker",
            ),
            _ => Err(unexpected_element(target.element_name(), "Expiration")),
        }
    }

    fn finish(self) -> Result<LifecycleExpiration, LifecycleConfigError> {
        match (self.days, self.date, self.delete_marker) {
            (Some(_), Some(_), _) => Err(LifecycleConfigError::InvalidArgument {
                reason: "Expiration may not specify both Days and Date".to_string(),
            }),
            (Some(_), _, Some(true)) | (_, Some(_), Some(true)) => {
                Err(LifecycleConfigError::MalformedXml {
                    reason: "ExpiredObjectDeleteMarker may not be combined with Days or Date"
                        .to_string(),
                })
            }
            (Some(days), None, _) => Ok(LifecycleExpiration::Days(days)),
            (None, Some(date), _) => Ok(LifecycleExpiration::Date(date)),
            (None, None, Some(true)) => Ok(LifecycleExpiration::ExpiredObjectDeleteMarker),
            _ => Err(LifecycleConfigError::InvalidArgument {
                reason: "Expiration must contain Days, Date, or ExpiredObjectDeleteMarker"
                    .to_string(),
            }),
        }
    }
}

impl NoncurrentVersionExpirationBuilder {
    fn accept(&mut self, target: TextTarget, value: &str) -> Result<(), LifecycleConfigError> {
        match target {
            TextTarget::NoncurrentDays => set_once(
                &mut self.noncurrent_days,
                parse_non_zero_u32_text(value, "NoncurrentDays")?,
                "NoncurrentDays",
            ),
            TextTarget::NewerNoncurrentVersions => {
                let value = parse_non_zero_u32_text(value, "NewerNoncurrentVersions")?;
                if value.get() > 100 {
                    return Err(LifecycleConfigError::InvalidArgument {
                        reason: "NewerNoncurrentVersions must be between 1 and 100".to_string(),
                    });
                }
                set_once(
                    &mut self.newer_noncurrent_versions,
                    value,
                    "NewerNoncurrentVersions",
                )
            }
            _ => Err(unexpected_element(
                target.element_name(),
                "NoncurrentVersionExpiration",
            )),
        }
    }

    fn finish(self) -> Result<NoncurrentVersionExpiration, LifecycleConfigError> {
        Ok(NoncurrentVersionExpiration {
            noncurrent_days: self.noncurrent_days.ok_or_else(|| {
                LifecycleConfigError::InvalidArgument {
                    reason: "NoncurrentVersionExpiration must contain NoncurrentDays".to_string(),
                }
            })?,
            newer_noncurrent_versions: self.newer_noncurrent_versions,
        })
    }
}

impl AbortIncompleteMultipartUploadBuilder {
    fn accept(&mut self, target: TextTarget, value: &str) -> Result<(), LifecycleConfigError> {
        match target {
            TextTarget::DaysAfterInitiation => set_once(
                &mut self.days,
                parse_non_zero_u32_text(value, "DaysAfterInitiation")?,
                "DaysAfterInitiation",
            ),
            _ => Err(unexpected_element(
                target.element_name(),
                "AbortIncompleteMultipartUpload",
            )),
        }
    }

    fn finish(self) -> Result<AbortIncompleteMultipartUpload, LifecycleConfigError> {
        Ok(AbortIncompleteMultipartUpload {
            days_after_initiation: self.days.ok_or_else(|| {
                LifecycleConfigError::InvalidArgument {
                    reason: "AbortIncompleteMultipartUpload must contain DaysAfterInitiation"
                        .to_string(),
                }
            })?,
        })
    }
}

fn unexpected_element(name: &str, parent: &str) -> LifecycleConfigError {
    LifecycleConfigError::MalformedXml {
        reason: format!("unexpected <{name}> in {parent}"),
    }
}

fn local_element_name(name: &[u8]) -> Result<&str, LifecycleConfigError> {
    std::str::from_utf8(name).map_err(|_| LifecycleConfigError::MalformedXml {
        reason: "XML element name is not valid UTF-8".to_string(),
    })
}

fn validate_lifecycle_xml_attributes(element: &BytesStart<'_>) -> Result<(), LifecycleConfigError> {
    for attribute in element.attributes() {
        attribute.map_err(|error| LifecycleConfigError::MalformedXml {
            reason: format!("invalid lifecycle XML attribute: {error}"),
        })?;
    }
    Ok(())
}

fn parse_lifecycle_date(text: &str) -> Result<LifecycleDate, LifecycleConfigError> {
    let text = text.trim();
    let date_part = if text.len() == 10 {
        text
    } else {
        let (date_part, time_part) = text
            .split_once('T')
            .ok_or_else(|| malformed_lifecycle_date(text))?;
        validate_lifecycle_midnight_utc(time_part, text)?;
        date_part
    };
    if date_part.len() != 10 || !date_part.is_ascii() {
        return Err(malformed_lifecycle_date(text));
    }
    if &date_part[4..5] != "-" || &date_part[7..8] != "-" {
        return Err(malformed_lifecycle_date(text));
    }
    let year: i32 = date_part[0..4]
        .parse()
        .map_err(|_| malformed_lifecycle_date(text))?;
    let month: u8 = date_part[5..7]
        .parse()
        .map_err(|_| malformed_lifecycle_date(text))?;
    let day: u8 = date_part[8..10]
        .parse()
        .map_err(|_| malformed_lifecycle_date(text))?;
    if !valid_calendar_date(year, month, day) {
        return Err(malformed_lifecycle_date(text));
    }
    Ok(LifecycleDate { year, month, day })
}

fn validate_lifecycle_midnight_utc(
    time_part: &str,
    full_text: &str,
) -> Result<(), LifecycleConfigError> {
    let Some(time_part) = time_part.strip_suffix('Z') else {
        return Err(malformed_lifecycle_date(full_text));
    };
    let (base_time, fraction) = match time_part.split_once('.') {
        Some((base_time, fraction)) => (base_time, Some(fraction)),
        None => (time_part, None),
    };
    if base_time.len() != 8
        || !base_time.is_ascii()
        || &base_time[2..3] != ":"
        || &base_time[5..6] != ":"
    {
        return Err(malformed_lifecycle_date(full_text));
    }
    let hour: u8 = base_time[0..2]
        .parse()
        .map_err(|_| malformed_lifecycle_date(full_text))?;
    let minute: u8 = base_time[3..5]
        .parse()
        .map_err(|_| malformed_lifecycle_date(full_text))?;
    let second: u8 = base_time[6..8]
        .parse()
        .map_err(|_| malformed_lifecycle_date(full_text))?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(malformed_lifecycle_date(full_text));
    }
    if let Some(fraction) = fraction {
        if fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(malformed_lifecycle_date(full_text));
        }
    }
    if hour != 0 || minute != 0 || second != 0 {
        return Err(invalid_lifecycle_date(full_text));
    }
    if fraction.is_some_and(|fraction| fraction.bytes().any(|byte| byte != b'0')) {
        return Err(invalid_lifecycle_date(full_text));
    }
    Ok(())
}

fn malformed_lifecycle_date(text: &str) -> LifecycleConfigError {
    LifecycleConfigError::MalformedXml {
        reason: format!("invalid lifecycle date {text}"),
    }
}

fn invalid_lifecycle_date(text: &str) -> LifecycleConfigError {
    LifecycleConfigError::InvalidArgument {
        reason: format!("invalid lifecycle date {text}"),
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

fn parse_u64_text(text: &str, field: &str) -> Result<u64, LifecycleConfigError> {
    text.trim()
        .parse()
        .map_err(|_| LifecycleConfigError::InvalidArgument {
            reason: format!("{field} must be a non-negative integer"),
        })
}

fn parse_non_zero_u32_text(text: &str, field: &str) -> Result<NonZeroU32, LifecycleConfigError> {
    let value: u32 = text
        .trim()
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

fn xml_unescape(text: &str) -> Result<String, LifecycleConfigError> {
    quick_xml::escape::unescape(text)
        .map(std::borrow::Cow::into_owned)
        .map_err(|error| LifecycleConfigError::MalformedXml {
            reason: format!("invalid XML entity: {error}"),
        })
}

fn xml_escape(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
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
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>rule1</ID><Prefix>logs/</Prefix><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>"
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
    fn lifecycle_tag_parser_accepts_numeric_character_references() {
        let config = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration><Rule><Filter><Tag><Key>&#x10400;</Key><Value>&#66560;</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>",
        )
        .unwrap();
        assert_eq!(
            config.rules[0].filter.tags,
            vec![LifecycleTag {
                key: "\u{10400}".to_string(),
                value: "\u{10400}".to_string(),
            }]
        );
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
        assert_eq!(
            render_lifecycle_configuration_xml(&config),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><Filter/><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule></LifecycleConfiguration>"
        );
    }

    #[test]
    fn lifecycle_rule_filter_constructors_render_canonical_shapes() {
        let days = NonZeroU32::new(1).unwrap();
        let config = BucketLifecycleConfiguration {
            rules: vec![
                LifecycleRule {
                    id: Some("legacy".to_string()),
                    status: LifecycleRuleStatus::Enabled,
                    filter: LifecycleRuleFilter::legacy_prefix("logs/").unwrap(),
                    expiration: Some(LifecycleExpiration::Days(days)),
                    noncurrent_version_expiration: None,
                    abort_incomplete_multipart_upload: None,
                },
                LifecycleRule {
                    id: Some("explicit".to_string()),
                    status: LifecycleRuleStatus::Enabled,
                    filter: LifecycleRuleFilter::explicit_with_predicates(
                        Some("logs/".to_string()),
                        vec![LifecycleTag {
                            key: "env".to_string(),
                            value: "prod".to_string(),
                        }],
                        LifecycleObjectSizeRange::default(),
                    )
                    .unwrap(),
                    expiration: Some(LifecycleExpiration::Days(days)),
                    noncurrent_version_expiration: None,
                    abort_incomplete_multipart_upload: None,
                },
            ],
        };

        let rendered = render_lifecycle_configuration_xml(&config);
        assert!(rendered.contains("<Rule><ID>legacy</ID><Prefix>logs/</Prefix>"));
        assert!(rendered.contains(
            "<Rule><ID>explicit</ID><Filter><And><Prefix>logs/</Prefix><Tag><Key>env</Key><Value>prod</Value></Tag></And></Filter>"
        ));
    }

    #[test]
    fn lifecycle_rule_filter_constructor_rejects_duplicate_tag_keys() {
        let err = LifecycleRuleFilter::explicit_with_predicates(
            None,
            vec![
                LifecycleTag {
                    key: "env".to_string(),
                    value: "prod".to_string(),
                },
                LifecycleTag {
                    key: "env".to_string(),
                    value: "stage".to_string(),
                },
            ],
            LifecycleObjectSizeRange::default(),
        )
        .unwrap_err();
        assert!(matches!(err, LifecycleConfigError::InvalidArgument { .. }));
    }

    #[test]
    fn lifecycle_object_size_range_accepts_valid_bounds() {
        assert_eq!(
            LifecycleObjectSizeRange::greater_than(10).object_size_greater_than(),
            Some(10)
        );
        assert_eq!(
            LifecycleObjectSizeRange::less_than(100).object_size_less_than(),
            Some(100)
        );

        let range = LifecycleObjectSizeRange::new(Some(10), Some(100)).unwrap();
        assert_eq!(range.object_size_greater_than(), Some(10));
        assert_eq!(range.object_size_less_than(), Some(100));
    }

    #[test]
    fn lifecycle_object_size_range_rejects_equal_or_inverted_bounds() {
        for (greater_than, less_than) in [(10, 10), (10, 5)] {
            let err =
                LifecycleObjectSizeRange::new(Some(greater_than), Some(less_than)).unwrap_err();
            match err {
                LifecycleConfigError::InvalidRequest { reason } => {
                    assert_eq!(
                        reason,
                        "'ObjectSizeLessThan' has to be a value greater than 'ObjectSizeGreaterThan'."
                    );
                }
                other => panic!("expected InvalidRequest, got {other:?}"),
            }
        }
    }

    #[test]
    fn lifecycle_rule_filter_constructor_accepts_typed_size_range() {
        let filter = LifecycleRuleFilter::explicit_with_predicates(
            None,
            Vec::new(),
            LifecycleObjectSizeRange::new(Some(10), Some(100)).unwrap(),
        )
        .unwrap();
        assert_eq!(filter.object_size_greater_than(), Some(10));
        assert_eq!(filter.object_size_less_than(), Some(100));
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
    fn rejects_mixed_explicit_filter_and_legacy_prefix_rules() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <ID>filter-rule</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
                <Rule>\
                    <ID>legacy-prefix</ID>\
                    <Prefix>logs/</Prefix>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert_eq!(
            err,
            LifecycleConfigError::InvalidRequest {
                reason: "Base level prefix cannot be used in Lifecycle V2, prefixes are only supported in the Filter.".to_string(),
            }
        );
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
    fn rejects_prefix_over_1024_bytes() {
        let prefix = "a".repeat(1025);
        let xml = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><Prefix>{prefix}</Prefix></Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );
        let err = parse_lifecycle_configuration_xml(xml.as_bytes()).unwrap_err();
        assert_eq!(
            err,
            LifecycleConfigError::InvalidRequest {
                reason: "The maximum size of a prefix is 1024".to_string(),
            }
        );
    }

    #[test]
    fn rejects_tag_key_outside_1_to_128_chars() {
        let key = "k".repeat(129);
        let xml = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><Tag><Key>{key}</Key><Value>v</Value></Tag></Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );
        let err = parse_lifecycle_configuration_xml(xml.as_bytes()).unwrap_err();
        assert_eq!(
            err,
            LifecycleConfigError::InvalidRequest {
                reason: "A Tag's Key must be a length between 1 and 128.".to_string(),
            }
        );

        let empty_key_xml = b"<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><Tag><Key></Key><Value>v</Value></Tag></Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>";
        let err = parse_lifecycle_configuration_xml(empty_key_xml).unwrap_err();
        assert_eq!(
            err,
            LifecycleConfigError::InvalidRequest {
                reason: "A Tag's Key must be a length between 1 and 128.".to_string(),
            }
        );
    }

    #[test]
    fn rejects_tag_value_over_256_chars() {
        let value = "v".repeat(257);
        let xml = format!(
            "<LifecycleConfiguration>\
                <Rule>\
                    <Status>Enabled</Status>\
                    <Filter><Tag><Key>env</Key><Value>{value}</Value></Tag></Filter>\
                    <Expiration><Days>1</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>"
        );
        let err = parse_lifecycle_configuration_xml(xml.as_bytes()).unwrap_err();
        assert_eq!(
            err,
            LifecycleConfigError::InvalidRequest {
                reason: "A Tag's Value must be a length between 0 and 256.".to_string(),
            }
        );
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
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
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
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
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
        assert!(matches!(err, LifecycleConfigError::InvalidRequest { .. }));
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
        assert!(matches!(err, LifecycleConfigError::InvalidRequest { .. }));
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
        assert!(matches!(err, LifecycleConfigError::InvalidRequest { .. }));
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
        assert!(matches!(err, LifecycleConfigError::MalformedXml { .. }));
    }

    #[test]
    fn rejects_newer_noncurrent_versions_with_legacy_prefix() {
        let err = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Prefix>logs/</Prefix>\
                    <Status>Enabled</Status>\
                    <NoncurrentVersionExpiration>\
                        <NoncurrentDays>1</NoncurrentDays>\
                        <NewerNoncurrentVersions>2</NewerNoncurrentVersions>\
                    </NoncurrentVersionExpiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap_err();
        assert_eq!(
            err,
            LifecycleConfigError::LifecycleV2Required {
                reason: "NewerNoncurrentVersions element can only be used in Lifecycle V2."
                    .to_string()
            }
        );
    }

    #[test]
    fn accepts_newer_noncurrent_versions_with_explicit_filter() {
        let config = parse_lifecycle_configuration_xml(
            b"<LifecycleConfiguration>\
                <Rule>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <NoncurrentVersionExpiration>\
                        <NoncurrentDays>1</NoncurrentDays>\
                        <NewerNoncurrentVersions>2</NewerNoncurrentVersions>\
                    </NoncurrentVersionExpiration>\
                </Rule>\
                <Rule>\
                    <Filter/>\
                    <Status>Enabled</Status>\
                    <NoncurrentVersionExpiration>\
                        <NoncurrentDays>1</NoncurrentDays>\
                        <NewerNoncurrentVersions>2</NewerNoncurrentVersions>\
                    </NoncurrentVersionExpiration>\
                </Rule>\
            </LifecycleConfiguration>",
        )
        .unwrap();
        assert_eq!(config.rules.len(), 2);
        assert!(config.rules.iter().all(|rule| rule.filter.explicit_filter));
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
