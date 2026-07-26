//! Shared value types and validation for AWS resource tags.
//!
//! Resource-tag keys and values use UTF-16 length limits and the AWS
//! `L | Z | N | +-=._:/@` character grammar. Operation-specific collection
//! limits and wire error envelopes remain with their respective services.

use std::collections::HashSet;
use std::fmt;

use quick_xml::{escape::unescape, events::BytesStart, events::Event, Reader};
use unicode_general_category::{get_general_category, GeneralCategory};

pub const MAX_TAG_KEY_UTF16_UNITS: usize = 128;
pub const MAX_TAG_VALUE_UTF16_UNITS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TagValidationError {
    #[error("tag key must not be empty")]
    EmptyKey,
    #[error("tag key is {actual} UTF-16 units; maximum is {maximum}")]
    KeyTooLong { actual: usize, maximum: usize },
    #[error("tag key must not use the reserved aws: prefix")]
    ReservedKeyPrefix,
    #[error("tag key contains invalid character {character:?}")]
    InvalidKeyCharacter { character: char },
    #[error("tag value is {actual} UTF-16 units; maximum is {maximum}")]
    ValueTooLong { actual: usize, maximum: usize },
    #[error("tag value contains invalid character {character:?}")]
    InvalidValueCharacter { character: char },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TagSetValidationError {
    #[error("invalid tag {key}={value}: {source}")]
    Tag {
        key: String,
        value: String,
        source: TagValidationError,
    },
    #[error("duplicate tag key {key}")]
    DuplicateKey { key: String },
    #[error("tag set contains {actual} tags; maximum is {maximum}")]
    TooMany { actual: usize, maximum: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CanonicalTagSetParseError {
    #[error("stored tag XML is malformed: {reason}")]
    Malformed { reason: String },
    #[error("stored tag XML contains an invalid tag: {0}")]
    Invalid(#[from] TagSetValidationError),
}

#[must_use]
pub fn tag_utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

#[must_use]
pub fn is_valid_tag_character(character: char) -> bool {
    matches!(
        get_general_category(character),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
            | GeneralCategory::DecimalNumber
            | GeneralCategory::LetterNumber
            | GeneralCategory::OtherNumber
            | GeneralCategory::SpaceSeparator
            | GeneralCategory::LineSeparator
            | GeneralCategory::ParagraphSeparator
    ) || matches!(character, '+' | '-' | '=' | '.' | '_' | ':' | '/' | '@')
}

pub fn validate_tag_key_length(key: &str) -> Result<(), TagValidationError> {
    let actual = tag_utf16_len(key);
    if actual == 0 {
        return Err(TagValidationError::EmptyKey);
    }
    if actual > MAX_TAG_KEY_UTF16_UNITS {
        return Err(TagValidationError::KeyTooLong {
            actual,
            maximum: MAX_TAG_KEY_UTF16_UNITS,
        });
    }
    Ok(())
}

pub fn validate_tag_value_length(value: &str) -> Result<(), TagValidationError> {
    let actual = tag_utf16_len(value);
    if actual > MAX_TAG_VALUE_UTF16_UNITS {
        return Err(TagValidationError::ValueTooLong {
            actual,
            maximum: MAX_TAG_VALUE_UTF16_UNITS,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TagKey(String);

impl TagKey {
    pub fn new(key: String) -> Result<Self, TagValidationError> {
        validate_tag_key_length(&key)?;
        if let Some(character) = key
            .chars()
            .find(|character| !is_valid_tag_character(*character))
        {
            return Err(TagValidationError::InvalidKeyCharacter { character });
        }
        Ok(Self(key))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for TagKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for TagKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for TagKey {
    type Error = TagValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TagValue(String);

impl TagValue {
    pub fn new(value: String) -> Result<Self, TagValidationError> {
        validate_tag_value_length(&value)?;
        if let Some(character) = value
            .chars()
            .find(|character| !is_valid_tag_character(*character))
        {
            return Err(TagValidationError::InvalidValueCharacter { character });
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for TagValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for TagValue {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for TagValue {
    type Error = TagValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag {
    key: TagKey,
    value: TagValue,
}

impl Tag {
    pub fn new(key: String, value: String) -> Result<Self, TagValidationError> {
        let reserved_prefix = key
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("aws:"));
        let key = TagKey::new(key)?;
        if reserved_prefix {
            return Err(TagValidationError::ReservedKeyPrefix);
        }
        Ok(Self {
            key,
            value: TagValue::new(value)?,
        })
    }

    #[must_use]
    pub fn key(&self) -> &TagKey {
        &self.key
    }

    #[must_use]
    pub fn value(&self) -> &TagValue {
        &self.value
    }

    #[must_use]
    pub fn into_pair(self) -> (String, String) {
        (self.key.into_string(), self.value.into_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagSet {
    tags: Vec<Tag>,
    maximum: usize,
}

impl TagSet {
    pub fn new(tags: Vec<Tag>, maximum: usize) -> Result<Self, TagSetValidationError> {
        let mut keys = HashSet::new();
        for tag in &tags {
            if !keys.insert(tag.key.as_str()) {
                return Err(TagSetValidationError::DuplicateKey {
                    key: tag.key.to_string(),
                });
            }
        }
        if tags.len() > maximum {
            return Err(TagSetValidationError::TooMany {
                actual: tags.len(),
                maximum,
            });
        }
        Ok(Self { tags, maximum })
    }

    pub fn from_pairs(
        pairs: Vec<(String, String)>,
        maximum: usize,
    ) -> Result<Self, TagSetValidationError> {
        let tags =
            pairs
                .into_iter()
                .map(|(key, value)| {
                    Tag::new(key.clone(), value.clone())
                        .map_err(|source| TagSetValidationError::Tag { key, value, source })
                })
                .collect::<Result<Vec<_>, _>>()?;
        Self::new(tags, maximum)
    }

    #[must_use]
    pub const fn empty(maximum: usize) -> Self {
        Self {
            tags: Vec::new(),
            maximum,
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[Tag] {
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
    pub fn into_pairs(self) -> Vec<(String, String)> {
        self.tags.into_iter().map(Tag::into_pair).collect()
    }

    pub fn merge(&self, updates: &Self) -> Result<Self, TagSetValidationError> {
        let mut merged = self.tags.clone();
        for update in &updates.tags {
            if let Some(existing) = merged
                .iter_mut()
                .find(|existing| existing.key == update.key)
            {
                existing.value = update.value.clone();
            } else {
                merged.push(update.clone());
            }
        }
        Self::new(merged, self.maximum)
    }

    #[must_use]
    pub fn remove_keys(&self, tag_keys: &[TagKey]) -> Self {
        let to_remove: HashSet<&str> = tag_keys.iter().map(TagKey::as_str).collect();
        Self {
            tags: self
                .tags
                .iter()
                .filter(|tag| !to_remove.contains(tag.key.as_str()))
                .cloned()
                .collect(),
            maximum: self.maximum,
        }
    }

    #[must_use]
    pub fn to_xml(&self) -> String {
        let mut xml = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>",
        );
        for tag in &self.tags {
            xml.push_str("<Tag><Key>");
            push_xml_escaped(&mut xml, tag.key.as_str());
            xml.push_str("</Key><Value>");
            push_xml_escaped(&mut xml, tag.value.as_str());
            xml.push_str("</Value></Tag>");
        }
        xml.push_str("</TagSet></Tagging>");
        xml
    }

    pub fn parse_canonical_xml(
        xml: &str,
        maximum: usize,
    ) -> Result<Self, CanonicalTagSetParseError> {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum State {
            Start,
            InRoot,
            InTagSet,
            ExpectKey,
            InKey,
            ExpectValue,
            InValue,
            ExpectTagEnd,
            ExpectRootEnd,
            Done,
        }

        let malformed = |reason: &str| CanonicalTagSetParseError::Malformed {
            reason: reason.to_string(),
        };
        let mut reader = Reader::from_str(xml);
        let mut state = State::Start;
        let mut pairs = Vec::new();
        let mut key = String::new();
        let mut value = String::new();
        let mut saw_declaration = false;

        loop {
            match reader.read_event() {
                Ok(Event::Start(element)) => match (state, element.name().as_ref()) {
                    (State::Start, b"Tagging") => {
                        validate_tagging_root_attributes(&element)?;
                        state = State::InRoot;
                    }
                    (State::InRoot, b"TagSet") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        state = State::InTagSet;
                    }
                    (State::InTagSet, b"Tag") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        key.clear();
                        value.clear();
                        state = State::ExpectKey;
                    }
                    (State::ExpectKey, b"Key") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        state = State::InKey;
                    }
                    (State::ExpectValue, b"Value") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        state = State::InValue;
                    }
                    _ => return Err(malformed("unexpected element in stored tag XML")),
                },
                Ok(Event::Empty(element)) => match (state, element.name().as_ref()) {
                    (State::InRoot, b"TagSet") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        state = State::ExpectRootEnd;
                    }
                    (State::ExpectKey, b"Key") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        state = State::ExpectValue;
                    }
                    (State::ExpectValue, b"Value") => {
                        ensure_no_stored_tag_attributes(&element)?;
                        state = State::ExpectTagEnd;
                    }
                    _ => return Err(malformed("unexpected empty element in stored tag XML")),
                },
                Ok(Event::End(element)) => match (state, element.name().as_ref()) {
                    (State::InKey, b"Key") => state = State::ExpectValue,
                    (State::InValue, b"Value") => state = State::ExpectTagEnd,
                    (State::ExpectTagEnd, b"Tag") => {
                        pairs.push((std::mem::take(&mut key), std::mem::take(&mut value)));
                        state = State::InTagSet;
                    }
                    (State::InTagSet, b"TagSet") => state = State::ExpectRootEnd,
                    (State::ExpectRootEnd, b"Tagging") => state = State::Done,
                    _ => return Err(malformed("unexpected closing element in stored tag XML")),
                },
                Ok(event @ (Event::Text(_) | Event::GeneralRef(_))) => {
                    let entity_reason = match state {
                        State::InKey => "invalid entity in stored tag key",
                        State::InValue => "invalid entity in stored tag value",
                        _ => "invalid entity in stored tag XML",
                    };
                    let text = decode_canonical_tag_text(event, entity_reason)?;
                    match state {
                        State::InKey => key.push_str(&text),
                        State::InValue => value.push_str(&text),
                        _ if text.trim().is_empty() => {}
                        _ => return Err(malformed("unexpected text in stored tag XML")),
                    }
                }
                Ok(Event::Decl(_)) if state == State::Start && !saw_declaration => {
                    saw_declaration = true;
                }
                Ok(Event::Decl(_)) => {
                    return Err(malformed("unexpected XML declaration in stored tag XML"));
                }
                Ok(Event::Eof) if state == State::Done => break,
                Ok(Event::Eof) => {
                    return Err(malformed("stored tag XML ended before the envelope"))
                }
                Ok(Event::CData(_) | Event::Comment(_) | Event::PI(_) | Event::DocType(_)) => {
                    return Err(malformed("unsupported content in stored tag XML"));
                }
                Err(error) => {
                    return Err(CanonicalTagSetParseError::Malformed {
                        reason: format!("invalid stored tag XML: {error}"),
                    });
                }
            }
        }

        Self::from_pairs(pairs, maximum).map_err(CanonicalTagSetParseError::from)
    }
}

fn decode_canonical_tag_text(
    event: Event<'_>,
    invalid_entity_reason: &str,
) -> Result<String, CanonicalTagSetParseError> {
    let malformed = |reason: &str| CanonicalTagSetParseError::Malformed {
        reason: reason.to_string(),
    };
    let escaped;
    let raw = match event {
        Event::Text(text) => text.into_inner(),
        Event::GeneralRef(reference) => {
            let mut bytes = Vec::with_capacity(reference.len() + 2);
            bytes.push(b'&');
            bytes.extend_from_slice(reference.as_ref());
            bytes.push(b';');
            escaped = bytes;
            escaped.into()
        }
        _ => unreachable!("decode_canonical_tag_text only accepts text and reference events"),
    };
    let raw =
        std::str::from_utf8(raw.as_ref()).map_err(|_| malformed("stored tag XML is not UTF-8"))?;
    unescape(raw)
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| malformed(invalid_entity_reason))
}

fn validate_tagging_root_attributes(
    element: &BytesStart<'_>,
) -> Result<(), CanonicalTagSetParseError> {
    const S3_NAMESPACE: &[u8] = b"http://s3.amazonaws.com/doc/2006-03-01/";

    let mut attributes = element.attributes();
    match attributes.next() {
        None => Ok(()),
        Some(Ok(attribute))
            if attribute.key.as_ref() == b"xmlns"
                && attribute.value.as_ref() == S3_NAMESPACE
                && attributes.next().is_none() =>
        {
            Ok(())
        }
        _ => Err(CanonicalTagSetParseError::Malformed {
            reason: "unexpected <Tagging> attributes in stored tag XML".to_string(),
        }),
    }
}

fn ensure_no_stored_tag_attributes(
    element: &BytesStart<'_>,
) -> Result<(), CanonicalTagSetParseError> {
    if element.attributes().next().is_some() {
        return Err(CanonicalTagSetParseError::Malformed {
            reason: "unexpected attributes in stored tag XML".to_string(),
        });
    }
    Ok(())
}

fn push_xml_escaped(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            other => output.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_tag_grammar_and_utf16_lengths_are_exact() {
        for accepted in [
            "letters-ABC-環境",
            "numbers-123-١-Ⅷ-²",
            "separators \u{00a0}\u{2028}\u{2029}",
            "punctuation+-=._:/@",
            &"𐐀".repeat(64),
        ] {
            TagKey::new(accepted.to_string()).unwrap();
        }
        for rejected in ["invalid!", "combining-ͅ", &"𐐀".repeat(65)] {
            TagKey::new(rejected.to_string()).unwrap_err();
        }
        TagValue::new("𐐀".repeat(128)).unwrap();
        TagValue::new("𐐀".repeat(129)).unwrap_err();
    }

    #[test]
    fn reserved_prefix_is_ascii_case_insensitive() {
        for key in ["aws:reserved", "AWS:reserved", "Aws:reserved"] {
            assert_eq!(
                Tag::new(key.to_string(), "value".to_string()),
                Err(TagValidationError::ReservedKeyPrefix)
            );
        }
    }

    #[test]
    fn canonical_xml_round_trips_through_validated_types() {
        let tags = TagSet::from_pairs(vec![("key".to_string(), "value".to_string())], 10).unwrap();
        assert_eq!(
            TagSet::parse_canonical_xml(&tags.to_xml(), 10).unwrap(),
            tags
        );
    }

    #[test]
    fn canonical_xml_parser_decodes_split_reference_events() {
        let tags = TagSet::parse_canonical_xml(
            "<Tagging><TagSet><Tag><Key>k&#x31;</Key><Value>v&#50;</Value></Tag></TagSet></Tagging>",
            10,
        )
        .unwrap();
        assert_eq!(
            tags,
            TagSet::from_pairs(vec![("k1".to_string(), "v2".to_string())], 10).unwrap()
        );
    }

    #[test]
    fn canonical_xml_parser_rejects_invalid_envelopes_and_members() {
        for xml in [
            "",
            "<TagSet></TagSet>",
            "prefix<Tagging><TagSet></TagSet></Tagging>",
            "<Tagging><TagSet></TagSet></Tagging>suffix",
            "<TaggingX><TagSet></TagSet></TaggingX>",
            "<Tagging xmlns=\"wrong\"><TagSet></TagSet></Tagging>",
            "<Tagging extra=\"value\"><TagSet></TagSet></Tagging>",
            "<Tagging><TagSet></TagSet><Unrelated/></Tagging>",
            "<Tagging><TagSet></TagSet>unrelated</Tagging>",
            "<Tagging><TagSet><Tag><Key>key</Key></Tag></TagSet></Tagging>",
            "<Tagging><TagSet>junk</TagSet></Tagging>",
        ] {
            assert!(matches!(
                TagSet::parse_canonical_xml(xml, 10),
                Err(CanonicalTagSetParseError::Malformed { .. })
            ));
        }
    }
}
