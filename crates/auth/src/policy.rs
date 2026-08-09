// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Policy-language primitives shared by service-specific policy documents.
//!
//! S3 resource policies, IAM identity policies, role trust policies, and
//! session policies have different valid statement shapes and decision
//! composition. This module contains only the language pieces that are truly
//! common to those typed documents.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PolicyValue {
    value: String,
    escaped_wildcards: Vec<usize>,
}

impl PolicyValue {
    pub(crate) fn literal(value: &str) -> Self {
        Self {
            value: value.to_string(),
            escaped_wildcards: Vec::new(),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.value
    }

    /// Append source policy syntax, preserving wildcard interpretation.
    pub(crate) fn push_pattern_fragment(&mut self, value: &str) {
        self.value.push_str(value);
    }

    /// Append one wildcard character that must be matched literally.
    ///
    /// The character and its index are updated atomically, so callers cannot
    /// invalidate or disorder the side table used during matching.
    pub(crate) fn push_literal_asterisk(&mut self) {
        self.escaped_wildcards.push(self.value.chars().count());
        self.value.push('*');
    }

    /// Append one question mark that must be matched literally.
    pub(crate) fn push_literal_question_mark(&mut self) {
        self.escaped_wildcards.push(self.value.chars().count());
        self.value.push('?');
    }

    fn wildcard_is_escaped(&self, char_index: usize) -> bool {
        self.escaped_wildcards.binary_search(&char_index).is_ok()
    }
}

impl std::ops::Deref for PolicyValue {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Three-valued result produced by evaluating one policy document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEvaluation {
    ExplicitDeny,
    ExplicitAllow,
    NoMatch,
}

/// Supported IAM policy-language versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyVersion {
    V2008_10_17,
    V2012_10_17,
}

impl PolicyVersion {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::V2008_10_17 => "2008-10-17",
            Self::V2012_10_17 => "2012-10-17",
        }
    }
}

/// Effect shared by all IAM policy statement kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

impl PolicyEffect {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "Allow",
            Self::Deny => "Deny",
        }
    }
}

/// One normalized IAM condition operator/key/value clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConditionClause {
    pub(crate) operator: String,
    pub(crate) key: String,
    pub(crate) values: Vec<String>,
}

/// Statement fields shared by typed resource, identity, trust, and session
/// policy wrappers.
///
/// Whether a statement may or must contain a principal, and which actions,
/// resources, and conditions are valid, remain responsibilities of the typed
/// document that owns these fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PolicyStatementCore {
    pub(crate) sid: Option<String>,
    pub(crate) effect: PolicyEffect,
    pub(crate) actions: Vec<String>,
    pub(crate) conditions: Vec<PolicyConditionClause>,
}

impl PolicyStatementCore {
    pub(crate) fn new(
        sid: Option<String>,
        effect: PolicyEffect,
        actions: Vec<String>,
        conditions: Vec<PolicyConditionClause>,
    ) -> Self {
        Self {
            sid,
            effect,
            actions,
            conditions,
        }
    }
}

impl PolicyConditionClause {
    #[must_use]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

pub(crate) fn action_pattern_matches(pattern: &str, action: &str) -> bool {
    wildcard_matches(&pattern.to_ascii_lowercase(), &action.to_ascii_lowercase())
}

pub(crate) fn wildcard_matches(pattern: &str, value: &str) -> bool {
    policy_value_wildcard_matches(&PolicyValue::literal(pattern), value)
}

pub(crate) fn policy_value_wildcard_matches(pattern: &PolicyValue, value: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;

    for (pattern_index, pattern_ch) in pattern_chars.into_iter().enumerate() {
        let mut current = vec![false; value.len() + 1];
        match pattern_ch {
            '*' if !pattern.wildcard_is_escaped(pattern_index) => {
                current[0] = previous[0];
                for index in 1..=value.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            '?' if !pattern.wildcard_is_escaped(pattern_index) => {
                current[1..(value.len() + 1)].copy_from_slice(&previous[..value.len()]);
            }
            _ => {
                for (index, actual) in value.iter().enumerate() {
                    if *actual == pattern_ch && previous[index] {
                        current[index + 1] = true;
                    }
                }
            }
        }
        previous = current;
    }

    previous[value.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_patterns_are_case_insensitive() {
        assert!(action_pattern_matches("S3:Get*", "s3:GetObject"));
        assert!(!action_pattern_matches("s3:Put*", "s3:GetObject"));
    }

    #[test]
    fn expanded_values_can_mark_wildcards_as_literal() {
        let mut pattern = PolicyValue::literal("prefix-");
        pattern.push_literal_asterisk();

        assert!(policy_value_wildcard_matches(&pattern, "prefix-*"));
        assert!(!policy_value_wildcard_matches(&pattern, "prefix-value"));
    }

    #[test]
    fn escaped_wildcard_indexes_remain_ordered_across_utf8_appends() {
        let mut pattern = PolicyValue::literal("");
        pattern.push_pattern_fragment("é");
        pattern.push_literal_asterisk();
        pattern.push_pattern_fragment("中");
        pattern.push_literal_question_mark();

        assert_eq!(pattern.escaped_wildcards, [1, 3]);
        assert!(policy_value_wildcard_matches(&pattern, "é*中?"));
        assert!(!policy_value_wildcard_matches(&pattern, "évalue中x"));
    }
}
