use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPolicy {
    version: Option<PolicyVersion>,
    statements: Vec<PolicyStatement>,
}

impl BucketPolicy {
    #[must_use]
    pub fn version(&self) -> Option<PolicyVersion> {
        self.version
    }

    #[must_use]
    pub fn statements(&self) -> &[PolicyStatement] {
        &self.statements
    }

    #[must_use]
    pub fn is_public(&self) -> bool {
        self.statements
            .iter()
            .any(PolicyStatement::allows_public_access)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyVersion {
    V2008_10_17,
    V2012_10_17,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyStatement {
    sid: Option<String>,
    effect: PolicyEffect,
    principal: PolicyPrincipal,
    actions: Vec<String>,
    resources: Vec<String>,
    conditions: Vec<PolicyConditionClause>,
}

impl PolicyStatement {
    #[must_use]
    pub fn sid(&self) -> Option<&str> {
        self.sid.as_deref()
    }

    #[must_use]
    pub fn effect(&self) -> PolicyEffect {
        self.effect
    }

    #[must_use]
    pub fn principal(&self) -> &PolicyPrincipal {
        &self.principal
    }

    #[must_use]
    pub fn actions(&self) -> &[String] {
        &self.actions
    }

    #[must_use]
    pub fn resources(&self) -> &[String] {
        &self.resources
    }

    #[must_use]
    pub fn conditions(&self) -> &[PolicyConditionClause] {
        &self.conditions
    }

    fn allows_public_access(&self) -> bool {
        if self.effect != PolicyEffect::Allow {
            return false;
        }

        if self.principal.is_fixed_non_public() {
            return false;
        }

        !conditions_constrain_public_principal(&self.conditions)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyPrincipal {
    aws: Vec<String>,
    service: Vec<String>,
    canonical_user: Vec<String>,
    wildcard: bool,
}

impl PolicyPrincipal {
    #[must_use]
    pub fn aws(&self) -> &[String] {
        &self.aws
    }

    #[must_use]
    pub fn service(&self) -> &[String] {
        &self.service
    }

    #[must_use]
    pub fn canonical_user(&self) -> &[String] {
        &self.canonical_user
    }

    #[must_use]
    pub fn wildcard(&self) -> bool {
        self.wildcard
    }

    fn is_fixed_non_public(&self) -> bool {
        !self.wildcard && self.has_any() && self.all_values().into_iter().all(is_fixed_value)
    }

    fn has_any(&self) -> bool {
        self.wildcard
            || !self.aws.is_empty()
            || !self.service.is_empty()
            || !self.canonical_user.is_empty()
    }

    fn all_values(&self) -> Vec<&str> {
        self.aws
            .iter()
            .chain(&self.service)
            .chain(&self.canonical_user)
            .map(String::as_str)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConditionClause {
    operator: String,
    key: String,
    values: Vec<String>,
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

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum BucketPolicyError {
    #[error("malformed policy: {reason}")]
    Malformed { reason: &'static str },
}

impl BucketPolicyError {
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Malformed { reason } => reason,
        }
    }
}

pub fn parse_bucket_policy(policy: &str) -> Result<BucketPolicy, BucketPolicyError> {
    let value: Value = serde_json::from_str(policy).map_err(|_| BucketPolicyError::Malformed {
        reason: "invalid JSON",
    })?;
    let object = value.as_object().ok_or(BucketPolicyError::Malformed {
        reason: "top-level policy must be an object",
    })?;

    let version = match object.get("Version") {
        Some(Value::String(version)) => Some(parse_version(version)?),
        Some(_) => {
            return Err(BucketPolicyError::Malformed {
                reason: "Version must be a string",
            });
        }
        None => None,
    };

    let statements = parse_statements(object.get("Statement").ok_or(
        BucketPolicyError::Malformed {
            reason: "missing Statement",
        },
    )?)?;

    Ok(BucketPolicy {
        version,
        statements,
    })
}

fn parse_version(version: &str) -> Result<PolicyVersion, BucketPolicyError> {
    match version {
        "2008-10-17" => Ok(PolicyVersion::V2008_10_17),
        "2012-10-17" => Ok(PolicyVersion::V2012_10_17),
        _ => Err(BucketPolicyError::Malformed {
            reason: "unsupported Version value",
        }),
    }
}

fn parse_statements(value: &Value) -> Result<Vec<PolicyStatement>, BucketPolicyError> {
    match value {
        Value::Array(statements) => statements.iter().map(parse_statement).collect(),
        Value::Object(_) => Ok(vec![parse_statement(value)?]),
        _ => Err(BucketPolicyError::Malformed {
            reason: "Statement must be an object or array",
        }),
    }
}

fn parse_statement(value: &Value) -> Result<PolicyStatement, BucketPolicyError> {
    let object = value.as_object().ok_or(BucketPolicyError::Malformed {
        reason: "statement must be an object",
    })?;

    if object.contains_key("NotPrincipal")
        || object.contains_key("NotAction")
        || object.contains_key("NotResource")
    {
        return Err(BucketPolicyError::Malformed {
            reason: "NotPrincipal, NotAction, and NotResource are not supported",
        });
    }

    let sid = match object.get("Sid") {
        Some(Value::String(sid)) => Some(sid.clone()),
        Some(_) => {
            return Err(BucketPolicyError::Malformed {
                reason: "Sid must be a string",
            });
        }
        None => None,
    };

    let effect = match object.get("Effect") {
        Some(Value::String(effect)) => parse_effect(effect)?,
        Some(_) => {
            return Err(BucketPolicyError::Malformed {
                reason: "Effect must be a string",
            });
        }
        None => {
            return Err(BucketPolicyError::Malformed {
                reason: "missing Effect",
            });
        }
    };

    let principal = parse_principal(object.get("Principal").ok_or(
        BucketPolicyError::Malformed {
            reason: "missing Principal",
        },
    )?)?;

    let actions = parse_string_or_array(
        object.get("Action").ok_or(BucketPolicyError::Malformed {
            reason: "missing Action",
        })?,
        "Action must be a string or array of strings",
    )?;
    let resources = parse_string_or_array(
        object.get("Resource").ok_or(BucketPolicyError::Malformed {
            reason: "missing Resource",
        })?,
        "Resource must be a string or array of strings",
    )?;
    let conditions = match object.get("Condition") {
        Some(value) => parse_conditions(value)?,
        None => Vec::new(),
    };

    Ok(PolicyStatement {
        sid,
        effect,
        principal,
        actions,
        resources,
        conditions,
    })
}

fn parse_effect(effect: &str) -> Result<PolicyEffect, BucketPolicyError> {
    match effect {
        "Allow" => Ok(PolicyEffect::Allow),
        "Deny" => Ok(PolicyEffect::Deny),
        _ => Err(BucketPolicyError::Malformed {
            reason: "Effect must be Allow or Deny",
        }),
    }
}

fn parse_principal(value: &Value) -> Result<PolicyPrincipal, BucketPolicyError> {
    match value {
        Value::String(principal) => {
            if principal == "*" {
                return Ok(PolicyPrincipal {
                    wildcard: true,
                    ..PolicyPrincipal::default()
                });
            }

            Ok(PolicyPrincipal {
                aws: vec![principal.clone()],
                ..PolicyPrincipal::default()
            })
        }
        Value::Object(object) => {
            let mut principal = PolicyPrincipal::default();
            for (kind, value) in object {
                let values = parse_string_or_array(
                    value,
                    "Principal value must be a string or array of strings",
                )?;
                match kind.as_str() {
                    "AWS" => {
                        for value in values {
                            if value == "*" {
                                principal.wildcard = true;
                            } else {
                                principal.aws.push(value);
                            }
                        }
                    }
                    "Service" => principal.service.extend(values),
                    "CanonicalUser" => principal.canonical_user.extend(values),
                    _ => {
                        return Err(BucketPolicyError::Malformed {
                            reason: "unsupported Principal type",
                        });
                    }
                }
            }
            if principal.has_any() {
                Ok(principal)
            } else {
                Err(BucketPolicyError::Malformed {
                    reason: "Principal must not be empty",
                })
            }
        }
        _ => Err(BucketPolicyError::Malformed {
            reason: "Principal must be a string or object",
        }),
    }
}

fn parse_conditions(value: &Value) -> Result<Vec<PolicyConditionClause>, BucketPolicyError> {
    let operators = value.as_object().ok_or(BucketPolicyError::Malformed {
        reason: "Condition must be an object",
    })?;
    let mut clauses = Vec::new();
    for (operator, operands) in operators {
        let operand_object = operands.as_object().ok_or(BucketPolicyError::Malformed {
            reason: "Condition operator value must be an object",
        })?;
        for (key, value) in operand_object {
            clauses.push(PolicyConditionClause {
                operator: operator.clone(),
                key: key.clone(),
                values: parse_string_or_array(
                    value,
                    "Condition value must be a string or array of strings",
                )?,
            });
        }
    }
    Ok(clauses)
}

fn parse_string_or_array(
    value: &Value,
    field_name: &'static str,
) -> Result<Vec<String>, BucketPolicyError> {
    match value {
        Value::String(value) => Ok(vec![value.clone()]),
        Value::Array(values) => {
            if values.is_empty() {
                return Err(BucketPolicyError::Malformed {
                    reason: "array field must not be empty",
                });
            }
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                let value = value
                    .as_str()
                    .ok_or(BucketPolicyError::Malformed { reason: field_name })?;
                parsed.push(value.to_string());
            }
            Ok(parsed)
        }
        _ => Err(BucketPolicyError::Malformed { reason: field_name }),
    }
}

fn conditions_constrain_public_principal(conditions: &[PolicyConditionClause]) -> bool {
    conditions.iter().any(is_non_public_condition_clause)
}

fn is_non_public_condition_clause(clause: &PolicyConditionClause) -> bool {
    match clause.key.as_str() {
        "aws:PrincipalOrgID"
        | "aws:SourceVpc"
        | "aws:SourceVpce"
        | "aws:SourceOwner"
        | "aws:SourceAccount"
        | "aws:userid"
        | "s3:DataAccessPointAccount" => {
            matches!(
                clause.operator.as_str(),
                "StringEquals" | "StringEqualsIgnoreCase" | "StringLike"
            ) && clause.values.iter().all(|value| is_fixed_value(value))
        }
        "aws:SourceArn" | "s3:DataAccessPointArn" => {
            matches!(
                clause.operator.as_str(),
                "ArnEquals" | "ArnLike" | "StringEquals" | "StringEqualsIgnoreCase" | "StringLike"
            ) && clause.values.iter().all(|value| is_fixed_value(value))
        }
        "aws:SourceIp" => {
            clause.operator == "IpAddress"
                && clause.values.iter().all(|value| is_fixed_source_ip(value))
        }
        _ => false,
    }
}

fn is_fixed_value(value: &str) -> bool {
    !value.contains('*') && !value.contains("${")
}

fn is_fixed_source_ip(value: &str) -> bool {
    if !is_fixed_value(value) {
        return false;
    }
    if let Some((_, prefix)) = value.split_once('/') {
        return prefix.parse::<u8>().is_ok();
    }
    value.parse::<std::net::IpAddr>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_statement_array() {
        let policy = parse_bucket_policy(r#"{"Version":"2012-10-17","Statement":[]}"#).unwrap();
        assert_eq!(policy.version(), Some(PolicyVersion::V2012_10_17));
        assert!(policy.statements().is_empty());
        assert!(!policy.is_public());
    }

    #[test]
    fn wildcard_allow_is_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn fixed_aws_principal_is_not_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn fixed_source_vpc_constrains_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aws:SourceVpc":"vpc-12345678"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn string_not_equals_does_not_constrain_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"aws:SourceVpc":"vpc-12345678"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn deny_statement_is_not_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn invalid_json_is_rejected() {
        let err = parse_bucket_policy("{").unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "invalid JSON"
            }
        );
    }

    #[test]
    fn missing_statement_is_rejected() {
        let err = parse_bucket_policy(r#"{"Version":"2012-10-17"}"#).unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "missing Statement"
            }
        );
    }

    #[test]
    fn not_principal_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","NotPrincipal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "NotPrincipal, NotAction, and NotResource are not supported"
            }
        );
    }
}
