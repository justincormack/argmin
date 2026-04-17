#![no_main]

use auth::bucket_policy::{
    parse_bucket_policy, BucketPolicy, PolicyAction, PolicyEvaluation, PolicyRequest, PolicyTag,
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Map, Value};

const DEFAULT_BUCKET: &str = "bucket";
const DEFAULT_KEY: &str = "key";
const ALT_ROOT: &str = "arn:aws:iam::444455556666:root";
const ALT_USER: &str = "arn:aws:iam::444455556666:user/alt";

struct RequestCase {
    action: PolicyAction,
    bucket: String,
    key: String,
    bucket_resource: bool,
    principal: Option<String>,
    existing_tags: Vec<(String, String)>,
    request_tags: Vec<(String, String)>,
    copy_source: Option<String>,
    metadata_directive: Option<String>,
    canned_acl: Option<String>,
    server_side_encryption: Option<String>,
    sse_customer_algorithm: Option<String>,
    grant_read: Option<String>,
    grant_write: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    grant_full_control: Option<String>,
}

impl RequestCase {
    fn from_value(value: &Value) -> Self {
        let object = value.as_object();
        Self {
            action: object
                .and_then(|map| map.get("action"))
                .and_then(Value::as_str)
                .map(parse_action)
                .unwrap_or(PolicyAction::PutObject),
            bucket: object
                .and_then(|map| map.get("bucket"))
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_BUCKET)
                .to_string(),
            key: object
                .and_then(|map| map.get("key"))
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_KEY)
                .to_string(),
            bucket_resource: object
                .and_then(|map| map.get("bucket_resource"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            principal: string_field(object, "principal"),
            existing_tags: tags_field(object, "existing_tags"),
            request_tags: tags_field(object, "request_tags"),
            copy_source: string_field(object, "copy_source"),
            metadata_directive: string_field(object, "metadata_directive"),
            canned_acl: string_field(object, "canned_acl"),
            server_side_encryption: string_field(object, "server_side_encryption"),
            sse_customer_algorithm: string_field(object, "sse_customer_algorithm"),
            grant_read: string_field(object, "grant_read"),
            grant_write: string_field(object, "grant_write"),
            grant_read_acp: string_field(object, "grant_read_acp"),
            grant_write_acp: string_field(object, "grant_write_acp"),
            grant_full_control: string_field(object, "grant_full_control"),
        }
    }

    fn evaluate(&self, policy: &BucketPolicy) -> PolicyEvaluation {
        let existing_tags = self
            .existing_tags
            .iter()
            .map(|(key, value)| PolicyTag::new(key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let request_tags = self
            .request_tags
            .iter()
            .map(|(key, value)| PolicyTag::new(key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let request = if self.bucket_resource {
            PolicyRequest::for_bucket(
                self.action,
                &self.bucket,
                self.principal.as_deref(),
                None,
            )
        } else {
            PolicyRequest::new(
                self.action,
                &self.bucket,
                &self.key,
                self.principal.as_deref(),
                None,
            )
        }
        .with_existing_object_tags(&existing_tags)
        .with_request_object_tags(&request_tags)
        .with_copy_source(self.copy_source.as_deref())
        .with_metadata_directive(self.metadata_directive.as_deref())
        .with_canned_acl(self.canned_acl.as_deref())
        .with_server_side_encryption(self.server_side_encryption.as_deref())
        .with_sse_customer_algorithm(self.sse_customer_algorithm.as_deref())
        .with_grant_read(self.grant_read.as_deref())
        .with_grant_write(self.grant_write.as_deref())
        .with_grant_read_acp(self.grant_read_acp.as_deref())
        .with_grant_write_acp(self.grant_write_acp.as_deref())
        .with_grant_full_control(self.grant_full_control.as_deref());
        policy.evaluate(&request)
    }
}

fuzz_target!(|data: &[u8]| {
    let Ok(envelope) = serde_json::from_slice::<Value>(data) else {
        return;
    };
    let Some(policy_value) = envelope.get("policy") else {
        return;
    };
    let Ok(policy_json) = serde_json::to_string(policy_value) else {
        return;
    };
    let Ok(policy) = parse_bucket_policy(&policy_json) else {
        return;
    };
    let request = RequestCase::from_value(envelope.get("request").unwrap_or(&Value::Null));

    let baseline = request.evaluate(&policy);
    let _ = policy.validate_evaluable_object_conditions();

    let normalized_json = policy.normalized_json();
    if let Ok(normalized_policy) = parse_bucket_policy(&normalized_json) {
        assert_eq!(baseline, request.evaluate(&normalized_policy));
    }

    let arrayified_value = force_singletons_to_arrays(policy_value);
    if let Ok(arrayified_json) = serde_json::to_string(&arrayified_value) {
        if let Ok(arrayified_policy) = parse_bucket_policy(&arrayified_json) {
            assert_eq!(baseline, request.evaluate(&arrayified_policy));
        }
    }

    let expanded_value = expand_statement_arrays(policy_value);
    if let Ok(expanded_json) = serde_json::to_string(&expanded_value) {
        if let Ok(expanded_policy) = parse_bucket_policy(&expanded_json) {
            assert_eq!(baseline, request.evaluate(&expanded_policy));
        }
    }
});

fn string_field(object: Option<&Map<String, Value>>, key: &str) -> Option<String> {
    object
        .and_then(|map| map.get(key))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn tags_field(object: Option<&Map<String, Value>>, key: &str) -> Vec<(String, String)> {
    let Some(tags) = object.and_then(|map| map.get(key)).and_then(Value::as_array) else {
        return Vec::new();
    };
    tags.iter()
        .filter_map(|tag| match tag {
            Value::Array(values) if values.len() >= 2 => Some((
                values.first()?.as_str()?.to_string(),
                values.get(1)?.as_str()?.to_string(),
            )),
            Value::Object(map) => Some((
                map.get("key")?.as_str()?.to_string(),
                map.get("value")?.as_str()?.to_string(),
            )),
            _ => None,
        })
        .collect()
}

fn parse_action(action: &str) -> PolicyAction {
    match action {
        "GetBucketLocation" | "s3:GetBucketLocation" => PolicyAction::GetBucketLocation,
        "ListBucket" | "s3:ListBucket" => PolicyAction::ListBucket,
        "GetObject" | "s3:GetObject" => PolicyAction::GetObject,
        "GetObjectAcl" | "s3:GetObjectAcl" => PolicyAction::GetObjectAcl,
        "GetObjectTagging" | "s3:GetObjectTagging" => PolicyAction::GetObjectTagging,
        "PutObject" | "s3:PutObject" => PolicyAction::PutObject,
        "PutObjectAcl" | "s3:PutObjectAcl" => PolicyAction::PutObjectAcl,
        "PutObjectTagging" | "s3:PutObjectTagging" => PolicyAction::PutObjectTagging,
        "DeleteObject" | "s3:DeleteObject" => PolicyAction::DeleteObject,
        "DeleteObjectTagging" | "s3:DeleteObjectTagging" => PolicyAction::DeleteObjectTagging,
        _ => PolicyAction::PutObject,
    }
}

fn force_singletons_to_arrays(policy: &Value) -> Value {
    let Some(root) = policy.as_object() else {
        return policy.clone();
    };
    let mut root = root.clone();
    let Some(statements) = root.get_mut("Statement").and_then(Value::as_array_mut) else {
        return Value::Object(root);
    };
    for statement in statements {
        let Some(statement_object) = statement.as_object_mut() else {
            continue;
        };
        wrap_string_field(statement_object, "Action");
        wrap_string_field(statement_object, "Resource");
        if let Some(principal) = statement_object
            .get_mut("Principal")
            .and_then(Value::as_object_mut)
        {
            wrap_string_field(principal, "AWS");
            wrap_string_field(principal, "Service");
            wrap_string_field(principal, "CanonicalUser");
        }
    }
    Value::Object(root)
}

fn wrap_string_field(object: &mut Map<String, Value>, field: &str) {
    if let Some(Value::String(value)) = object.get(field).cloned() {
        object.insert(field.to_string(), Value::Array(vec![Value::String(value)]));
    }
}

fn expand_statement_arrays(policy: &Value) -> Value {
    let Some(root) = policy.as_object() else {
        return policy.clone();
    };
    let Some(statements) = root.get("Statement").and_then(Value::as_array) else {
        return policy.clone();
    };
    let mut expanded = Vec::new();
    for statement in statements {
        expanded.extend(expand_statement(statement));
    }
    let mut root = root.clone();
    root.insert("Statement".to_string(), Value::Array(expanded));
    Value::Object(root)
}

fn expand_statement(statement: &Value) -> Vec<Value> {
    let Some(statement_object) = statement.as_object() else {
        return vec![statement.clone()];
    };

    let actions = value_variants(statement_object.get("Action"));
    let resources = value_variants(statement_object.get("Resource"));
    let principals = principal_variants(statement_object.get("Principal"));

    let mut expanded = Vec::new();
    for principal in principals {
        for action in &actions {
            for resource in &resources {
                let mut next = statement_object.clone();
                next.insert("Principal".to_string(), principal.clone());
                next.insert("Action".to_string(), action.clone());
                next.insert("Resource".to_string(), resource.clone());
                expanded.push(Value::Object(next));
            }
        }
    }
    if expanded.is_empty() {
        vec![statement.clone()]
    } else {
        expanded
    }
}

fn value_variants(value: Option<&Value>) -> Vec<Value> {
    match value {
        Some(Value::Array(values)) if !values.is_empty() => values.clone(),
        Some(other) => vec![other.clone()],
        None => vec![Value::Null],
    }
}

fn principal_variants(value: Option<&Value>) -> Vec<Value> {
    let Some(principal) = value else {
        return vec![Value::Null];
    };
    let Some(principal_object) = principal.as_object() else {
        return vec![principal.clone()];
    };
    let mut variants = vec![Map::new()];
    for key in ["AWS", "Service", "CanonicalUser"] {
        let Some(value) = principal_object.get(key) else {
            continue;
        };
        match value {
            Value::Array(values) => {
                if values.is_empty() {
                    return vec![principal.clone()];
                }
                let mut next_variants = Vec::new();
                for variant in &variants {
                    for entry in values {
                        let mut next = variant.clone();
                        next.insert(key.to_string(), entry.clone());
                        next_variants.push(next);
                    }
                }
                variants = next_variants;
            }
            other => {
                for variant in &mut variants {
                    variant.insert(key.to_string(), other.clone());
                }
            }
        }
    }
    if variants.is_empty() {
        vec![principal.clone()]
    } else {
        variants.into_iter().map(Value::Object).collect()
    }
}

#[allow(dead_code)]
fn _seed_examples() -> [&'static str; 2] {
    [ALT_ROOT, ALT_USER]
}
