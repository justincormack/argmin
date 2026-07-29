use auth::{
    authenticate_request, generate_session_credential_material, IamRoleArn, PolicyEvaluation,
    ResolvedPrincipalAuthorization, RoleSessionName, RoleSessionNameError, SessionLifetime,
};

use super::request::{percent_decode_strict, S3Request};
use super::response::{S3Response, StsAssumeRoleResponse, WireResponseIds};
use super::HttpFrontend;

const STS_QUERY_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";
pub(crate) const MAX_STS_QUERY_BODY_SIZE: usize = 10_000_000;
const DEFAULT_SESSION_DURATION_SECONDS: u32 = 3_600;
const MIN_SESSION_DURATION_SECONDS: u32 = 900;
const SIGNATURE_MISMATCH_MESSAGE: &str = "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method. Consult the service documentation for details.";

enum StsRequestError {
    Sender {
        status: u16,
        code: &'static str,
        message: String,
    },
    Internal,
}

impl StsRequestError {
    fn validation(message: impl Into<String>) -> Self {
        Self::Sender {
            status: 400,
            code: "ValidationError",
            message: message.into(),
        }
    }

    fn access_denied(message: impl Into<String>) -> Self {
        Self::Sender {
            status: 403,
            code: "AccessDenied",
            message: message.into(),
        }
    }

    fn authentication(error: auth::AuthError) -> Self {
        match error {
            auth::AuthError::SignatureMismatch { .. } => Self::Sender {
                status: 403,
                code: "SignatureDoesNotMatch",
                message: SIGNATURE_MISMATCH_MESSAGE.to_string(),
            },
            _ => Self::access_denied("Access denied"),
        }
    }

    fn into_response(self, wire_ids: &WireResponseIds) -> S3Response {
        match self {
            Self::Sender {
                status,
                code,
                message,
            } => S3Response::sts_error(status, code, &message, wire_ids),
            Self::Internal => S3Response::sts_error(
                500,
                "InternalFailure",
                "The request processing has failed because of an unknown error, exception or failure.",
                wire_ids,
            ),
        }
    }
}

fn has_malformed_percent_triplet(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len()
            || !bytes[index + 1].is_ascii_hexdigit()
            || !bytes[index + 2].is_ascii_hexdigit()
        {
            return true;
        }
        index += 3;
    }
    false
}

fn decode_form_component(value: &str) -> Result<String, StsRequestError> {
    if has_malformed_percent_triplet(value) {
        return Err(StsRequestError::validation("invalid form encoding"));
    }
    percent_decode_strict(&value.replace('+', " "))
        .map_err(|_| StsRequestError::validation("invalid form encoding"))
}

fn parse_form(body: &[u8]) -> Result<Vec<(String, String)>, StsRequestError> {
    let body = std::str::from_utf8(body)
        .map_err(|_| StsRequestError::validation("invalid form encoding"))?;
    if body.is_empty() {
        return Ok(Vec::new());
    }
    body.split('&')
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            Ok((decode_form_component(name)?, decode_form_component(value)?))
        })
        .collect()
}

fn required_parameter<'a>(
    parameters: &'a [(String, String)],
    name: &str,
    field_name: &str,
) -> Result<&'a str, StsRequestError> {
    parameters
        .iter()
        .find_map(|(parameter_name, value)| {
            (parameter_name == name).then_some(value.as_str())
        })
        .ok_or_else(|| {
            StsRequestError::validation(format!(
            "1 validation error detected: Value null at '{field_name}' failed to satisfy constraint: Member must not be null"
        ))
        })
}

fn optional_parameter<'a>(parameters: &'a [(String, String)], name: &str) -> Option<&'a str> {
    parameters
        .iter()
        .find_map(|(parameter_name, value)| (parameter_name == name).then_some(value.as_str()))
}

fn parse_role_session_name(
    parameters: &[(String, String)],
) -> Result<RoleSessionName, StsRequestError> {
    const TOO_SHORT: &str = "Member must have length greater than or equal to 2";
    const TOO_LONG: &str = "Member must have length less than or equal to 64";
    const INVALID_CHARACTER: &str = r"Member must satisfy regular expression pattern: [\w+=,.@-]*";

    let value = required_parameter(parameters, "RoleSessionName", "roleSessionName")?;
    RoleSessionName::new(value.to_string()).map_err(|error| {
        let constraints: &[&str] = match error {
            RoleSessionNameError::TooShort => &[TOO_SHORT],
            RoleSessionNameError::TooLong => &[TOO_LONG],
            RoleSessionNameError::InvalidCharacter => &[INVALID_CHARACTER],
            RoleSessionNameError::TooShortAndInvalidCharacter => {
                &[INVALID_CHARACTER, TOO_SHORT]
            }
            RoleSessionNameError::TooLongAndInvalidCharacter => {
                &[INVALID_CHARACTER, TOO_LONG]
            }
        };
        let clauses = constraints
            .iter()
            .map(|constraint| {
                format!(
                    "Value '{value}' at 'roleSessionName' failed to satisfy constraint: {constraint}"
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        let plural = if constraints.len() == 1 { "" } else { "s" };
        StsRequestError::validation(format!(
            "{} validation error{plural} detected: {clauses}",
            constraints.len()
        ))
    })
}

fn validate_request_envelope(req: &S3Request) -> Result<Vec<(String, String)>, StsRequestError> {
    if req.method != http::Method::POST || req.path() != "/" {
        return Err(StsRequestError::Sender {
            status: 400,
            code: "UnknownOperationException",
            message: "Unknown operation".to_string(),
        });
    }
    if req.header("content-type") != Some(STS_QUERY_CONTENT_TYPE) {
        return Err(StsRequestError::Sender {
            status: 400,
            code: "InvalidAction",
            message: "Could not find operation for the given request".to_string(),
        });
    }
    let parameters = parse_form(&req.body)?;
    if optional_parameter(&parameters, "Action") != Some("AssumeRole")
        || optional_parameter(&parameters, "Version") != Some("2011-06-15")
    {
        return Err(StsRequestError::Sender {
            status: 400,
            code: "InvalidAction",
            message: "Could not find operation for the given request".to_string(),
        });
    }
    Ok(parameters)
}

impl HttpFrontend {
    #[must_use]
    pub(crate) fn handle_sts_request(
        &self,
        req: &S3Request,
        wire_ids: &WireResponseIds,
    ) -> S3Response {
        self.handle_sts_request_inner(req, wire_ids)
            .unwrap_or_else(|error| error.into_response(wire_ids))
    }

    fn handle_sts_request_inner(
        &self,
        req: &S3Request,
        wire_ids: &WireResponseIds,
    ) -> Result<S3Response, StsRequestError> {
        let parameters = validate_request_envelope(req)?;
        let auth = authenticate_request(
            req.method.as_str(),
            req.path(),
            req.query_string(),
            &req.header_source(),
            &req.body,
            &self.identity_provider,
            auth::ExpectedSigningRegion::ExactEndpointRegion(self.coordinator.region()),
            auth::SigningService::Sts,
            req.request_epoch_seconds(),
        )
        .map_err(StsRequestError::authentication)?;
        let caller = auth
            .identity
            .as_ref()
            .ok_or_else(|| StsRequestError::access_denied("Access denied"))?;
        let caller_authorization = match self
            .identity_provider
            .resolve_principal_authorization(caller)
            .map_err(|_| StsRequestError::Internal)?
        {
            ResolvedPrincipalAuthorization::Configured(Some(authorization)) => authorization,
            ResolvedPrincipalAuthorization::Configured(None)
            | ResolvedPrincipalAuthorization::RoleSession { .. } => {
                return Err(StsRequestError::access_denied("Access denied"));
            }
        };

        let role_arn_value = required_parameter(&parameters, "RoleArn", "roleArn")?;
        let role_arn = IamRoleArn::new(role_arn_value.to_string())
            .map_err(|_| StsRequestError::validation(format!("{role_arn_value} is invalid")))?;
        let session_name = parse_role_session_name(&parameters)?;
        let target = self
            .identity_provider
            .lookup_role_authorization_by_arn(&role_arn)
            .map_err(|_| StsRequestError::Internal)?
            .ok_or_else(|| {
                StsRequestError::access_denied(format!(
                    "User: {} is not authorized to perform: sts:AssumeRole on resource: {role_arn_value}",
                    caller_authorization.record().key().principal().principal()
                ))
            })?;
        if target
            .record()
            .evaluate_configured_caller_assume_role(caller_authorization.record())
            .map_err(|_| StsRequestError::access_denied("Access denied"))?
            != PolicyEvaluation::ExplicitAllow
        {
            return Err(StsRequestError::access_denied(format!(
                "User: {} is not authorized to perform: sts:AssumeRole on resource: {role_arn_value}",
                caller_authorization.record().key().principal().principal()
            )));
        }

        let duration_seconds = optional_parameter(&parameters, "DurationSeconds")
            .map(|value| {
                value.parse::<u32>().map_err(|_| StsRequestError::Sender {
                    status: 400,
                    code: "MalformedInput",
                    message: "malformed input".to_string(),
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_SESSION_DURATION_SECONDS);
        if duration_seconds < MIN_SESSION_DURATION_SECONDS
            || duration_seconds > target.record().maximum_session_duration().seconds()
        {
            return Err(StsRequestError::validation(format!(
                "Value '{duration_seconds}' at 'durationSeconds' is outside the permitted range"
            )));
        }

        let issued_at =
            i64::try_from(req.request_epoch_seconds()).map_err(|_| StsRequestError::Internal)?;
        let expires_at = issued_at
            .checked_add(i64::from(duration_seconds))
            .ok_or(StsRequestError::Internal)?;
        let lifetime =
            SessionLifetime::new(issued_at, expires_at).map_err(|_| StsRequestError::Internal)?;
        let issuer = self
            .identity_provider
            .resolve_authorized_role_identity(&target)
            .map_err(|_| StsRequestError::Internal)?
            .ok_or(StsRequestError::Internal)?;
        let material =
            generate_session_credential_material().map_err(|_| StsRequestError::Internal)?;
        let access_key_id = material.access_key_id().to_string();
        let secret_access_key = material.secret_key().as_str().to_string();
        let session_token = self
            .identity_provider
            .seal_session_credential(material, &issuer, session_name.clone(), lifetime, None)
            .map_err(|_| StsRequestError::Internal)?;
        let assumed_role_id = format!(
            "{}:{}",
            target.record().identity().role().stable_id().as_str(),
            session_name.as_str()
        );
        let assumed_role_arn = format!(
            "arn:aws:sts::{}:assumed-role/{}/{}",
            target.record().identity().role().account_id().as_str(),
            target.record().identity().role().name().as_str(),
            session_name.as_str()
        );

        Ok(S3Response::sts_assume_role(
            &StsAssumeRoleResponse {
                access_key_id: &access_key_id,
                secret_access_key: &secret_access_key,
                session_token: &session_token,
                expiration_epoch_seconds: u64::try_from(expires_at)
                    .map_err(|_| StsRequestError::Internal)?,
                assumed_role_id: &assumed_role_id,
                assumed_role_arn: &assumed_role_arn,
            },
            wire_ids,
        ))
    }
}
