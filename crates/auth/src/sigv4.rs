/// SigV4 signature verification.
use ring::hmac;

use crate::canonical::{
    amz_date_matches_date_stamp, canonical_headers, canonical_query_string, canonical_request,
    sha256_hex, string_to_sign,
};
use crate::credential::{
    parse_credential_scope_ref, CredentialRecord, CredentialScope, CredentialStore, SecretKey,
};
use crate::encoding::hex_encode_lower;
use crate::error::AuthError;
use crate::request::HeaderSource;
use crate::{is_lower_hex, MAX_SIGNED_HEADERS_LEN, MAX_SIGNED_HEADER_COUNT, SIGNATURE_HEX_LEN};

/// Parsed AWS SigV4 Authorization header.
#[derive(Clone)]
pub struct SigV4Auth {
    pub credential: CredentialScope,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

impl std::fmt::Debug for SigV4Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let signed_headers: Vec<_> = self
            .signed_headers
            .iter()
            .map(|header| observability::escaped(header))
            .collect();
        f.debug_struct("SigV4Auth")
            .field("credential", &self.credential)
            .field("signed_headers", &signed_headers)
            .field("signature", &observability::redacted("sigv4_signature"))
            .finish()
    }
}

/// Parse an AWS SigV4 Authorization header value.
///
/// Expected format:
/// ```text
/// AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,
/// SignedHeaders=host;x-amz-content-sha256;x-amz-date,
/// Signature=hex
/// ```
pub fn parse_auth_header(value: &str) -> Result<SigV4Auth, AuthError> {
    let value = value.trim();
    let rest = value
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or(AuthError::UnsupportedAuthType)?
        .trim_start();

    // Parse the three components: Credential, SignedHeaders, Signature
    let mut credential_str = None;
    let mut signed_headers_str = None;
    let mut signature_str = None;

    for part in rest.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Credential=") {
            credential_str = Some(val);
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers_str = Some(val);
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature_str = Some(val);
        }
    }

    let credential_str = credential_str.ok_or(AuthError::MalformedAuth)?;
    let signed_headers_str = signed_headers_str.ok_or(AuthError::MalformedAuth)?;
    let signature_str = signature_str.ok_or(AuthError::MalformedAuth)?;

    if signed_headers_str.is_empty() || signed_headers_str.len() > MAX_SIGNED_HEADERS_LEN {
        return Err(AuthError::MalformedAuth);
    }
    if signature_str.len() != SIGNATURE_HEX_LEN || !is_lower_hex(signature_str) {
        return Err(AuthError::MalformedAuth);
    }

    let credential = CredentialScope::from(
        parse_credential_scope_ref(credential_str).ok_or(AuthError::MalformedAuth)?,
    );

    let signed_headers: Vec<String> = signed_headers_str
        .split(';')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    if signed_headers.is_empty() || signed_headers.len() > MAX_SIGNED_HEADER_COUNT {
        return Err(AuthError::MalformedAuth);
    }

    Ok(SigV4Auth {
        credential,
        signed_headers,
        signature: signature_str.to_string(),
    })
}

pub(crate) fn unsigned_required_headers<H, S>(signed_headers: &[S], headers: &H) -> Vec<String>
where
    H: HeaderSource + ?Sized,
    S: AsRef<str>,
{
    let mut unsigned_headers: Vec<String> = Vec::new();
    if headers.first_value("host").is_some()
        && !signed_headers
            .iter()
            .any(|signed_header| signed_header.as_ref() == "host")
    {
        unsigned_headers.push("host".to_string());
    }
    headers.visit(|name, _| {
        if name.starts_with("x-amz-")
            && !signed_headers
                .iter()
                .any(|signed_header| signed_header.as_ref() == name)
            && !unsigned_headers.iter().any(|header| header == name)
        {
            unsigned_headers.push(name.to_string());
        }
    });
    unsigned_headers
}

/// Derive the SigV4 signing key.
///
/// SigningKey = HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(
///     "AWS4" + secret, date), region), service), "aws4_request")
pub fn derive_signing_key(
    secret: &SecretKey,
    date: &str,
    region: &str,
    service: &str,
) -> hmac::Tag {
    let k_secret = format!("AWS4{}", secret.as_str());
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
}

pub(crate) struct VerifyRequestRecordInput<'a, H: HeaderSource + ?Sized> {
    pub method: &'a str,
    pub uri: &'a str,
    pub query_string: &'a str,
    pub headers: &'a H,
    pub body_hash: &'a str,
    pub auth: &'a SigV4Auth,
    pub now_epoch_secs: u64,
}

pub(crate) fn verify_request_record<'a, H: HeaderSource + ?Sized>(
    input: VerifyRequestRecordInput<'_, H>,
    store: &'a CredentialStore,
) -> Result<(&'a CredentialRecord, String), AuthError> {
    let VerifyRequestRecordInput {
        method,
        uri,
        query_string,
        headers,
        body_hash,
        auth,
        now_epoch_secs,
    } = input;

    // Look up the secret key
    let record = store
        .get_record(&auth.credential.access_key_id)
        .ok_or(AuthError::UnknownAccessKey)?;
    if !record.enabled {
        return Err(AuthError::UnknownAccessKey);
    }
    if let Some(expiry) = record.expires_at_epoch_secs {
        if now_epoch_secs > expiry {
            return Err(AuthError::ExpiredToken);
        }
    }
    let secret = &record.secret_key;

    // Extract signed headers — collect all values for each header name
    // to handle duplicate headers (values combined by canonical_headers).
    let mut signed_header_pairs: Vec<(&str, &str)> = Vec::new();
    for signed_name in &auth.signed_headers {
        let mut found = false;
        headers.visit(|name, value| {
            if name == signed_name.as_str() {
                signed_header_pairs.push((signed_name.as_str(), value));
                found = true;
            }
        });
        if !found {
            return Err(AuthError::MissingSignedHeader {
                header: signed_name.clone(),
            });
        }
    }

    // AWS requires Host and all x-amz-* headers to be signed (security:
    // prevents injection of request-routing and S3 control headers).
    let unsigned_headers = unsigned_required_headers(&auth.signed_headers, headers);
    if !unsigned_headers.is_empty() {
        return Err(AuthError::UnsignedHeaders {
            headers: unsigned_headers,
        });
    }

    let canonical_hdrs = canonical_headers(&signed_header_pairs);
    let signed_headers_str = auth.signed_headers.join(";");
    let canonical_qs = canonical_query_string(query_string);

    let creq = canonical_request(
        method,
        uri,
        &canonical_qs,
        &canonical_hdrs,
        &signed_headers_str,
        body_hash,
    );
    let creq_hash = sha256_hex(creq.as_bytes());

    // Find the x-amz-date header for the timestamp
    let timestamp = headers
        .first_value("x-amz-date")
        .ok_or(AuthError::MissingSignedHeader {
            header: "x-amz-date".to_string(),
        })?;
    if !amz_date_matches_date_stamp(timestamp, &auth.credential.date) {
        return Err(AuthError::MalformedAuth);
    }

    let scope = format!(
        "{}/{}/{}/aws4_request",
        auth.credential.date, auth.credential.region, auth.credential.service
    );

    let sts = string_to_sign(timestamp, &scope, &creq_hash);

    // Derive signing key and compute expected signature
    let signing_key = derive_signing_key(
        secret,
        &auth.credential.date,
        &auth.credential.region,
        &auth.credential.service,
    );

    let expected_sig = hmac_sha256(signing_key.as_ref(), sts.as_bytes());
    let expected_hex = hex_encode(expected_sig.as_ref());

    // Constant-time comparison to prevent timing attacks on signature values.
    if !crate::constant_time_eq(expected_hex.as_bytes(), auth.signature.as_bytes()) {
        return Err(AuthError::SignatureMismatch {
            diagnostics: Some(Box::new(crate::SignatureMismatchDiagnostics {
                access_key_id: auth.credential.access_key_id.to_string(),
                string_to_sign: sts,
                signature_provided: auth.signature.to_string(),
                canonical_request: Some(creq),
            })),
        });
    }

    Ok((record, creq))
}

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data)
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    hex_encode_lower(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::parse_amz_date;
    use crate::request::{authenticate_request, AuthContext, AuthMode, ExpectedSigningRegion};

    fn example_store() -> CredentialStore {
        let mut store = CredentialStore::new();
        store.add(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        );
        store
    }

    fn aws_example_time() -> u64 {
        parse_amz_date("20130524T000000Z").unwrap()
    }

    fn with_auth_header<'a>(
        auth_header: &'a str,
        headers: &'a [(&'a str, &'a str)],
    ) -> Vec<(&'a str, &'a str)> {
        let mut with_auth = Vec::with_capacity(headers.len() + 1);
        with_auth.push(("authorization", auth_header));
        with_auth.extend_from_slice(headers);
        with_auth
    }

    fn authenticate_header_for_test(
        method: &str,
        uri: &str,
        query_string: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        store: &CredentialStore,
    ) -> Result<AuthContext, AuthError> {
        authenticate_request(
            method,
            uri,
            query_string,
            headers,
            body,
            store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
    }

    #[test]
    fn parse_auth_header_valid() {
        let header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
            Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024";

        let auth = parse_auth_header(header).unwrap();
        assert_eq!(auth.credential.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(auth.credential.date, "20130524");
        assert_eq!(auth.credential.region, "us-east-1");
        assert_eq!(auth.credential.service, "s3");
        assert_eq!(
            auth.signed_headers,
            vec!["host", "range", "x-amz-content-sha256", "x-amz-date"]
        );
        assert_eq!(
            auth.signature,
            "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024"
        );
    }

    #[test]
    fn parse_auth_header_missing_prefix() {
        assert!(parse_auth_header("Basic foo").is_err());
    }

    #[test]
    fn parse_auth_header_missing_credential() {
        let header = "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(parse_auth_header(header).is_err());
    }

    #[test]
    fn parse_auth_header_bad_credential_format() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/bad, SignedHeaders=host, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(parse_auth_header(header).is_err());
    }

    #[test]
    fn parse_auth_header_invalid_credential_date() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/2013052X/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(parse_auth_header(header).is_err());
    }

    #[test]
    fn derive_signing_key_aws_example() {
        // Verified correct via the full SigV4 e2e tests (GET/PUT object examples).
        // The signing key for s3 differs from the iam example in the AWS general docs.
        let secret = SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string());
        let tag = derive_signing_key(&secret, "20130524", "us-east-1", "s3");
        let hex = hex_encode(tag.as_ref());
        assert_eq!(
            hex,
            "dbb893acc010964918f1fd433add87c70e8b0db6be30c1fbeafefa5ec6ba8378"
        );
    }

    #[test]
    fn credential_store_lookup() {
        let store = example_store();
        assert!(store.get_record("AKIAIOSFODNN7EXAMPLE").is_some());
        assert!(store.get_record("NONEXISTENT").is_none());
    }

    // AWS SigV4 test: GET object
    // From: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    #[test]
    fn sigv4_get_object_example() {
        let store = example_store();

        let method = "GET";
        let uri = "/test.txt";
        let query_string = "";
        let body_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", body_hash),
            ("x-amz-date", "20130524T000000Z"),
        ];

        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
            Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";

        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test(method, uri, query_string, &headers, b"", &store);
        assert!(result.is_ok(), "verify failed: {:?}", result.err());
        let ctx = result.unwrap();
        assert_eq!(ctx.mode, AuthMode::HeaderSigV4);
        assert_eq!(ctx.access_key_id.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
    }

    // AWS SigV4 test: PUT object
    #[test]
    fn sigv4_put_object_example() {
        let store = example_store();

        let method = "PUT";
        let uri = "/test$file.text";
        let query_string = "";
        let body_hash = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072";
        let headers = [
            ("date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", body_hash),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ];

        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, \
            Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd";

        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test(method, uri, query_string, &headers, b"", &store);
        assert!(result.is_ok(), "verify failed: {:?}", result.err());
    }

    // AWS SigV4 test: GET bucket lifecycle
    #[test]
    fn sigv4_get_bucket_lifecycle_example() {
        let store = example_store();

        let method = "GET";
        let uri = "/";
        let query_string = "lifecycle";
        let body_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", body_hash),
            ("x-amz-date", "20130524T000000Z"),
        ];

        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543";

        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test(method, uri, query_string, &headers, b"", &store);
        assert!(result.is_ok(), "verify failed: {:?}", result.err());
    }

    #[test]
    fn wrong_signature_fails() {
        let store = example_store();

        let method = "GET";
        let uri = "/test.txt";
        let body_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", body_hash),
            ("x-amz-date", "20130524T000000Z"),
        ];

        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=0000000000000000000000000000000000000000000000000000000000000000";

        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test(method, uri, "", &headers, b"", &store);
        assert!(matches!(result, Err(AuthError::SignatureMismatch { .. })));
    }

    #[test]
    fn parse_auth_header_wrong_suffix() {
        // 5 parts but last is not "aws4_request"
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/wrong_suffix, SignedHeaders=host, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(matches!(
            parse_auth_header(header),
            Err(AuthError::MalformedAuth)
        ));
    }

    #[test]
    fn parse_auth_header_missing_signed_headers() {
        let header =
            "AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert!(matches!(
            parse_auth_header(header),
            Err(AuthError::MalformedAuth)
        ));
    }

    #[test]
    fn parse_auth_header_missing_signature() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request, SignedHeaders=host";
        let err = parse_auth_header(header).unwrap_err();
        assert_eq!(err.to_string(), AuthError::MalformedAuth.to_string());
    }

    #[test]
    fn parse_auth_header_empty_signed_headers() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request, SignedHeaders=, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let err = parse_auth_header(header).unwrap_err();
        assert_eq!(err.to_string(), AuthError::MalformedAuth.to_string());
    }

    #[test]
    fn parse_auth_header_ignores_unknown_components() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request, Foo=bar, SignedHeaders=host, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let auth = parse_auth_header(header).unwrap();
        assert_eq!(auth.credential.access_key_id, "AKID");
        assert_eq!(auth.signed_headers, vec!["host"]);
        assert_eq!(
            auth.signature,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn authenticate_header_missing_host_header() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        // Provide x-amz-date and x-amz-content-sha256 but NOT host
        let headers = [
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(
            result,
            Err(AuthError::MissingSignedHeader { header }) if header == "host"
        ));
    }

    #[test]
    fn authenticate_header_missing_amz_date_header() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        // Provide host and x-amz-content-sha256 but NOT x-amz-date
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(
            result,
            Err(AuthError::MissingSignedHeader { header }) if header == "x-amz-date"
        ));
    }

    #[test]
    fn authenticate_header_missing_content_sha256_header() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        // Provide host and x-amz-date but NOT x-amz-content-sha256
        let headers = [("host", "example.com"), ("x-amz-date", "20130524T000000Z")];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(
            result,
            Err(AuthError::MissingSignedHeader { header }) if header == "x-amz-content-sha256"
        ));
    }

    #[test]
    fn authenticate_header_rejects_credential_date_mismatch() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130525/us-east-1/s3/aws4_request, \
            SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
            Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41";
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/test.txt", "", &headers, b"", &store);
        assert!(matches!(result, Err(AuthError::MalformedAuth)));
    }

    #[test]
    fn authenticate_header_missing_optional_header_is_rejected() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-custom-header, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("x-amz-date", "20130524T000000Z"),
            // x-custom-header is signed but not present.
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(
            result,
            Err(AuthError::MissingSignedHeader { header }) if header == "x-custom-header"
        ));
    }

    #[test]
    fn authenticate_header_disabled_key() {
        let mut store = CredentialStore::new();
        store.add_record(crate::credential::CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            account: s3_types::AccountIdentity::from_principal("u1"),
            authorization_profile: crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs: None,
            enabled: false,
        });
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com"), ("x-amz-date", "20130524T000000Z")];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        let err = result.unwrap_err();
        assert_eq!(err.to_string(), AuthError::UnknownAccessKey.to_string());
    }

    #[test]
    fn authenticate_header_unsigned_amz_header() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        // x-amz-meta-custom is present but not in SignedHeaders
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-meta-custom", "value"),
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(result, Err(AuthError::UnsignedHeaders { .. })));
    }

    #[test]
    fn authenticate_header_unsigned_host_header() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=x-amz-content-sha256;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(
            result,
            Err(AuthError::UnsignedHeaders { headers }) if headers == ["host"]
        ));
    }

    #[test]
    fn authenticate_header_duplicate_unsigned_amz_header() {
        let store = example_store();
        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        // Two instances of the same unsigned x-amz header — should only appear once in error
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-meta-custom", "val1"),
            ("x-amz-meta-custom", "val2"),
        ];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        let err = result.unwrap_err();
        let debug = format!("{err:?}");
        // The duplicated header should only be reported once.
        assert!(debug.contains("x-amz-meta-custom"));
        assert_eq!(debug.matches("x-amz-meta-custom").count(), 1);
    }

    #[test]
    fn unknown_access_key_fails() {
        let store = example_store();

        let auth_header = "AWS4-HMAC-SHA256 \
            Credential=UNKNOWNKEY123456/20130524/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-date, \
            Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let headers = [("host", "example.com"), ("x-amz-date", "20130524T000000Z")];
        let headers = with_auth_header(auth_header, &headers);
        let result = authenticate_header_for_test("GET", "/", "", &headers, b"", &store);
        assert!(matches!(result, Err(AuthError::UnknownAccessKey)));
    }

    #[test]
    fn sigv4_auth_debug_redacts_signature_and_escapes_headers() {
        let auth = SigV4Auth {
            credential: CredentialScope {
                access_key_id: "AK\r\nID".into(),
                date: "20250101".into(),
                region: "us-east-1".into(),
                service: "s3".into(),
            },
            signed_headers: vec!["host".into(), "x-amz-meta-\nname".into()],
            signature: "deadbeef".into(),
        };

        let debug = format!("{auth:?}");
        assert!(debug.contains(r#""AK\r\nID""#));
        assert!(debug.contains(r#""x-amz-meta-\nname""#));
        assert!(debug.contains("<redacted:sigv4_signature>"));
        assert!(!debug.contains("deadbeef"));
    }
}
