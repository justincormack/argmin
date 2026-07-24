---
id: LOSSYSTR-001
bug_class: lossy-str-conversion
title: SigV4 query-string authentication parameters and canonical query string are decoded with UTF-8-lossy substitution instead of exact bytes
location: crates/auth/src/canonical.rs:179
function: percent_decode
confidence: High
worker: worker-12
fp_verdict: FALSE_POSITIVE
fp_rationale: "A live AWS oracle proved that S3 also canonicalizes percent-decoded invalid UTF-8 through U+FFFD: a query value signed as %EF%BF%BD remained valid after mutation to %FF, while mutation to an ordinary different value produced SignatureDoesNotMatch. Argmin's authentication and downstream query consumers share that AWS-observed normalization, so the reported verifier/consumer disagreement does not exist."
severity: NONE
attack_vector: Remote
exploitability: Difficult
severity_rationale: "No security impact: the normalization matches live AWS S3, authentication and downstream consumers agree on the normalized value, auth parameter formats fail closed, and changing only authentication would introduce rather than remove verifier/consumer disagreement."
status: invalid
---

## Description
The AWS SigV4 spec requires the canonical query string (and the individual
auth parameters carried in it) to be built from the *exact bytes* of the
request: percent-decode to raw bytes, then re-percent-encode under SigV4's
own encoding rules. This implementation instead routes every query
parameter — including the auth-critical ones — through
`String::from_utf8_lossy`, which silently substitutes U+FFFD for any byte
sequence that is not valid UTF-8:

- `crates/auth/src/canonical.rs:152` `canonical_query_string()` — used to
  build the canonical request that is hashed and checked against the
  client-supplied `X-Amz-Signature` for every presigned (query-based)
  SigV4 request.
- `crates/auth/src/request.rs:990` `query_param_lossy()` — used to extract
  `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-SignedHeaders`,
  `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-Signature`, and
  `X-Amz-Security-Token` (the STS session token) directly from the query
  string of a presigned URL.

Because the decode goes through a `str`/`String` (UTF-8) type rather than
operating on `&[u8]`, an attacker-controlled query string containing
percent-encoded invalid-UTF-8 byte sequences (e.g. a lone `%FF`) is not
rejected and not preserved byte-for-byte — it is coerced to the 3-byte
U+FFFD sequence. Any two distinct invalid byte sequences at the same
position collapse to the identical decoded string. This both (a) makes
this implementation diverge from the byte-exact canonicalization that AWS
SDKs perform when signing, which can cause interoperability/verification
failures for otherwise-legitimate requests containing non-UTF-8 bytes in
these parameters, and (b) means the value used to compute the "canonical
request hash" that anchors the entire signature check, and the value used
for the `X-Amz-Credential`/`X-Amz-Security-Token` extraction that
downstream authorization/STS logic acts on, is not the value that was
actually transmitted on the wire — it is a lossy re-interpretation of it.

## Code
```rust
// crates/auth/src/canonical.rs
/// Percent-decode a string (RFC 3986). Does NOT treat + as space.
fn percent_decode(s: &str) -> String {
    percent_decode_lossy(s).into_owned()   // U+FFFD substitution, not raw bytes
}

pub fn canonical_query_string(query: &str) -> String {
    ...
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let val = parts.next().unwrap_or("");
            (
                uri_encode(&percent_decode(key)),   // re-encodes the *lossy* string, not the original bytes
                uri_encode(&percent_decode(val)),
            )
        })
        .collect();
    ...
}
```
```rust
// crates/auth/src/request.rs
fn query_param_lossy<'a>(query: &'a str, name: &str) -> Option<Cow<'a, str>> {
    query.split('&').filter(|s| !s.is_empty()).find_map(|pair| {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next()?;
        if key != name {
            return None;
        }
        let val = parts.next().unwrap_or("");
        Some(percent_decode_lossy(val))   // used for X-Amz-Credential / X-Amz-Signature / X-Amz-Security-Token, etc.
    })
}
```

## Data flow
- **Source:** the query string of any presigned (SigV4 query-auth) S3
  request — fully attacker-controlled, arrives over the REMOTE HTTP
  surface before any authentication has succeeded.
- **Sink:**
  - `canonical_query_string()` (`crates/auth/src/canonical.rs:152`), whose
    output feeds `canonical_request()` -> the SHA-256 hash that is bound
    into the string-to-sign and checked against `X-Amz-Signature`
    (`crates/auth/src/request.rs:810-820`).
  - `query_param_lossy()` (`crates/auth/src/request.rs:990`), whose output
    is used to parse `X-Amz-Credential` (access-key id, date, region,
    service scope), `X-Amz-SignedHeaders`, `X-Amz-Date`, `X-Amz-Expires`,
    `X-Amz-Signature`, and `X-Amz-Security-Token`
    (`crates/auth/src/request.rs:683-852`).
- **Validation:** none — invalid-UTF-8 byte sequences in any of these
  parameters are silently replaced with U+FFFD rather than causing the
  request to be rejected as malformed, and rather than being preserved
  as the exact bytes the SigV4 spec canonicalization requires.

## Reachability trace
`HTTP request with presigned query string -> HttpFrontend request parsing
-> verify_presigned_request-style entry point in crates/auth/src/request.rs
(~line 675) -> query_param_lossy() for X-Amz-Credential/SignedHeaders/
Signature/Security-Token (lines 683-852) and canonical_query_string(
query_without_signature(query_string)) (line 810) -> canonical_request()
-> signature comparison`.

## Impact
This sits directly in the SigV4 authentication path that gates every S3
operation reachable via a presigned URL, including cross-account and
STS-session-token-bound requests. The concretely demonstrable impact is a
spec-compliance divergence: this server's canonicalization is not
byte-exact where the AWS SigV4 spec requires it to be, so legitimately
signed requests containing percent-encoded non-UTF-8 bytes in these
parameters can fail verification unexpectedly, and — more importantly for
a security review — the hash that anchors the signature check, and the
`X-Amz-Security-Token`/`X-Amz-Credential` values acted on afterward, are
computed from a lossily-reinterpreted string rather than the bytes that
were actually sent. This is exactly the class of canonicalization
divergence that historically enables auth confusion/bypass bugs when a
verifier and a downstream consumer disagree about what the "real" value
of a security-relevant field is; a full forged-signature exploit was not
demonstrated in this review (forging a signature still requires the
server's secret key), so this is filed as a confirmed spec/implementation
defect on an auth-critical path for the fp+severity judge to weigh.

## Mitigations checked
- No explicit rejection of non-UTF-8 percent-encoded bytes exists before
  or after this lossy decode in either `canonical.rs` or `request.rs`.
- The canonical-query and query-param-extraction lossy decoders are at
  least *internally* consistent (same `percent_decode_bytes` +
  `from_utf8_lossy` implementation is duplicated in
  `crates/auth/src/encoding.rs` and `crates/server-http/src/http/request.rs`),
  which rules out an auth-vs-canonical-string divergence *within this
  crate* for the fields checked, but does not address the divergence from
  the AWS spec's byte-exact requirement, nor a divergence against any
  other query-string consumer added in the future that does not use the
  same lossy helper.
- `percent_decode_strict` (which rejects invalid UTF-8 outright) exists
  elsewhere in `server-http` and is evidently the intended treatment for
  security-relevant decoded values; it is not used here.

## Original recommendation
Perform SigV4 canonicalization on raw `&[u8]`, not `String`/`&str`: percent-
decode into bytes, re-percent-encode those bytes directly (SigV4's
uri-encoding rules operate byte-wise and do not require UTF-8 validity),
and never route an auth-parameter or the canonical query string through
`String::from_utf8_lossy`. If a query parameter's percent-decoded bytes
are not valid UTF-8 and a `String` representation is unavoidable
downstream (e.g. for `X-Amz-Credential` field splitting), reject the
request instead of substituting U+FFFD.

## Resolution

This finding is invalid. The premise that S3 verifies the exact decoded
bytes is not true for invalid UTF-8 query values, despite the observable
lossy conversion in Argmin's implementation.

The shared endpoint-neutral regression
`test_presigned_invalid_utf8_query_value_matches_replacement_character`
creates an object and signs a presigned GET containing the otherwise-unused
query parameter `lossy=%EF%BF%BD`. It then sends three requests to both AWS
and the local server:

- the URL as signed succeeds and returns the object;
- replacing `%EF%BF%BD` with the invalid byte encoding `%FF`, without
  changing the signature, also succeeds and returns the object;
- replacing the value with `text%2Fplain`, again without changing the
  signature, returns `SignatureDoesNotMatch`.

The third request is the binding canary: AWS is not ignoring the unknown
parameter. It includes the parameter in SigV4 verification but treats the
invalid UTF-8 byte and U+FFFD as the same canonical value. Argmin matches
that behavior. The focused regression passed both locally and against live
AWS S3.

Keeping one shared normalization model is also the safer authentication
design. Argmin's canonical-request calculation and its downstream query
consumers both map invalid UTF-8 through U+FFFD. Making only the verifier
byte-exact or strict would create the very verifier/consumer disagreement
that could enable authentication confusion. Making every layer strict would
avoid that internal disagreement, but would unnecessarily diverge from the
AWS oracle. The lossy conversion is therefore intentional and is documented
beside `canonical::percent_decode` as well as pinned by the shared test.

The [public AWS SigV4 query-string documentation](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html)
says to URI-encode every byte and explains the resulting `%XX`
representation, but it does not specify how S3 decodes an incoming
percent-encoded sequence that is not valid UTF-8 before recreating the
canonical query string. The documentation therefore omits the behavior that
this oracle exposed; its byte-oriented wording is not sufficient to infer
S3's invalid-UTF-8 handling.

### Official client behavior

The locked official Rust signer aligns with the service behavior for
presigned URLs. Botocore's non-CRT Python presigner follows the same model;
this review did not verify the separate CRT implementation:

- The [locked official Rust `aws-sigv4` 1.4.5 signer](https://docs.rs/crate/aws-sigv4/1.4.5/source/src/http_request/canonical_request.rs)
  used by `s3-tests` parses the existing URI query with
  `form_urlencoded::parse`. That parser calls `decode_utf8_lossy`, and the
  signer rebuilds the encoded query. Supplying `%FF` to the signer therefore
  normalizes it to the valid UTF-8 encoding of U+FFFD before signing and
  emitting the request.
- [Botocore/Boto3's non-CRT Python query-signing path](https://github.com/boto/botocore/blob/develop/botocore/auth.py)
  imports Python's `urllib.parse.parse_qs`, parses the existing query, and
  rebuilds it with `percent_encode_sequence`. [`parse_qs` defaults to UTF-8
  with replacement on decoding errors](https://docs.python.org/3/library/urllib.parse.html#urllib.parse.parse_qs),
  so its presigned-URL path follows the same normalize-and-reencode model.
  Its header-auth canonical-query path instead preserves an already-encoded
  raw query string; a caller that manually injects `%FF` there can sign a
  value S3 will canonicalize differently. Normal modeled SDK requests use
  Unicode parameter values and do not produce such an invalid raw query.
  When `HAS_CRT` is enabled, Botocore replaces these Python signers with CRT
  implementations; the CRT path's invalid-UTF-8 behavior was not established
  by this review.

Thus live S3 and the locked Rust signer are aligned, and Botocore's non-CRT
Python presigner also aligns. No conclusion is claimed for Botocore's CRT
path. The important invariant for Argmin is to keep authentication and all
downstream consumers on the same AWS-observed normalization, not to infer
stricter receiver behavior from the incomplete documentation.
