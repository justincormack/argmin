---
id: RECURSEDROP-001
bug_class: recursive-drop-stack-overflow
title: Unbounded XML nesting depth in lifecycle-config parser causes stack overflow on implicit Drop
location: crates/s3-types/src/lifecycle.rs:1111
function: parse_xml_document
confidence: High
worker: worker-11
fp_verdict: TRUE_POSITIVE
fp_rationale: "Confirmed unbounded-depth XmlElement tree built from a 2MB PutBucketLifecycleConfiguration body, dropped via compiler-derived recursive Drop before per-bucket authorization runs. The production authentication path also admits anonymous requests, so no signing credentials are required."
severity: HIGH
attack_vector: Remote
exploitability: Reliable
severity_rationale: "A single ~2MB deeply-nested request deterministically stack-overflows and aborts the entire process (not just the request), killing every in-flight S3 client's connections -- a reliable, broadly-reachable remote DoS matching the HIGH remote-DoS criteria."
status: fixed
---

## Description

`XmlElement` is a hand-rolled recursive tree type (`children: Vec<XmlElement>`,
recursive via `Vec<Self>`) used only by the custom, non-`quick-xml` parser in
`crates/s3-types/src/lifecycle.rs` to parse `PutBucketLifecycleConfiguration`
request bodies. `parse_xml_document` builds this tree with an explicit `Vec`
used as a parse stack (so the *parsing* itself is iterative and does not
overflow), but it imposes **no limit on nesting depth** — every `<tag>...</tag>`
pair nests one level deeper regardless of tag name, and the loop happily
accepts arbitrarily deep, balanced nesting up to the byte-size cap alone.

`XmlElement` has no hand-written `impl Drop`. The compiler-generated Drop glue
for `Vec<XmlElement>` walks the tree recursively: dropping `Vec<XmlElement>`
drops each `XmlElement`, whose `children: Vec<XmlElement>` field is then
dropped, recursing to the next level, one stack frame per nesting level. A
tree built from N levels of pure nesting (`<a><a>...<a></a>...</a></a>`)
consumes N stack frames when it goes out of scope — even if the tree is
*rejected* by later semantic validation, because the reject happens after the
tree is fully built and only causes an early return, which still drops
`root`.

This is exactly the documented Rust footgun in `rust-lang/rust#58068`
("Recursive Drop causes stack overflow ... still open"): stack overflow is
not a panic, is not caught by `catch_unwind`, and aborts the whole process
regardless of panic strategy.

## Code

```rust
// crates/s3-types/src/lifecycle.rs
struct XmlElement {
    name: String,
    children: Vec<XmlElement>,   // recursive via Vec<Self> — no depth cap anywhere
    text: String,
}

fn parse_xml_document(data: &[u8]) -> Result<XmlElement, LifecycleConfigError> {
    ...
    let mut stack: Vec<XmlElement> = Vec::new();
    while index < bytes.len() {
        ...
        // open tag: unconditionally `stack.push(element)` — no check on stack.len()
        // close tag: unconditionally `parent.children.push(element)` — no depth check
    }
    ...
}

pub fn parse_lifecycle_configuration_xml(
    data: &[u8],
) -> Result<BucketLifecycleConfiguration, LifecycleConfigError> {
    let root = parse_xml_document(data)?;         // tree fully built here, unbounded depth
    if root.name != "LifecycleConfiguration" {
        return Err(LifecycleConfigError::MalformedXml { .. }); // `root` still drops here
    }
    ...
}
```

```rust
// crates/server-http/src/http/xml.rs
pub fn parse_bucket_lifecycle_configuration_xml(
    data: &[u8],
) -> Result<BucketLifecycleConfiguration, ServerError> {
    const MAX_LIFECYCLE_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024; // size cap only
    ensure_xml_body_size(data, MAX_LIFECYCLE_CONFIGURATION_BYTES)?;   // no depth cap
    s3_types::parse_lifecycle_configuration_xml(data)...
}
```

## Data flow

- **Source:** HTTP request body of a `PutBucketLifecycleConfiguration` S3 API
  call (`S3Operation::PutBucketLifecycle` in
  `crates/server-http/src/http/mod.rs:2642`), fully attacker-controlled and
  bounded only at 2 MiB by `ensure_xml_body_size`.
- **Sink:** implicit recursive `Drop` of the `XmlElement` tree returned by
  `parse_xml_document` (`crates/s3-types/src/lifecycle.rs:1137`), first
  dropped when `parse_lifecycle_configuration_xml`
  (`crates/s3-types/src/lifecycle.rs:286`) returns (success *or* the
  `MalformedXml` early-return on line 291).
- **Validation:** only a total-byte-size cap (2 MiB); no check on nesting
  depth, element count, or stack size anywhere in the parse path.

## Reachability trace

```
HTTP PUT .../?lifecycle  (S3Operation::PutBucketLifecycle)
  → server-http/src/http/mod.rs:2645
      xml::parse_bucket_lifecycle_configuration_xml(&req.body)
  → server-http/src/http/xml.rs:3034
      s3_types::parse_lifecycle_configuration_xml(data)
  → s3-types/src/lifecycle.rs:289
      let root = parse_xml_document(data)?;   // builds unbounded-depth tree
  → s3-types/src/lifecycle.rs:291 / :308  (fn returns, `root` dropped here)
      recursive Drop glue over Vec<XmlElement> → stack overflow → SIGSEGV, whole process aborts
```

Note: authentication runs before this dispatch arm, but missing authentication
is accepted as the anonymous identity. Bucket/operation authorization runs
**after** the XML is parsed and dropped. An anonymous caller can therefore
trigger the vulnerable parser by supplying a valid request checksum; signing
credentials and lifecycle permission are not required.

## Impact

A single ~2 MiB HTTP request (`<a><a>...<a></a>...</a></a>`, ~300,000 levels
of nesting at 7 bytes/level) from an anonymous remote caller causes the worker
thread handling the request to overflow its stack
while dropping the parsed `XmlElement` tree. Stack overflow triggers the
Rust runtime's SIGSEGV guard-page handler, which aborts the **entire process**
— every in-flight request across every tenant is killed, not just the
attacker's connection. This is a full remote denial-of-service against the
S3-compatible server from a single request.

## Mitigations checked

- Body size cap: present (2 MiB via `ensure_xml_body_size`), but does not
  bound nesting *depth* — 2 MiB of minimal 1-character tags still yields
  ~300K levels, vastly more than needed to exhaust any realistic thread stack
  (tokio's default worker stack is a few MiB; this server does not configure
  a custom `thread_stack_size` in `crates/argmin-s3/src/main.rs`).
- `// SAFETY:` / manual `impl Drop`: none — `XmlElement` relies entirely on
  the compiler-derived recursive Drop.
- `catch_unwind` / `panic = "abort"` vs `"unwind"`: irrelevant — stack
  overflow is not a panic and is not caught either way.
- No `serde`-style recursion limit applies here; this is a hand-rolled parser,
  not `serde_json`/`quick-xml`. (All *other* XML endpoints in
  `server-http/src/http/xml.rs` — ACL, tagging, CORS, retention, etc. — use
  the streaming `quick_xml::Reader` event API and never materialize a
  recursive tree, so they are not affected by this issue.)

## Recommendation

Enforce an explicit maximum nesting depth in `parse_xml_document` (e.g. reject
once `stack.len()` exceeds a small constant such as 32, matching realistic
`LifecycleConfiguration` shapes which never nest more than a few levels deep),
in addition to the existing byte-size cap. As defense in depth, also give
`XmlElement` a manual iterative `Drop` impl that drains `children` into a
worklist `Vec` and pops in a loop rather than relying on the derived recursive
Drop, so that even a tree that slips past the depth cap (e.g. via a future
code path that also constructs `XmlElement`) cannot overflow the stack when
dropped.

## Resolution

Fixed in `d121bc86` (`Harden lifecycle XML parsing against deep nesting`).

The lifecycle parser now uses the existing `quick-xml` dependency as a bounded
event stream and constructs `LifecycleRule`, filter, tag, expiration, and abort
builders directly. The recursive `XmlElement` tree and hand-written XML token
scanner have been removed entirely, so no attacker-controlled recursive value
remains for compiler-generated drop glue to destroy.

The typed parser rejects unexpected structure as soon as its start event is
observed and independently caps open XML state at 32 elements. It retains the
existing lifecycle semantic validation and AWS-facing error categories while
also delegating XML end-tag and attribute syntax validation to `quick-xml`.

The deterministic regression
`parse_lifecycle_configuration_rejects_deep_nesting_without_recursive_drop`
submits a 1.4 MiB lifecycle document containing 200,000 balanced nested
elements through the server HTTP XML boundary. It completes with
`MalformedXML` without recursive construction or destruction. The existing
lifecycle parser, renderer, checksum, and HTTP operation suites continue to
cover valid and semantically invalid lifecycle configurations.

The shared endpoint-neutral regression
`test_put_bucket_lifecycle_rejects_nested_unknown_elements_as_malformed_xml`
also pins the public response category and non-mutation behavior. It passes
against both the local server and live AWS S3, which return `MalformedXML` for
the nested unknown-element document.
