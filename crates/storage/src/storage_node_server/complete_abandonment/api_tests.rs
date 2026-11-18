// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_abandonment_receiver_authority_api_rejects_sibling_constructor_and_literal() {
    let authority = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/storage_node_server/complete_abandonment/authority.rs");
    let fixture =
        include_str!("api_fixture.rs").replace("AUTHORITY_PATH", authority.to_str().unwrap());
    for (name, probe, expected) in [
        ("borrowed", "let _ = authority.require_effect(pg, request);", None),
        ("constructor", "let _ = CompleteAbandonmentAuthority::admit(handler, session, route, pg, request, handler.complete_abandonment_route(request).ok().unwrap(), Instant::now());", Some("E0624")),
        ("literal", "let _ = CompleteAbandonmentAuthority { handler, session, _route: route, store: pg, request, binding: handler.complete_abandonment_route(request).ok().unwrap().0, fence: StorageNodeRouteFence, deadline: Instant::now() };", Some("E0451")),
    ] {
        let dir = test_util::tempdir();
        let mut child = Command::new("rustc").args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "abandonment_api", "--out-dir"]).arg(dir.path()).arg("-").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        child.stdin.take().unwrap().write_all(fixture.replace("PROBE", probe).as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        if let Some(code) = expected { assert!(!output.status.success(), "{name} compiled"); assert!(stderr.contains(code), "{name}: {stderr}"); }
        else { assert!(output.status.success(), "{name}: {stderr}"); }
    }
}
