// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_complete_publication_authority_api_rejects_sibling_minting() {
    let authority = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/storage_node_server/complete_publication/authority.rs");
    let fixture =
        include_str!("api_fixture.rs").replace("AUTHORITY_PATH", authority.to_str().unwrap());
    for (name, probe, diagnostic) in [
        ("borrowed", "let _ = authority.require_effect(pg, request);", None),
        ("constructor", "let _ = CompletePublicationAuthority::admit(handler, session, route, pg, request, Instant::now());", Some("E0624")),
        ("literal", "let (binding, fence) = handler.complete_publication_route(request).unwrap_or_else(|_| panic!()); let _ = CompletePublicationAuthority { handler, session, _route: route, store: pg, request, deadline: Instant::now(), binding, fence };", Some("E0451")),
    ] {
        let output_dir = test_util::tempdir();
        let mut child = Command::new("rustc")
            .args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "publication_api", "--out-dir"])
            .arg(output_dir.path()).arg("-").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        child.stdin.take().unwrap().write_all(fixture.replace("PROBE", probe).as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        match diagnostic {
            None => assert!(output.status.success(), "{name}: {stderr}"),
            Some(code) => {
                assert!(!output.status.success(), "{name} unexpectedly compiled");
                assert!(stderr.contains(code), "{name}: {stderr}");
            }
        }
    }
}
