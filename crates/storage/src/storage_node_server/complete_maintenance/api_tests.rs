// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_complete_maintenance_authority_api_rejects_sibling_minting() {
    let authority = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/storage_node_server/complete_maintenance/authority.rs");
    let fixture =
        include_str!("api_fixture.rs").replace("AUTHORITY_PATH", authority.to_str().unwrap());
    // A positive compilation proves dependencies/visibility resolve. Check each
    // forbidden construction separately, requiring its specific Rust diagnostic.
    for (name, probe, diagnostic) in [
        ("borrowed", "let _ = authority.require_effect(pg, request.cluster_epoch, Mutation::Record);", None),
        ("constructor", "let _ = CompleteLogMutationAuthority::admit(handler, session, serialization, pg, request, Mutation::Record);", Some("E0624")),
        ("literal", "let _ = CompleteLogMutationAuthority { handler, store: pg, pg_id: request.pg_id, mutation: Mutation::Record, _session: session, _serialization: serialization };", Some("E0451")),
    ] {
        let output_dir = test_util::tempdir();
        let mut child = Command::new("rustc")
            .args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "maintenance_api", "--out-dir"])
            .arg(output_dir.path())
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn().unwrap();
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
