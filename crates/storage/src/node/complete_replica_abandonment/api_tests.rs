// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_replica_abandonment_api_requires_private_custody_and_store_authority() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let fixture = include_str!("api_fixture.rs")
        .replace(
            "CUSTODY_PATH",
            src.join("pg_store/complete_replica_abandonment_owner.rs")
                .to_str()
                .unwrap(),
        )
        .replace(
            "OWNER_PATH",
            src.join("node/complete_replica_abandonment.rs")
                .to_str()
                .unwrap(),
        );
    for (name, probe, diagnostic) in [
        ("borrowed", "let _ = permit.record_embedded(request, Instant::now());", None),
        ("store", "let _ = pg.record_complete_replica_abandonment(authority);", None),
        ("permit_literal", "let _ = ReplicaAbandonmentPermit { owner, target };", Some("E0451")),
        ("authority_literal", "let _ = EmbeddedReplicaAbandonmentAuthority { permit, store: pg, request, deadline: Instant::now() };", Some("E0451")),
        ("raw_store", "let _ = pg.record_complete_replica_abandonment(request);", Some("E0308")),
        ("raw_target", "let _ = CompleteUnstartedAbandonment::record_target(owner, &mut Default::default(), target, Instant::now());", Some("E0624")),
        ("clone", "let _ = ReplicaAbandonmentPermit::clone(permit);", Some("E0277")),
    ] {
        let dir = test_util::tempdir();
        let mut child = Command::new("rustc")
            .args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "replica_abandonment_api", "--out-dir"])
            .arg(dir.path()).arg("-").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        child.stdin.take().unwrap().write_all(fixture.replace("PROBE", probe).as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        match diagnostic {
            None => assert!(output.status.success(), "{name}: {stderr}"),
            Some(code) => { assert!(!output.status.success(), "{name} unexpectedly compiled"); assert!(stderr.contains(code), "{name}: {stderr}"); }
        }
    }
}
