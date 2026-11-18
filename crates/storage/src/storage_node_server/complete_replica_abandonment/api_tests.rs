// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_replica_abandonment_receiver_api_requires_private_store_bound_authority() {
    let owner = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/storage_node_server/complete_replica_abandonment.rs");
    let fixture = include_str!("api_fixture.rs").replace("OWNER_PATH", owner.to_str().unwrap());
    for (name, probe, diagnostic) in [
        ("borrowed", "let _ = authority.require_received_request(pg, request);", None),
        ("closed", "let _ = handler.complete_replica_abandonment_response(session, route, &[]);", None),
        ("literal", "let _ = RemoteReplicaAbandonmentAuthority { handler, _route: route, _serialization: guard, store: pg, request, fence: StorageNodeRouteFence::current(&handler.config, None), deadline: Instant::now() };", Some("E0451")),
        ("callback", "let _ = handler.with_replica_abandonment(session, route, request, Instant::now(), |_, _| ());", Some("E0624")),
        ("clone", "let _ = RemoteReplicaAbandonmentAuthority::clone(authority);", Some("E0277")),
    ] {
        let dir = test_util::tempdir();
        let mut child = Command::new("rustc").args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "replica_abandonment_receiver_api", "--out-dir"])
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
