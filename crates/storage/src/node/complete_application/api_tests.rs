// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_complete_application_embedded_api_requires_borrowed_target_and_private_authority() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let fixture = include_str!("api_fixture.rs")
        .replace(
            "HISTORICAL_TARGET_PATH",
            src.join("cluster/complete_publication_route/historical_remote.rs")
                .to_str()
                .unwrap(),
        )
        .replace(
            "REMOTE_TARGET_PATH",
            src.join("cluster/complete_fanout_route/embedded_replica.rs")
                .to_str()
                .unwrap(),
        )
        .replace(
            "OWNER_PATH",
            src.join("node/complete_application.rs").to_str().unwrap(),
        )
        .replace(
            "TARGET_PATH",
            src.join("cluster/complete_publication_route/application_target.rs")
                .to_str()
                .unwrap(),
        );
    for (name, probe, diagnostic) in [
        ("borrowed", "let _ = authority.require_effect(pg, request);", None),
        ("closed", "let _ = target.apply_complete_command(request);", None),
        ("store", "let _ = pg.apply_embedded_complete_metadata_command(authority);", None),
        ("authority", "let _ = EmbeddedApplicationAuthority { target, store: pg, request, deadline: Instant::now() };", Some("E0451")),
        ("target", "let _ = EmbeddedApplicationTarget { authority: todo!() };", Some("E0451")),
        ("remote", "let _ = route.embedded_replica_target(client, &session, NodeId, Instant::now());", None),
        ("remote_literal", "let _ = RemotePrimaryEmbeddedTarget { route: &route, _primary: todo!(), member: todo!(), deadline: Instant::now() };", Some("E0451")),
        ("remote_session", "let target = route.embedded_replica_target(client, &session, NodeId, Instant::now()).ok().unwrap(); drop(session); let _ = target.require_effect();", Some("E0505")),
        ("remote_route", "let target = route.embedded_replica_target(client, &session, NodeId, Instant::now()).ok().unwrap(); drop(route); let _ = target.require_effect();", Some("E0505")),
        ("historical", "let _ = historical.embedded_replica_target(client, &session, NodeId, Instant::now());", None),
        ("historical_session", "let target = historical.embedded_replica_target(client, &session, NodeId, Instant::now()).ok().unwrap(); drop(session); let _ = target.require_effect();", Some("E0505")),
        ("historical_route", "let target = historical.embedded_replica_target(client, &session, NodeId, Instant::now()).ok().unwrap(); drop(historical); let _ = target.require_effect();", Some("E0505")),
        ("historical_literal", "let _ = HistoricalRemoteApplicationRoute { admission: todo!(), subject: todo!(), fence: todo!(), cutoff: Instant::now() };", Some("E0451")),
        ("callback", "let _ = target.with_authority(request, |_, _| Ok(()));", Some("E0624")),
        ("detach", "let detached = section.application_target(NodeId).ok().unwrap(); drop(section); let _ = detached.require_effect();", Some("E0505")),
    ] {
        let output_dir = test_util::tempdir();
        let mut child = Command::new("rustc")
            .args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "embedded_application_api", "--out-dir"])
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
