// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_replica_abandonment_sender_api_requires_original_holder_and_primary_borrow() {
    let owner = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/pg_store/complete_replica_abandonment_sender.rs");
    let fixture = include_str!("api_fixture.rs").replace("OWNER_PATH", owner.to_str().unwrap());
    for (name, probe, diagnostic) in [
        ("closed", "let _ = holder.record_remote_replica_from_embedded_primary(section, client, Instant::now());", None),
        ("send", "let _ = client.send_complete_replica_abandonment(attempt);", None),
        ("literal", "let _ = CompleteReplicaAbandonmentAttempt { owner, primary: panic!(), transport, observation, previous: ReplicaAbandonmentObservation::NoAttemptObserved };", Some("E0451")),
        ("primary", "let _ = owner::PrimarySerialization::Embedded(section);", Some("E0603")),
        ("callback", "let _ = holder.with_replica_remote_attempt(client, panic!(), Instant::now(), |_| Ok(()));", Some("E0624")),
        ("clone", "let _ = CompleteReplicaAbandonmentAttempt::clone(&attempt);", Some("E0277")),
        ("reuse", "let _ = client.send_complete_replica_abandonment(attempt); let _ = client.send_complete_replica_abandonment(attempt);", Some("E0382")),
    ] {
        let dir = test_util::tempdir();
        let mut child = Command::new("rustc").args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "replica_abandonment_sender_api", "--out-dir"])
            .arg(dir.path()).arg("-").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        child.stdin.take().unwrap().write_all(fixture.replace("PROBE", probe).as_bytes()).unwrap();
        let output = child.wait_with_output().unwrap(); let stderr = String::from_utf8(output.stderr).unwrap();
        match diagnostic { None => assert!(output.status.success(), "{name}: {stderr}"), Some(code) => { assert!(!output.status.success(), "{name} unexpectedly compiled"); assert!(stderr.contains(code), "{name}: {stderr}"); } }
    }
}
