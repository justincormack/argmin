// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_complete_publication_embedded_api_rejects_minting_and_callback_escape() {
    let source =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/node/complete_publication.rs");
    let fixture = include_str!("api_fixture.rs").replace("OWNER_PATH", source.to_str().unwrap());
    for (name, probe, diagnostic) in [
        ("borrowed", "let _ = authority.require_effect(pg, request);", None),
        ("ack-authority-mint", "let _ = engine::EmbeddedStagingAckAuthority { runtime: panic!(), store: pg, batch: panic!() };", Some("E0451")),
        ("section", "let _ = section.start_complete_publication(request, Instant::now());", None),
        ("retirement_without_terminal", "let _ = section.retire_complete_publication(request, authority, Instant::now());", Some("E0308")),
        ("abandonment_without_custody", "let _ = section.abandon_unstarted_complete_publication(request, authority, Instant::now());", Some("E0308")),
        ("installation_without_custody", "let _ = section.install_abandoned_part_cleanup(request, authority, Instant::now());", Some("E0308")),
        ("cleanup_publication_without_custody", "let _ = section.publish_abandoned_part_cleanup(authority, Instant::now());", Some("E0308")),
        ("cleanup_inspection_without_custody", "let _ = section.inspect_abandoned_part_custody(authority, Instant::now());", Some("E0308")),
        ("literal", "let _ = EmbeddedPublicationAuthority { section, store: pg, request, deadline: Instant::now() };", Some("E0451")),
        ("callback", "let _ = section.with_authority(request, Instant::now(), |_, _| Ok(()));", Some("E0624")),
    ] {
        let output_dir = test_util::tempdir();
        let mut child = Command::new("rustc")
            .args(["--edition=2024", "--crate-type=lib", "--emit=metadata", "--crate-name", "embedded_publication_api", "--out-dir"])
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
