// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn staging_abandonment_handoff_api_is_consuming_opaque_and_not_cleanup_authority() {
    let owner = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/pg_store/complete_abandonment_handoff.rs");
    let fixture = include_str!("api_fixture.rs").replace("OWNER_PATH", owner.to_str().unwrap());
    for (name, probe, diagnostic) in [
        ("consume", "let _ = owner.into_abandoned();", None),
        ("prepare", "let _ = complete.prepare_part_cleanup(section);", None),
        ("install", "let _ = prepared.install_cleanup(section); let _ = prepared.cleanup_installation();", None),
        ("publish", "let _ = prepared.publish_cleanup(section); let _ = prepared.cleanup_publication();", None),
        ("apply", "let _ = prepared.apply_cleanup_embedded(section, NodeId); let _ = prepared.cleanup_application(NodeId);", None),
        ("apply-remote", "let _ = prepared.apply_cleanup_remote(section, replica);", None),
        ("renew-remote", "let _ = prepared.apply_cleanup_remote(section, replica, std::time::Instant::now());", Some("E0061")),
        ("extract-remote-attempt", "let _ = prepared.remote_application_attempt(section, replica);", Some("E0599")),
        ("reset-application", "prepared.applications.clear();", Some("E0616")),
        ("detach-application", "let _ = prepared.application_request(NodeId);", Some("E0599")),
        ("cleanup-terminal", "let _ = prepared.into_terminal();", None),
        ("cleanup-terminal-observe", "let _ = (terminal.source_id(), terminal.cleanup_id(), terminal.work(), terminal.require_valid(), terminal.cleanup_application(NodeId));", None),
        ("cleanup-terminal-retry", "let _ = terminal_failure.into_prepared();", None),
        ("cleanup-terminal-reuse", "let _ = prepared.into_terminal(); let _ = prepared.into_terminal();", Some("E0382")),
        ("cleanup-terminal-retry-twice", "let _ = terminal_failure.into_prepared(); let _ = terminal_failure.into_prepared();", Some("E0382")),
        ("cleanup-terminal-mint", "let _ = AbandonedPartTerminalHandoff { owner: prepared, work: panic!(), retained_finalizer_reservation: TerminalReservationObservation::NoAttemptObserved };", Some("E0451")),
        ("cleanup-terminal-finalizer-release", "let _ = terminal.release_retained_finalizer_reservation(); let _ = terminal.retained_finalizer_reservation_observation();", None),
        ("cleanup-terminal-inspect", "let _ = terminal.inspect_retained_custody(section);", None),
        ("cleanup-inspection-not-retirement", "let observation = terminal.inspect_retained_custody(section).unwrap_or_else(|_| panic!()); let _ = observation.retire_pending();", Some("E0599")),
        ("cleanup-preterminal-finalizer-release", "let _ = prepared.release_retained_finalizer_reservation();", Some("E0599")),
        ("cleanup-terminal-reset-release", "terminal.retained_finalizer_reservation = TerminalReservationObservation::NoAttemptObserved;", Some("E0616")),
        ("cleanup-terminal-release-args", "let _ = terminal.release_retained_finalizer_reservation(replica);", Some("E0061")),
        ("cleanup-terminal-release-permit", "let _ = AbandonedPartFinalizerRelease { route: panic!(), cleanup: panic!(), cutoff: panic!(), observation: panic!() };", Some("E0451")),
        ("cleanup-terminal-extract-release-permit", "let _ = terminal.release_permit();", Some("E0599")),
        ("cleanup-terminal-clone", "let _ = AbandonedPartTerminalHandoff::clone(&terminal);", Some("E0277")),
        ("cleanup-terminal-extract", "let _ = terminal.owner;", Some("E0616")),
        ("cleanup-terminal-reverse", "let _ = terminal.into_prepared();", Some("E0599")),
        ("cleanup-terminal-reset", "let _ = terminal.into_publication_sender();", Some("E0599")),
        ("cleanup-terminal-release", "let _ = terminal.release_command_reservation();", Some("E0599")),
        ("cleanup-terminal-retire", "let _ = terminal.retire_quiescent_pending();", Some("E0599")),
        ("mint-publication", "let _ = AbandonedPartPublication { source: &complete, candidate: panic!(), installation: panic!() };", Some("E0451")),
        ("reset-publication", "prepared.publication = PublicationObservation::NoAttemptObserved;", Some("E0616")),
        ("publication-submission", "let _ = prepared.into_publication_sender();", Some("E0599")),
        ("reset-installation", "prepared.installation = AbandonedPartInstallationObservation::NoAttemptObserved;", Some("E0616")),
        ("mint-installation", "let _ = AbandonedPartInstallation { source: &complete, candidate: panic!(), observation: panic!() };", Some("E0451")),
        ("retry-preparation", "let _ = failed.into_handoff();", None),
        ("observe-preparation", "let _ = (prepared.source_id(), prepared.cleanup_id(), prepared.prefix(), prepared.require_valid());", None),
        ("extract-preparation", "let _ = prepared.candidate;", Some("E0616")),
        ("extract-preparation-source", "let _ = prepared.owner;", Some("E0616")),
        ("clone-preparation", "let _ = PreparedAbandonedPart::clone(&prepared);", Some("E0277")),
        ("mint-permit", "let _ = AbandonedPartPreparation { owner: &complete };", Some("E0451")),
        ("reuse-handoff", "let _ = complete.prepare_part_cleanup(section); let _ = complete.prepare_part_cleanup(section);", Some("E0382")),
        ("extract-submission", "let _ = prepared.into_submission();", Some("E0599")),
        ("return-failed", "let _ = failure.into_abandonment();", None),
        (
            "observe",
            "let _ = (complete.id(), complete.confirmed_peers(), complete.require_valid());",
            None,
        ),
        (
            "literal",
            "let _ = CompleteAbandonmentHandoff { owner };",
            Some("E0451"),
        ),
        ("extract", "let _ = complete.owner;", Some("E0616")),
        (
            "clone",
            "let _ = CompleteAbandonmentHandoff::clone(&complete);",
            Some("E0277"),
        ),
        (
            "reuse",
            "let _ = owner.into_abandoned(); let _ = owner.into_abandoned();",
            Some("E0382"),
        ),
        (
            "retry-failure-twice",
            "let _ = failure.into_abandonment(); let _ = failure.into_abandonment();",
            Some("E0382"),
        ),
        (
            "reverse",
            "let _ = complete.into_abandonment();",
            Some("E0599"),
        ),
        (
            "submission",
            "let _ = complete.into_submission();",
            Some("E0599"),
        ),
        (
            "terminal",
            "let _ = complete.into_terminal();",
            Some("E0599"),
        ),
    ] {
        let dir = test_util::tempdir();
        let mut child = Command::new("rustc")
            .args([
                "--edition=2024",
                "--crate-type=lib",
                "--emit=metadata",
                "--crate-name",
                "abandonment_handoff_api",
                "--out-dir",
            ])
            .arg(dir.path())
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(fixture.replace("PROBE", probe).as_bytes())
            .unwrap();
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
