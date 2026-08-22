// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};
use std::process::Command;

fn argmin_s3_bin() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_argmin-s3")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let current = std::env::current_exe().expect("current test executable path");
            current
                .parent()
                .and_then(Path::parent)
                .expect("integration test should run from target/*/deps")
                .join("argmin-s3")
        })
}

fn replicated_guide_manifest() -> &'static str {
    include_str!("../../../guides/configuration.md")
        .split_once("### Replicated manifest example")
        .expect("configuration guide should contain the replicated example")
        .1
        .split_once("```toml\n")
        .expect("replicated example should contain a TOML block")
        .1
        .split_once("\n```")
        .expect("replicated TOML block should be terminated")
        .0
}

#[test]
fn unsupported_manifest_schema_stops_complete_process_startup_before_state_or_material_access() {
    let bin = argmin_s3_bin();

    for version in [0, 2] {
        let dir = test_util::tempdir();
        let state_root = dir.path().join("state-must-remain-absent");
        let material_root = dir.path().join("material-must-remain-absent");
        let manifest_path = dir.path().join("cluster.toml");
        let manifest = replicated_guide_manifest()
            .replacen(
                "schema_version = 1",
                &format!("schema_version = {version}"),
                1,
            )
            .replace("/srv/argmin", state_root.to_str().unwrap())
            .replace("/etc/argmin", material_root.to_str().unwrap());
        std::fs::write(&manifest_path, manifest).unwrap();

        let output = Command::new(&bin)
            .env_clear()
            .env("ARGMIN_CLUSTER_CONFIG_PATH", &manifest_path)
            .env("ARGMIN_PROCESS_ID", "control-1")
            .output()
            .expect("argmin-s3 should run");
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "unexpected success: {stderr}");
        assert!(
            stderr.contains(&format!(
                "configuration error: unsupported cluster manifest schema version {version}"
            )),
            "unexpected stderr: {stderr}"
        );
        assert!(
            !state_root.exists(),
            "unsupported schema created or locked durable state"
        );
        assert!(
            !material_root.exists(),
            "unsupported schema accessed or created material state"
        );
    }
}
