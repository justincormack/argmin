// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fs::{self, DirBuilder};
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::Path;

pub(crate) const PRIVATE_DATA_DIR_MODE: u32 = 0o700;
const WORLD_WRITE: u32 = 0o002;

pub(crate) fn prepare_private_data_dir(path: &Path) -> io::Result<()> {
    let existed = path.try_exists()?;
    DirBuilder::new()
        .recursive(true)
        .mode(PRIVATE_DATA_DIR_MODE)
        .create(path)?;
    enforce_private_data_dir(path, existed)
}

fn enforce_private_data_dir(path: &Path, existed: bool) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a directory", path.display()),
        ));
    }

    let mode = metadata.permissions().mode() & 0o777;
    if existed && mode & WORLD_WRITE != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} must not be world-writable; mode is {mode:#o}",
                path.display()
            ),
        ));
    }

    if mode != PRIVATE_DATA_DIR_MODE {
        fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DATA_DIR_MODE))?;
    }

    let final_mode = fs::metadata(path)?.permissions().mode() & 0o777;
    if final_mode != PRIVATE_DATA_DIR_MODE {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} must be private; mode is {final_mode:#o}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_data_dir_is_created_private() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");

        prepare_private_data_dir(&data_dir).unwrap();

        let mode = fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, PRIVATE_DATA_DIR_MODE);
    }

    #[test]
    fn existing_readable_data_dir_is_tightened() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        fs::create_dir_all(&data_dir).unwrap();
        fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o755)).unwrap();

        prepare_private_data_dir(&data_dir).unwrap();

        let mode = fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, PRIVATE_DATA_DIR_MODE);
    }

    #[test]
    fn existing_writable_data_dir_fails_closed() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        fs::create_dir_all(&data_dir).unwrap();
        fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o777)).unwrap();

        let error = prepare_private_data_dir(&data_dir).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let _ = fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700));
    }
}
