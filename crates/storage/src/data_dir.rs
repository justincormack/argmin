// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::ffi::{CString, OsStr};
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Component;
use std::path::{Path, PathBuf};

pub(crate) const PRIVATE_DATA_DIR_MODE: u32 = 0o700;
const WORLD_WRITE: u32 = 0o002;

pub(crate) fn prepare_private_data_dir(path: &Path) -> io::Result<()> {
    prepare_private_data_dir_with_sync(path, |_, directory| directory.sync_all())
}

fn prepare_private_data_dir_with_sync<F>(path: &Path, mut sync: F) -> io::Result<()>
where
    F: FnMut(&Path, &File) -> io::Result<()>,
{
    let (directories, final_created) = open_or_create_directory_chain(path)?;
    let (final_path, final_directory) = directories.last().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "private data directory must name a directory below its filesystem anchor",
        )
    })?;
    enforce_private_data_dir(final_path, final_directory, !final_created)?;

    // Always replay the complete proof, including when a previous call
    // created every directory but failed during a later sync. Descriptors are
    // held across the pass, so sync cannot be redirected through a symlink.
    for (directory_path, directory) in directories.iter().rev() {
        sync(directory_path, directory)?;
    }
    Ok(())
}

fn open_or_create_directory_chain(path: &Path) -> io::Result<(Vec<(PathBuf, File)>, bool)> {
    let absolute = path.is_absolute();
    let anchor_path = if absolute {
        Path::new("/")
    } else {
        Path::new(".")
    };
    let anchor = open_directory_nofollow(anchor_path)?;
    let mut directories = vec![(anchor_path.to_path_buf(), anchor)];
    let components = path
        .components()
        .filter_map(|component| match component {
            Component::RootDir | Component::CurDir => None,
            Component::Normal(name) => Some(Ok(name.to_os_string())),
            Component::ParentDir => Some(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "private data directory {} must not contain '..'",
                    path.display()
                ),
            ))),
            Component::Prefix(_) => Some(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unix private data directory has an unsupported path prefix",
            ))),
        })
        .collect::<io::Result<Vec<_>>>()?;
    if components.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "private data directory {} must be below its filesystem anchor",
                path.display()
            ),
        ));
    }

    let mut display_path = anchor_path.to_path_buf();
    let mut final_created = false;
    for (index, name) in components.iter().enumerate() {
        let parent = &directories.last().expect("anchor is present").1;
        let (directory, created) = open_or_create_directory_at(parent, name)?;
        if created {
            directory.set_permissions(fs::Permissions::from_mode(PRIVATE_DATA_DIR_MODE))?;
        }
        display_path.push(name);
        if index + 1 == components.len() {
            final_created = created;
        }
        directories.push((display_path.clone(), directory));
    }
    Ok((directories, final_created))
}

fn open_or_create_directory_at(parent: &File, name: &OsStr) -> io::Result<(File, bool)> {
    match open_directory_at(parent, name) {
        Ok(directory) => Ok((directory, false)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let name = c_path_component(name)?;
            // SAFETY: `parent` and `name` remain valid for the call.
            let result = unsafe {
                libc::mkdirat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    PRIVATE_DATA_DIR_MODE as libc::mode_t,
                )
            };
            let created = if result == 0 {
                true
            } else {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::AlreadyExists {
                    return Err(error);
                }
                false
            };
            open_directory_at_cstr(parent, &name).map(|directory| (directory, created))
        }
        Err(error) => Err(error),
    }
}

fn open_directory_nofollow(path: &Path) -> io::Result<File> {
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "private data directory path contains NUL",
        )
    })?;
    // SAFETY: `path` remains valid for the call. A successful call returns one
    // owned descriptor, transferred immediately to `File`.
    let descriptor = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    owned_file_from_descriptor(descriptor)
}

fn open_directory_at(parent: &File, name: &OsStr) -> io::Result<File> {
    let name = c_path_component(name)?;
    open_directory_at_cstr(parent, &name)
}

fn open_directory_at_cstr(parent: &File, name: &CString) -> io::Result<File> {
    // SAFETY: `parent` and `name` remain valid for the call. A successful call
    // returns one owned descriptor, transferred immediately to `File`.
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    owned_file_from_descriptor(descriptor)
}

fn c_path_component(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "private data directory component contains NUL",
        )
    })
}

fn owned_file_from_descriptor(descriptor: libc::c_int) -> io::Result<File> {
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `descriptor` is a new owned descriptor returned by `open` or
    // `openat`.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn enforce_private_data_dir(path: &Path, directory: &File, existed: bool) -> io::Result<()> {
    let metadata = directory.metadata()?;
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
        directory.set_permissions(fs::Permissions::from_mode(PRIVATE_DATA_DIR_MODE))?;
    }

    let final_mode = directory.metadata()?.permissions().mode() & 0o777;
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
    fn missing_data_dir_syncs_every_new_directory_and_existing_parent() {
        let tmp = test_util::tempdir();
        let parent = tmp.path().join("managed");
        let data_dir = parent.join("node");
        let mut synced = Vec::new();

        prepare_private_data_dir_with_sync(&data_dir, |path, _| {
            synced.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();

        let expected = data_dir
            .ancestors()
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        assert_eq!(synced, expected);
    }

    #[test]
    fn retry_replays_complete_durability_proof_after_parent_sync_failure() {
        let tmp = test_util::tempdir();
        let data_dir = tmp.path().join("node");
        let mut first_attempt = Vec::new();

        let error = prepare_private_data_dir_with_sync(&data_dir, |path, _| {
            first_attempt.push(path.to_path_buf());
            if path == tmp.path() {
                return Err(io::Error::other("injected parent sync failure"));
            }
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(data_dir.is_dir());
        assert_eq!(first_attempt.last().map(PathBuf::as_path), Some(tmp.path()));

        let mut retry = Vec::new();
        prepare_private_data_dir_with_sync(&data_dir, |path, _| {
            retry.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();

        let expected = data_dir
            .ancestors()
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        assert_eq!(retry, expected);
    }

    #[test]
    fn symlinked_path_component_is_rejected_without_creating_target() {
        use std::os::unix::fs::symlink;

        let tmp = test_util::tempdir();
        let real_parent = tmp.path().join("real");
        fs::create_dir(&real_parent).unwrap();

        let final_link = tmp.path().join("final-link");
        symlink(&real_parent, &final_link).unwrap();
        let final_error = prepare_private_data_dir(&final_link).unwrap_err();
        assert!(matches!(
            final_error.raw_os_error(),
            Some(libc::ELOOP | libc::ENOTDIR)
        ));

        let link = tmp.path().join("link");
        symlink(&real_parent, &link).unwrap();
        let data_dir = link.join("node");

        let error = prepare_private_data_dir(&data_dir).unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::ELOOP | libc::ENOTDIR)
        ));
        assert!(!real_parent.join("node").exists());
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
