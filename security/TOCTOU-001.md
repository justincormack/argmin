---
id: TOCTOU-001
bug_class: toctou
title: Data-dir privacy enforced with path-based metadata check + set_permissions (no O_NOFOLLOW / fd recheck)
location: crates/storage/src/data_dir.rs:39
function: enforce_private_data_dir
confidence: Medium
worker: worker-21
fp_verdict: FALSE_POSITIVE
fp_rationale: "The path-based check/chmod race exists mechanically, but exploiting it requires an untrusted user to be able to replace the configured data-directory path through its parent. That deployment violates the documented filesystem trust assumption, while static-manifest production validation already rejects symlinked, wrong-owner, and group/other-writable path components."
severity: NONE
attack_vector: Local
exploitability: Not applicable
severity_rationale: "No supported deployment permits an unprivileged attacker to replace the data-directory pathname or its trusted ancestors. A legacy environment-configured path beneath an attacker-replaceable parent is an insecure deployment configuration outside the promised boundary, and the process also refuses root operation."
status: invalid
---

## Description
`prepare_private_data_dir` / `enforce_private_data_dir` enforce that the storage
data directory (`ARGMIN_DATA_DIR`, resolved from environment configuration) is a
private, non-world-writable directory. The enforcement is done entirely with
**path-based** syscalls that follow symlinks and are not anchored to a single
open file descriptor:

1. `fs::metadata(path)` — the *check*. Follows symlinks; used both to reject a
   world-writable existing directory and to decide whether a `chmod` is needed.
2. `fs::set_permissions(path, 0o700)` — a later, separate *use*. Also follows
   symlinks.
3. `fs::metadata(path)` again for the final confirmation.

The security decision (reject world-writable, then force mode `0700`) rides on
the result of step 1, but steps 1→2→3 are three independent `stat`/`chmod`
syscalls on a *name*, with no `O_NOFOLLOW`, no open-once-then-`fstat`, and no
`dev`/`ino` identity check binding the check to the use. This is precisely the
check→use window the storage-node code elsewhere in this crate is careful to
close (see `StorageNodeDataDirLock::acquire` and the identity/lock helpers in
`static_cluster_state.rs`, which all use `O_NOFOLLOW | O_CLOEXEC`, `create_new`,
`fstat` on the returned fd, and `dev`/`ino` comparison). `data_dir.rs` is the one
filesystem-privacy path that does not follow that hardened pattern.

An attacker who can write to the *parent* directory of the configured data dir
(the LOCAL_UNPRIVILEGED "TOCTOU on the data dir / symlink attacks" boundary
called out in the review context) can substitute the data-dir name with a
symlink between the `metadata` check and the `set_permissions` use: the check can
be satisfied against a benign target, then swapped so the `chmod 0700` (and the
directory the server then populates with SQLite metadata and shard files) refers
to a directory of the attacker's choosing. Because the world-writable rejection
in step 1 is what protects against an attacker-provisioned data dir, defeating
that check via the race removes the protection.

## Code
```rust
pub(crate) fn prepare_private_data_dir(path: &Path) -> io::Result<()> {
    let existed = path.try_exists()?;                 // follows symlinks
    DirBuilder::new().recursive(true).mode(PRIVATE_DATA_DIR_MODE).create(path)?;
    enforce_private_data_dir(path, existed)
}

fn enforce_private_data_dir(path: &Path, existed: bool) -> io::Result<()> {
    let metadata = fs::metadata(path)?;               // CHECK (follows symlinks)
    if !metadata.is_dir() { /* ... */ }
    let mode = metadata.permissions().mode() & 0o777;
    if existed && mode & WORLD_WRITE != 0 {           // security decision on CHECK
        return Err(/* world-writable */);
    }
    if mode != PRIVATE_DATA_DIR_MODE {
        fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DATA_DIR_MODE))?; // USE
    }
    let final_mode = fs::metadata(path)?.permissions().mode() & 0o777; // re-CHECK (still path-based)
    if final_mode != PRIVATE_DATA_DIR_MODE { return Err(/* ... */); }
    Ok(())
}
```

## Data flow
- **Source:** `ARGMIN_DATA_DIR` (environment configuration) and the on-disk state
  of that path / its parent directory, both attacker-influenced under
  LOCAL_UNPRIVILEGED per the review context.
- **Sink:** `fs::set_permissions(path, 0o700)` at `crates/storage/src/data_dir.rs:39`
  and the subsequent directory population by callers
  (`StorageNodeDataDirLock::acquire`, `node.rs:326/712`, `cluster/local.rs:4947`).
- **Validation:** world-writable rejection and mode enforcement, but performed
  with symlink-following `fs::metadata`/`fs::set_permissions` and re-`stat` by
  name — no `O_NOFOLLOW`/fd anchoring, so the validation and the protected
  operation can act on different filesystem objects.

## Reachability trace
`StorageNodeServer::bind → bind_with_rpc_auth → StorageNodeDataDirGuard::acquire
→ StorageNodeDataDirLock::acquire → prepare_private_data_dir → enforce_private_data_dir`
(also reached from `node.rs:326`, `node.rs:712`, `cluster/local.rs:4947`, and
`storage_node_server.rs:724`).

## Impact
Defeats the data-directory privacy/world-writable guard on a config-supplied
path whose parent an unprivileged local user can write to: a symlink swap in the
check→chmod→populate window lets the server tighten permissions on, and then
create metadata/shard state inside, an attacker-selected directory. Because the
service intentionally runs non-root (euid==0 is refused), `chmod` on a symlink
target the process does not own fails with `EPERM`, so arbitrary-victim chmod is
not achievable; the realistic impact is bypassing the world-writable safety check
and redirecting where the server writes its private state, which is a local
integrity/confidentiality concern rather than a remote one. Exploitability is
conditional on the data dir being placed under an attacker-writable parent.

## Mitigations checked
- `O_NOFOLLOW` / `O_CLOEXEC`: **not used** here (contrast: the same crate's
  `StorageNodeDataDirLock::acquire` and `static_cluster_state.rs` helpers use them).
- Open-once + `fstat` on the returned fd: **not used**; all three operations are
  path-based.
- `dev`/`ino` identity binding between check and use: **absent** (present in
  `entry_names_excluding_held_lock`).
- Final `fs::metadata` re-check exists but is itself path-based and does not close
  the window.
- Non-root enforcement (euid!=0) limits `chmod` to process-owned targets,
  reducing but not eliminating impact.

## Recommendation
Open the data directory once with `O_NOFOLLOW | O_DIRECTORY | O_CLOEXEC`, then
perform all checks (`fstat` for mode/`is_dir`/world-writable) and the
`fchmod`-equivalent (`File::set_permissions` on the open handle) against that
single descriptor, mirroring the hardened pattern already used by
`StorageNodeDataDirLock::acquire`. Reject symlinked path components rather than
following them, and bind the world-writable check and the mode enforcement to the
same fd so the two cannot refer to different filesystem objects.

## Validity assessment

This report is invalid as a security finding under the repository's documented
filesystem threat model. The implementation does perform path-based
`metadata`/`set_permissions` operations, so a process able to replace the data
directory's pathname between those operations could make them refer to
different filesystem objects. That mechanical race is not disputed.

The proposed attacker capability is outside the supported boundary. The threat
model assumes that host filesystem permissions are trusted and that
unprivileged users cannot modify `ARGMIN_DATA_DIR`. Replacing the directory
entry through its parent is equivalent authority over the configured durable
state: the same actor can make startup fail persistently, remove the pathname,
or redirect subsequent path-based state access without relying on the narrow
check-to-chmod window.

The production static-manifest path additionally validates every existing
component below the declared mount before startup. Components must not be
symlinks, must remain on the declared device, must be owned by the effective
service user, and must not be writable by group or other users. Consequently,
an ordinary local user cannot perform the reported swap in that deployment.
The remaining legacy environment-configured mode relies on the operator to
place the data directory beneath trusted ancestry, consistent with its local
compatibility purpose.

The report also overstates what its proposed fix would establish. Opening the
final directory once and applying `fstat`/`fchmod` would bind the privacy check,
but later lock creation, PG opening, SQLite access, shard paths, and runtime
configuration persistence still resolve the pathname. Fully defending an
attacker-replaceable parent would require either a trusted-ancestry policy or a
directory capability propagated through the complete storage filesystem API.
That cross-cutting refactor is not justified for an excluded deployment model,
and a partial `O_NOFOLLOW` change would create a misleading security claim.

## Disposition

No code change was made. Deployments must keep the configured data directory
and its ancestors outside untrusted write control. Static-manifest replicated
deployments enforce the stronger ownership, permission, device, and symlink
rules directly. A future descriptor-relative filesystem abstraction may adopt
a retained data-directory capability as general hardening, but it is not a
required security fix for this finding.
