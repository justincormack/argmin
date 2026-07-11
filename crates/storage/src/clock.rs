use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

thread_local! {
    static TIME_OVERRIDE_MILLIS: Cell<Option<u64>> = const { Cell::new(None) };
}

struct TimeOverrideReset<'a> {
    slot: &'a Cell<Option<u64>>,
    previous: Option<u64>,
}

impl Drop for TimeOverrideReset<'_> {
    fn drop(&mut self) {
        self.slot.set(self.previous);
    }
}

pub fn with_time_override<T>(now_millis: u64, f: impl FnOnce() -> T) -> T {
    TIME_OVERRIDE_MILLIS.with(|slot| {
        let previous = slot.replace(Some(now_millis));
        let _reset = TimeOverrideReset { slot, previous };
        f()
    })
}

#[cfg(test)]
pub struct TestTimeOverrideGuard {
    previous: Option<u64>,
}

#[cfg(test)]
impl TestTimeOverrideGuard {
    pub fn set(&self, now_millis: u64) {
        TIME_OVERRIDE_MILLIS.with(|slot| slot.set(Some(now_millis)));
    }
}

#[cfg(test)]
impl Drop for TestTimeOverrideGuard {
    fn drop(&mut self) {
        TIME_OVERRIDE_MILLIS.with(|slot| slot.set(self.previous));
    }
}

#[cfg(test)]
pub fn test_time_override_guard(now_millis: u64) -> TestTimeOverrideGuard {
    TIME_OVERRIDE_MILLIS.with(|slot| TestTimeOverrideGuard {
        previous: slot.replace(Some(now_millis)),
    })
}

pub fn override_time_millis() -> Option<u64> {
    TIME_OVERRIDE_MILLIS.with(Cell::get)
}

pub fn wall_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn current_time_millis() -> u64 {
    override_time_millis().unwrap_or_else(wall_time_millis)
}

pub fn monotonic_time_millis() -> u64 {
    if let Some(now_millis) = override_time_millis() {
        return now_millis;
    }
    lease_time_millis().unwrap_or(u64::MAX)
}

/// Monotonic sample used to distinguish wall-clock steps from normal elapsed time.
///
/// Apple keeps this separate from the raw, suspend-inclusive lease clock so
/// normal frequency correction does not accumulate as false clock drift.
pub fn clock_health_time_millis() -> Option<u64> {
    if let Some(now_millis) = override_time_millis() {
        return Some(now_millis);
    }
    clock_health_time_millis_inner()
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "openbsd"))]
const LEASE_CLOCK_ID: libc::clockid_t = libc::CLOCK_BOOTTIME;

// Apple documents CLOCK_MONOTONIC_RAW as the clock_gettime equivalent of
// mach_continuous_time, which advances while the machine is asleep.
#[cfg(any(target_os = "macos", target_os = "ios"))]
const LEASE_CLOCK_ID: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;

// FreeBSD documents CLOCK_MONOTONIC as including suspend while CLOCK_UPTIME
// excludes it.
#[cfg(target_os = "freebsd")]
const LEASE_CLOCK_ID: libc::clockid_t = libc::CLOCK_MONOTONIC;

// NetBSD and DragonFly retain Unix support with CLOCK_MONOTONIC. Their
// production qualification must prove suspend semantics; admission-time clock
// health validation otherwise detects a pause and fails bounded leases closed.
#[cfg(any(target_os = "netbsd", target_os = "dragonfly"))]
const LEASE_CLOCK_ID: libc::clockid_t = libc::CLOCK_MONOTONIC;

// Preserve compilation on other Unix targets. Their platform qualification
// must prove CLOCK_MONOTONIC suspend semantics before production use.
#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "openbsd",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))
))]
const LEASE_CLOCK_ID: libc::clockid_t = libc::CLOCK_MONOTONIC;

#[cfg(any(target_os = "macos", target_os = "ios"))]
const CLOCK_HEALTH_CLOCK_ID: libc::clockid_t = libc::CLOCK_MONOTONIC;

#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
const CLOCK_HEALTH_CLOCK_ID: libc::clockid_t = LEASE_CLOCK_ID;

#[cfg(unix)]
fn clock_time_millis(clock_id: libc::clockid_t) -> Option<u64> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: timestamp points to writable storage for a libc timespec and the
    // selected clock id is a platform constant for the current Unix target.
    if unsafe { libc::clock_gettime(clock_id, &raw mut timestamp) } != 0 {
        return None;
    }
    let seconds = u64::try_from(timestamp.tv_sec).ok()?;
    let nanoseconds = u64::try_from(timestamp.tv_nsec).ok()?;
    seconds
        .checked_mul(1_000)?
        .checked_add(nanoseconds / 1_000_000)
}

#[cfg(unix)]
fn lease_time_millis() -> Option<u64> {
    clock_time_millis(LEASE_CLOCK_ID)
}

#[cfg(unix)]
fn clock_health_time_millis_inner() -> Option<u64> {
    clock_time_millis(CLOCK_HEALTH_CLOCK_ID)
}

#[cfg(not(unix))]
fn lease_time_millis() -> Option<u64> {
    None
}

#[cfg(not(unix))]
fn clock_health_time_millis_inner() -> Option<u64> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::{clock_health_time_millis_inner, lease_time_millis};

    #[test]
    fn platform_lease_clock_is_available_and_monotonic() {
        let first = lease_time_millis().expect("the platform lease clock should be available");
        let second = lease_time_millis().expect("the platform lease clock should remain available");
        assert!(second >= first);
    }

    #[test]
    fn platform_clock_health_source_is_available_and_monotonic() {
        let first = clock_health_time_millis_inner()
            .expect("the platform clock-health source should be available");
        let second = clock_health_time_millis_inner()
            .expect("the platform clock-health source should remain available");
        assert!(second >= first);
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[test]
    fn apple_separates_raw_lease_time_from_adjusted_clock_health_time() {
        assert_eq!(super::LEASE_CLOCK_ID, libc::CLOCK_MONOTONIC_RAW);
        assert_eq!(super::CLOCK_HEALTH_CLOCK_ID, libc::CLOCK_MONOTONIC);
    }
}

pub fn current_time_secs() -> u64 {
    override_time_millis().map_or_else(
        || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        },
        |millis| millis / 1000,
    )
}
