// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::cell::Cell;
#[cfg(any(test, feature = "test-hooks"))]
use std::marker::PhantomData;
#[cfg(any(test, feature = "test-hooks"))]
use std::rc::Rc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{Arc, Weak};
#[cfg(any(test, feature = "test-hooks"))]
use std::thread::{self, ThreadId};
use std::time::{SystemTime, UNIX_EPOCH};

const CLOCK_HEALTH_SAMPLE_MAX_WINDOW_MS: u64 = 0;
const CLOCK_HEALTH_SAMPLE_ATTEMPTS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WallClockHealthSample {
    wall_time_ms: u64,
    health_time_ms: Option<u64>,
}

impl WallClockHealthSample {
    #[must_use]
    pub(crate) fn wall_time_ms(self) -> u64 {
        self.wall_time_ms
    }

    #[must_use]
    pub(crate) fn health_time_ms(self) -> Option<u64> {
        self.health_time_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WallClockHealthSampleWindowTooWide {
    narrowest_window_ms: u64,
    max_window_ms: u64,
}

impl WallClockHealthSampleWindowTooWide {
    #[must_use]
    pub(crate) fn narrowest_window_ms(self) -> u64 {
        self.narrowest_window_ms
    }

    #[must_use]
    pub(crate) fn max_window_ms(self) -> u64 {
        self.max_window_ms
    }
}

thread_local! {
    static TIME_OVERRIDE_MILLIS: Cell<Option<u64>> = const { Cell::new(None) };
    static MONOTONIC_TIME_OVERRIDE_MILLIS: Cell<Option<u64>> = const { Cell::new(None) };
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

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn with_time_and_monotonic_override<T>(
    wall_time_millis: u64,
    monotonic_time_millis: u64,
    f: impl FnOnce() -> T,
) -> T {
    TIME_OVERRIDE_MILLIS.with(|wall_slot| {
        MONOTONIC_TIME_OVERRIDE_MILLIS.with(|monotonic_slot| {
            let previous_wall = wall_slot.replace(Some(wall_time_millis));
            let previous_monotonic = monotonic_slot.replace(Some(monotonic_time_millis));
            let _wall_reset = TimeOverrideReset {
                slot: wall_slot,
                previous: previous_wall,
            };
            let _monotonic_reset = TimeOverrideReset {
                slot: monotonic_slot,
                previous: previous_monotonic,
            };
            f()
        })
    })
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone)]
pub(crate) struct TestTimeOverrideControl {
    origin_thread: ThreadId,
    guard_liveness: Weak<()>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl TestTimeOverrideControl {
    pub(crate) fn set(&self, now_millis: u64) {
        assert!(
            self.guard_liveness.upgrade().is_some(),
            "test clock override control used after its guard was dropped"
        );
        assert_eq!(
            thread::current().id(),
            self.origin_thread,
            "test clock override control used from a thread other than its origin"
        );
        TIME_OVERRIDE_MILLIS.with(|slot| slot.set(Some(now_millis)));
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct TestTimeOverrideGuard {
    previous: Option<u64>,
    control: TestTimeOverrideControl,
    _guard_liveness: Arc<()>,
    not_send_or_sync: PhantomData<Rc<()>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl TestTimeOverrideGuard {
    pub(crate) fn set(&self, now_millis: u64) {
        self.control.set(now_millis);
    }

    pub(crate) fn control(&self) -> TestTimeOverrideControl {
        self.control.clone()
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for TestTimeOverrideGuard {
    fn drop(&mut self) {
        assert_eq!(
            thread::current().id(),
            self.control.origin_thread,
            "test clock override guard dropped from a thread other than its origin"
        );
        TIME_OVERRIDE_MILLIS.with(|slot| slot.set(self.previous));
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_time_override_guard(now_millis: u64) -> TestTimeOverrideGuard {
    TIME_OVERRIDE_MILLIS.with(|slot| {
        let guard_liveness = Arc::new(());
        TestTimeOverrideGuard {
            previous: slot.replace(Some(now_millis)),
            control: TestTimeOverrideControl {
                origin_thread: thread::current().id(),
                guard_liveness: Arc::downgrade(&guard_liveness),
            },
            _guard_liveness: guard_liveness,
            not_send_or_sync: PhantomData,
        }
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
    try_monotonic_time_millis().unwrap_or(u64::MAX)
}

pub(crate) fn try_monotonic_time_millis() -> Option<u64> {
    if let Some(now_millis) = MONOTONIC_TIME_OVERRIDE_MILLIS.with(Cell::get) {
        return Some(now_millis);
    }
    if let Some(now_millis) = override_time_millis() {
        return Some(now_millis);
    }
    lease_time_millis()
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

/// Sample wall and health clocks without mistaking scheduler delay for drift.
pub(crate) fn wall_clock_health_sample(
) -> Result<WallClockHealthSample, WallClockHealthSampleWindowTooWide> {
    wall_clock_health_sample_with(current_time_millis, clock_health_time_millis)
}

fn wall_clock_health_sample_with(
    mut wall_time: impl FnMut() -> u64,
    mut health_time: impl FnMut() -> Option<u64>,
) -> Result<WallClockHealthSample, WallClockHealthSampleWindowTooWide> {
    let mut narrowest_window_ms = u64::MAX;
    let mut greatest_health_ms = None;
    for _ in 0..CLOCK_HEALTH_SAMPLE_ATTEMPTS {
        let Some(health_before_ms) = health_time() else {
            return Ok(WallClockHealthSample {
                wall_time_ms: wall_time(),
                health_time_ms: None,
            });
        };
        if greatest_health_ms.is_some_and(|greatest| health_before_ms < greatest) {
            return Ok(WallClockHealthSample {
                wall_time_ms: wall_time(),
                health_time_ms: None,
            });
        }
        greatest_health_ms = Some(health_before_ms);
        let wall_time_ms = wall_time();
        let Some(health_after_ms) = health_time() else {
            return Ok(WallClockHealthSample {
                wall_time_ms,
                health_time_ms: None,
            });
        };
        if health_after_ms < greatest_health_ms.expect("the before sample established a maximum") {
            return Ok(WallClockHealthSample {
                wall_time_ms,
                health_time_ms: None,
            });
        }
        greatest_health_ms = Some(health_after_ms);
        let Some(window_ms) = health_after_ms.checked_sub(health_before_ms) else {
            return Ok(WallClockHealthSample {
                wall_time_ms,
                health_time_ms: None,
            });
        };
        if window_ms == CLOCK_HEALTH_SAMPLE_MAX_WINDOW_MS {
            return Ok(WallClockHealthSample {
                wall_time_ms,
                health_time_ms: Some(health_before_ms + window_ms / 2),
            });
        }
        narrowest_window_ms = narrowest_window_ms.min(window_ms);
    }
    Err(WallClockHealthSampleWindowTooWide {
        narrowest_window_ms,
        max_window_ms: CLOCK_HEALTH_SAMPLE_MAX_WINDOW_MS,
    })
}

#[cfg(test)]
pub(crate) fn test_wall_clock_health_sample_with(
    wall_time: impl FnMut() -> u64,
    health_time: impl FnMut() -> Option<u64>,
) -> Result<WallClockHealthSample, WallClockHealthSampleWindowTooWide> {
    wall_clock_health_sample_with(wall_time, health_time)
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
    use std::cell::Cell;

    use super::{
        clock_health_time_millis_inner, lease_time_millis, wall_clock_health_sample_with,
        CLOCK_HEALTH_SAMPLE_ATTEMPTS,
    };

    #[test]
    fn wall_clock_health_sample_retries_scheduler_delay() {
        let mut walls = [1_000, 2_001].into_iter();
        let mut health = [0, 1_001, 2_000, 2_000].into_iter();

        let sample = wall_clock_health_sample_with(
            || walls.next().expect("each sampling attempt needs wall time"),
            || {
                Some(
                    health
                        .next()
                        .expect("each sampling attempt needs two health times"),
                )
            },
        )
        .expect("the narrow retry should be accepted");

        assert_eq!(sample.wall_time_ms(), 2_001);
        assert_eq!(sample.health_time_ms(), Some(2_000));
    }

    #[test]
    fn wall_clock_health_sample_rejects_persistently_wide_windows() {
        let health = Cell::new(0u64);
        let error = wall_clock_health_sample_with(
            || 1_000,
            || {
                let value = health.get();
                health.set(value + 20);
                Some(value)
            },
        )
        .expect_err("every sample window should exceed the precision bound");

        assert_eq!(health.get(), 2 * 20 * CLOCK_HEALTH_SAMPLE_ATTEMPTS as u64);
        assert_eq!(error.narrowest_window_ms(), 20);
        assert_eq!(error.max_window_ms(), 0);
    }

    #[test]
    fn wall_clock_health_sample_rejects_rollback_between_attempts() {
        let mut walls = [9_000, 10_002].into_iter();
        let mut health = [0, 1_002, 502].into_iter();

        let sample = wall_clock_health_sample_with(
            || walls.next().expect("each attempted sample needs wall time"),
            || {
                Some(
                    health
                        .next()
                        .expect("the attempted samples need health times"),
                )
            },
        )
        .expect("clock rollback produces an unusable sample");

        assert_eq!(sample.wall_time_ms(), 10_002);
        assert_eq!(sample.health_time_ms(), None);
    }

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
