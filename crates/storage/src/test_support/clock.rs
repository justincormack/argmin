/// Opaque mutable wall-clock override for deterministic cross-crate tests.
///
/// The process-local clock slot and its reset guard remain owned by storage.
/// The guard is deliberately neither `Send` nor `Sync`, so it must be dropped
/// on the thread where the override was installed.
pub struct TestClockOverrideGuard {
    inner: crate::clock::TestTimeOverrideGuard,
}

impl TestClockOverrideGuard {
    /// Replace the current thread's overridden wall-clock value.
    pub fn set(&self, now_millis: u64) {
        self.inner.set(now_millis);
    }

    /// Create a transferable controller for callbacks required to be
    /// `Send + Sync` even though they execute synchronously on this thread.
    ///
    /// Using the controller from another thread is rejected.
    #[must_use]
    pub fn control(&self) -> TestClockOverrideControl {
        TestClockOverrideControl {
            inner: self.inner.control(),
        }
    }
}

/// Cloneable controller for a live, thread-bound test clock override.
#[derive(Clone)]
pub struct TestClockOverrideControl {
    inner: crate::clock::TestTimeOverrideControl,
}

impl TestClockOverrideControl {
    /// Replace the originating thread's overridden wall-clock value.
    ///
    /// # Panics
    ///
    /// Panics if invoked after the associated [`TestClockOverrideGuard`] was
    /// dropped or from a thread other than the one which created that guard.
    pub fn set(&self, now_millis: u64) {
        self.inner.set(now_millis);
    }
}

/// Override the current thread's wall clock until the returned guard drops.
pub fn test_time_override_guard(now_millis: u64) -> TestClockOverrideGuard {
    TestClockOverrideGuard {
        inner: crate::clock::test_time_override_guard(now_millis),
    }
}

/// Run one deterministic action with independently selected wall and
/// monotonic clock values.
pub fn with_time_and_monotonic_override<T>(
    wall_time_millis: u64,
    monotonic_time_millis: u64,
    action: impl FnOnce() -> T,
) -> T {
    crate::clock::with_time_and_monotonic_override(wall_time_millis, monotonic_time_millis, action)
}

#[cfg(test)]
mod tests {
    use super::{test_time_override_guard, with_time_and_monotonic_override};

    #[test]
    fn mutable_clock_guard_updates_and_restores_the_thread_clock() {
        crate::clock::with_time_override(111, || {
            let guard = test_time_override_guard(222);
            assert_eq!(crate::clock::current_time_millis(), 222);

            guard.set(333);
            assert_eq!(crate::clock::current_time_millis(), 333);

            drop(guard);
            assert_eq!(crate::clock::current_time_millis(), 111);
        });
    }

    #[test]
    fn coupled_override_sets_and_restores_both_clock_domains() {
        crate::clock::with_time_override(111, || {
            with_time_and_monotonic_override(222, 333, || {
                assert_eq!(crate::clock::current_time_millis(), 222);
                assert_eq!(crate::clock::monotonic_time_millis(), 333);
            });

            assert_eq!(crate::clock::current_time_millis(), 111);
            assert_eq!(crate::clock::monotonic_time_millis(), 111);
        });
    }

    #[test]
    fn transferable_control_rejects_cross_thread_mutation() {
        crate::clock::with_time_override(111, || {
            let guard = test_time_override_guard(222);
            let control = guard.control();
            let cross_thread =
                std::thread::spawn(move || std::panic::catch_unwind(|| control.set(333)))
                    .join()
                    .expect("the worker should capture the rejected clock mutation");

            assert!(cross_thread.is_err());
            assert_eq!(crate::clock::current_time_millis(), 222);
            drop(guard);
            assert_eq!(crate::clock::current_time_millis(), 111);
        });
    }

    #[test]
    fn controller_rejects_mutation_after_guard_drop() {
        crate::clock::with_time_override(111, || {
            let guard = test_time_override_guard(222);
            let control = guard.control();

            drop(guard);
            assert_eq!(crate::clock::current_time_millis(), 111);

            let stale_use = std::panic::catch_unwind(|| control.set(333));
            assert!(stale_use.is_err());
            assert_eq!(crate::clock::current_time_millis(), 111);
        });
    }
}
