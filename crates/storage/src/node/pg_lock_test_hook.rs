// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local scheduling at an actually contended PG lock, not thread start.

use super::PgStore;
use std::cell::RefCell;
use std::sync::{Mutex, MutexGuard, TryLockError, TryLockResult};

type Acquired = Box<dyn FnOnce(u32, &PgStore)>;

struct Hook {
    before: Box<dyn FnOnce(u32)>,
    after: Acquired,
}

thread_local! {
    static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

pub(super) fn lock_pg(mutex: &Mutex<PgStore>, pg_id: u32) -> MutexGuard<'_, PgStore> {
    finish_acquisition(pg_id, mutex.try_lock(), || {
        mutex.lock().unwrap_or_else(|error| error.into_inner())
    })
}

fn finish_acquisition<'a>(
    pg_id: u32,
    attempted: TryLockResult<MutexGuard<'a, PgStore>>,
    wait: impl FnOnce() -> MutexGuard<'a, PgStore>,
) -> MutexGuard<'a, PgStore> {
    match attempted {
        // These guards already own the lock. Never release and reacquire them:
        // doing so would introduce an unobserved contention gap into test builds.
        Ok(guard) => guard,
        Err(TryLockError::Poisoned(error)) => error.into_inner(),
        Err(TryLockError::WouldBlock) => {
            let hook = HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                (hook.before)(pg_id);
                let guard = wait();
                (hook.after)(pg_id, &guard);
                guard
            } else {
                wait()
            }
        }
    }
}

pub(in crate::node_runtime) fn with_contention<T>(
    before: impl FnOnce(u32) + 'static,
    after: impl FnOnce(u32, &PgStore) + 'static,
    action: impl FnOnce() -> T,
) -> T {
    struct Reset(Option<Hook>);
    impl Drop for Reset {
        fn drop(&mut self) {
            HOOK.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _reset = Reset(HOOK.with(|slot| {
        slot.replace(Some(Hook {
            before: Box::new(before),
            after: Box::new(after),
        }))
    }));
    action()
}

#[test]
fn pg_lock_probe_returns_acquired_guard_without_relocking() {
    for poisoned in [false, true] {
        let tmp = test_util::tempdir();
        let mutex = Mutex::new(PgStore::open(tmp.path(), 0).unwrap());
        if poisoned {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = mutex.lock().unwrap();
                panic!("poison PG lock for acquisition regression");
            }));
            assert!(panic.is_err());
        }
        let attempted = mutex.try_lock();
        assert_eq!(
            matches!(&attempted, Err(TryLockError::Poisoned(_))),
            poisoned
        );
        assert!(!matches!(&attempted, Err(TryLockError::WouldBlock)));
        let guard = with_contention(
            |_| panic!("acquired lock is not contended"),
            |_, _| panic!("acquired lock must not take the waiting path"),
            || {
                let guard = finish_acquisition(0, attempted, || {
                    panic!("must return the acquired guard, not release and relock it")
                });
                assert!(HOOK.with(|slot| slot.borrow().is_some()));
                guard
            },
        );
        assert!(matches!(mutex.try_lock(), Err(TryLockError::WouldBlock)));
        drop(guard);
        assert!(!matches!(mutex.try_lock(), Err(TryLockError::WouldBlock)));
    }
}
