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
