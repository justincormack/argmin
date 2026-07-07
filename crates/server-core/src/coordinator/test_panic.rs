use std::cell::Cell;
use std::sync::Once;

thread_local! {
    static SUPPRESS_EXPECTED_TEST_PANIC: Cell<bool> = const { Cell::new(false) };
}

static EXPECTED_TEST_PANIC_HOOK: Once = Once::new();

pub(crate) struct SuppressExpectedTestPanic {
    previous_suppressed: bool,
}

impl SuppressExpectedTestPanic {
    pub(crate) fn enter() -> Self {
        EXPECTED_TEST_PANIC_HOOK.call_once(|| {
            let previous_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                if SUPPRESS_EXPECTED_TEST_PANIC.with(Cell::get) {
                    return;
                }
                previous_hook(panic_info);
            }));
        });
        let previous_suppressed = SUPPRESS_EXPECTED_TEST_PANIC.with(|suppressed| {
            let previous = suppressed.get();
            suppressed.set(true);
            previous
        });
        Self {
            previous_suppressed,
        }
    }
}

impl Drop for SuppressExpectedTestPanic {
    fn drop(&mut self) {
        SUPPRESS_EXPECTED_TEST_PANIC.with(|suppressed| suppressed.set(self.previous_suppressed));
    }
}
