#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeFailpoints {
    recovery_queue_saturated: bool,
}

impl RuntimeFailpoints {
    pub const fn new() -> Self {
        Self {
            recovery_queue_saturated: false,
        }
    }

    pub fn set_recovery_queue_saturated(&mut self, saturated: bool) {
        self.recovery_queue_saturated = saturated;
    }

    pub const fn recovery_queue_saturated(&self) -> bool {
        self.recovery_queue_saturated
    }
}
