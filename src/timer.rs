#[derive(PartialEq, Eq, Debug, Clone, Default)]
pub struct TimerToken(u64);

impl TimerToken {
    pub fn inc(&mut self) -> TimerToken {
        self.0 += 1;
        self.clone()
    }
}
