use std::ops::{Add, AddAssign, Sub, SubAssign};

use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct NodeId(pub u64);

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Term(pub u64);

impl Term {
    pub fn inc(&mut self) {
        self.0 += 1;
    }
}

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LogIndex(pub u64);

impl LogIndex {
    pub const ZERO: LogIndex = LogIndex(0);
}

impl Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(mut self, other: u64) -> LogIndex {
        self += other;
        self
    }
}

impl AddAssign<u64> for LogIndex {
    fn add_assign(&mut self, other: u64) {
        self.0 += other;
    }
}

impl Sub<LogIndex> for LogIndex {
    type Output = u64;
    fn sub(self, other: LogIndex) -> u64 {
        self.0 - other.0
    }
}

impl Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(mut self, other: u64) -> LogIndex {
        self -= other;
        self
    }
}

impl SubAssign<u64> for LogIndex {
    fn sub_assign(&mut self, other: u64) {
        assert!(self.0 >= other);
        self.0 -= other;
    }
}
