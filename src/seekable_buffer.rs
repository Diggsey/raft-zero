use std::cmp::Ordering;
use std::collections::{vec_deque, VecDeque};
use std::ops::Range;
use std::{cmp, mem};

pub struct SeekableBuffer<T> {
    items: VecDeque<T>,
    pos: u64,
    max_len: usize,
}

pub enum SeekDir<'a, T> {
    Still,
    Forward(vec_deque::Drain<'a, T>),
    Backward(Gap<'a, T>),
    Far(&'a mut SeekableBuffer<T>),
}

pub struct Gap<'a, T> {
    buffer: &'a mut SeekableBuffer<T>,
    end_pos: u64,
    append_items: VecDeque<T>,
}

impl<'a, T> Gap<'a, T> {
    pub fn range(&self) -> Range<u64> {
        self.buffer.pos..self.end_pos
    }
    pub fn fill(&mut self, pos: u64, items: impl IntoIterator<Item = T>) {
        self.buffer.fill(pos, items);
        if self.buffer.pos >= self.end_pos {
            let skip = (self.buffer.pos - self.end_pos) as usize;
            self.buffer.items.extend(self.append_items.drain(skip..))
        }
    }
}

impl<T> SeekableBuffer<T> {
    pub fn new(max_len: usize) -> Self {
        Self {
            items: VecDeque::new(),
            pos: 0,
            max_len,
        }
    }
    pub fn fill(&mut self, pos: u64, items: impl IntoIterator<Item = T>) -> bool {
        if pos == self.end_pos() {
            self.items.extend(items);
            true
        } else {
            false
        }
    }
    pub fn seek(&mut self, pos: u64) -> SeekDir<T> {
        match pos.cmp(&self.pos) {
            Ordering::Equal => SeekDir::Still,
            Ordering::Less => {
                // Seeking backwards, we're leaving a gap
                let gap_len = self.pos - pos;
                self.pos = pos;

                // Check if gap is small enough to keep some of our buffer around
                if gap_len < self.max_len as u64 {
                    let keep_len = self.max_len - gap_len as usize;
                    let mut append_items = mem::replace(&mut self.items, VecDeque::new());
                    append_items.truncate(keep_len);
                    SeekDir::Backward(Gap {
                        buffer: self,
                        end_pos: pos + gap_len,
                        append_items,
                    })
                } else {
                    SeekDir::Far(self)
                }
            }
            Ordering::Greater => {
                // Seeking forwards, remove items from the front of our buffer
                let len = pos - self.pos;
                self.pos = pos;
                if len < self.items.len() as u64 {
                    SeekDir::Forward(self.items.drain(0..len as usize))
                } else {
                    self.items.clear();
                    SeekDir::Far(self)
                }
            }
        }
    }
    pub fn pos(&self) -> u64 {
        self.pos
    }
    pub fn end_pos(&self) -> u64 {
        self.pos + self.items.len() as u64
    }
    pub fn calc_fill_range(&self, desired_len: usize, limit: u64) -> Range<u64> {
        let from = self.end_pos();
        if self.items.len() < desired_len {
            let to = cmp::min(self.pos + desired_len as u64, limit);
            from..to
        } else {
            from..from
        }
    }
    pub fn iter(&self) -> vec_deque::Iter<T> {
        self.items.iter()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}
