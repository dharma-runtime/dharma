use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Clone, Debug)]
pub struct ScheduledEvent<E> {
    pub time: i64,
    pub order: u64,
    pub event: E,
}

impl<E> PartialEq for ScheduledEvent<E> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.order == other.order
    }
}

impl<E> Eq for ScheduledEvent<E> {}

impl<E> PartialOrd for ScheduledEvent<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Ord for ScheduledEvent<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .time
            .cmp(&self.time)
            .then_with(|| other.order.cmp(&self.order))
    }
}

#[derive(Clone, Debug)]
pub struct SimScheduler<E> {
    now: i64,
    queue: BinaryHeap<ScheduledEvent<E>>,
}

impl<E> SimScheduler<E> {
    pub fn new() -> Self {
        Self {
            now: 0,
            queue: BinaryHeap::new(),
        }
    }

    pub fn now(&self) -> i64 {
        self.now
    }

    pub fn schedule(&mut self, time: i64, order: u64, event: E) {
        self.queue.push(ScheduledEvent {
            time,
            order,
            event,
        });
    }

    pub fn next(&mut self) -> Option<ScheduledEvent<E>> {
        let ev = self.queue.pop()?;
        self.now = ev.time;
        Some(ev)
    }

    pub fn peek_time(&self) -> Option<i64> {
        self.queue.peek().map(|ev| ev.time)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn any<F>(&self, mut f: F) -> bool
    where
        F: FnMut(&ScheduledEvent<E>) -> bool,
    {
        self.queue.iter().any(|ev| f(ev))
    }
}
