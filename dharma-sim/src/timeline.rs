use crate::scheduler::SimScheduler;
use crate::{ClockFaultConfig, FsFaultConfig};
use crate::net::{FaultConfig, NodeId};

#[derive(Clone, Debug)]
pub enum FaultEvent {
    Net(FaultConfig),
    Partition { a: NodeId, b: NodeId, blocked: bool },
    Online { node: NodeId, online: bool },
    Fs { node: NodeId, config: FsFaultConfig },
    Clock { node: NodeId, config: ClockFaultConfig },
}

pub struct FaultTimeline {
    scheduler: SimScheduler<FaultEvent>,
    next_order: u64,
}

impl FaultTimeline {
    pub fn new() -> Self {
        Self {
            scheduler: SimScheduler::new(),
            next_order: 0,
        }
    }

    pub fn schedule_net(&mut self, time: i64, config: FaultConfig) {
        self.schedule(time, FaultEvent::Net(config));
    }

    pub fn schedule_partition(&mut self, time: i64, a: NodeId, b: NodeId, blocked: bool) {
        self.schedule(time, FaultEvent::Partition { a, b, blocked });
    }

    pub fn schedule_online(&mut self, time: i64, node: NodeId, online: bool) {
        self.schedule(time, FaultEvent::Online { node, online });
    }

    pub fn schedule_fs(&mut self, time: i64, node: NodeId, config: FsFaultConfig) {
        self.schedule(time, FaultEvent::Fs { node, config });
    }

    pub fn schedule_clock(&mut self, time: i64, node: NodeId, config: ClockFaultConfig) {
        self.schedule(time, FaultEvent::Clock { node, config });
    }

    pub fn drain_ready(&mut self, now: i64) -> Vec<FaultEvent> {
        let mut out = Vec::new();
        loop {
            match self.scheduler.peek_time() {
                Some(time) if time <= now => {
                    if let Some(event) = self.scheduler.next() {
                        out.push(event.event);
                    }
                }
                _ => break,
            }
        }
        out
    }

    fn schedule(&mut self, time: i64, event: FaultEvent) {
        let order = self.next_order;
        self.next_order = self.next_order.wrapping_add(1);
        self.scheduler.schedule(time, order, event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_timeline_orders_by_time() {
        let mut timeline = FaultTimeline::new();
        timeline.schedule_online(5, 2, false);
        timeline.schedule_net(
            1,
            FaultConfig {
                drop_rate: 0.2,
                dup_rate: 0.0,
                min_delay: 0,
                max_delay: 1,
            },
        );
        let events = timeline.drain_ready(1);
        assert_eq!(events.len(), 1);
        match events[0] {
            FaultEvent::Net(_) => {}
            _ => panic!("expected net event"),
        }
        let events = timeline.drain_ready(5);
        assert_eq!(events.len(), 1);
        match events[0] {
            FaultEvent::Online { node, online } => {
                assert_eq!(node, 2);
                assert!(!online);
            }
            _ => panic!("expected online event"),
        }
    }
}
