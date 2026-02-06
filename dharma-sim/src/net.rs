use crate::scheduler::SimScheduler;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub type NodeId = u64;

#[derive(Clone, Debug)]
pub struct FaultConfig {
    pub drop_rate: f32,
    pub dup_rate: f32,
    pub min_delay: i64,
    pub max_delay: i64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            drop_rate: 0.0,
            dup_rate: 0.0,
            min_delay: 0,
            max_delay: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum NetEvent {
    Deliver {
        from: NodeId,
        to: NodeId,
        bytes: Vec<u8>,
    },
}

pub struct SimNet {
    scheduler: SimScheduler<NetEvent>,
    seed: u64,
    faults: FaultConfig,
    inbox: HashMap<NodeId, HashMap<NodeId, VecDeque<Vec<u8>>>>,
    partitions: HashSet<(NodeId, NodeId)>,
    next_id: NodeId,
    send_seq: HashMap<NodeId, u64>,
    online: HashSet<NodeId>,
    trace: Vec<TraceLine>,
}

#[derive(Clone, Debug)]
struct TraceLine {
    time: i64,
    order: u64,
    line: String,
}

impl SimNet {
    pub fn new(seed: u64) -> Self {
        Self {
            scheduler: SimScheduler::new(),
            seed,
            faults: FaultConfig::default(),
            inbox: HashMap::new(),
            partitions: HashSet::new(),
            next_id: 1,
            send_seq: HashMap::new(),
            online: HashSet::new(),
            trace: Vec::new(),
        }
    }

    pub fn register_node(&mut self) -> NodeId {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.inbox.entry(id).or_default();
        self.send_seq.entry(id).or_default();
        self.online.insert(id);
        id
    }

    pub fn set_faults(&mut self, faults: FaultConfig) {
        self.faults = faults;
        let time = self.scheduler.now();
        self.record(
            time,
            0,
            format!(
                "faults t={time} drop={} dup={} min_delay={} max_delay={}",
                self.faults.drop_rate,
                self.faults.dup_rate,
                self.faults.min_delay,
                self.faults.max_delay
            ),
        );
    }

    pub fn set_partition(&mut self, a: NodeId, b: NodeId, blocked: bool) {
        if blocked {
            self.partitions.insert((a, b));
            self.partitions.insert((b, a));
        } else {
            self.partitions.remove(&(a, b));
            self.partitions.remove(&(b, a));
        }
        let time = self.scheduler.now();
        let state = if blocked { "blocked" } else { "open" };
        let order = self.order_key(a.min(b), a.max(b));
        self.record(
            time,
            order,
            format!("partition t={time} a={a} b={b} state={state}"),
        );
    }

    pub fn set_online(&mut self, node: NodeId, online: bool) {
        if online {
            self.online.insert(node);
            self.inbox.entry(node).or_default();
        } else {
            self.online.remove(&node);
            if let Some(peers) = self.inbox.get_mut(&node) {
                for queue in peers.values_mut() {
                    queue.clear();
                }
            }
        }
        let time = self.scheduler.now();
        let state = if online { "up" } else { "down" };
        self.record(
            time,
            node,
            format!("online t={time} node={node} state={state}"),
        );
    }

    pub fn now(&self) -> i64 {
        self.scheduler.now()
    }

    pub fn has_pending(&self) -> bool {
        if !self.scheduler.is_empty() {
            return true;
        }
        for peers in self.inbox.values() {
            for queue in peers.values() {
                if !queue.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    pub fn has_pending_between(&self, node: NodeId, peer: NodeId) -> bool {
        if let Some(peers) = self.inbox.get(&node) {
            if let Some(queue) = peers.get(&peer) {
                if !queue.is_empty() {
                    return true;
                }
            }
        }
        self.scheduler.any(|ev| {
            matches!(
                &ev.event,
                NetEvent::Deliver { from, to, .. } if *from == peer && *to == node
            )
        })
    }

    pub fn send(&mut self, from: NodeId, to: NodeId, bytes: Vec<u8>) {
        let now = self.scheduler.now();
        let seq = self.next_send_seq(from);
        let order = self.order_key(from, seq);
        if !self.online.contains(&from) || !self.online.contains(&to) {
            self.record(
                now,
                order,
                format!(
                    "drop t={now} from={from} to={to} len={} reason=offline",
                    bytes.len()
                ),
            );
            return;
        }
        if self.partitions.contains(&(from, to)) {
            self.record(
                now,
                order,
                format!(
                    "drop t={now} from={from} to={to} len={} reason=partition",
                    bytes.len()
                ),
            );
            return;
        }
        if self.should_drop(from, to, seq) {
            self.record(
                now,
                order,
                format!(
                    "drop t={now} from={from} to={to} len={} reason=fault",
                    bytes.len()
                ),
            );
            return;
        }
        let delay = self.compute_delay(from, to, seq);
        let deliver_at = self.scheduler.now().saturating_add(delay);
        let dup = self.should_dup(from, to, seq);
        self.record(
            now,
            order,
            format!(
                "send t={now} from={from} to={to} len={} delay={delay} dup={dup}",
                bytes.len()
            ),
        );
        self.scheduler.schedule(
            deliver_at,
            order,
            NetEvent::Deliver {
                from,
                to,
                bytes: bytes.clone(),
            },
        );
        if dup {
            let dup_delay = self.compute_delay(from, to, seq ^ 0x9e37).saturating_add(1);
            let dup_at = self.scheduler.now().saturating_add(dup_delay);
            let order = self.order_key(from, seq.wrapping_add(1));
            self.scheduler
                .schedule(dup_at, order, NetEvent::Deliver { from, to, bytes });
        }
    }

    pub fn step(&mut self) -> Option<NodeId> {
        let event = self.scheduler.next()?;
        let time = event.time;
        let order = event.order;
        match event.event {
            NetEvent::Deliver { from, to, bytes } => {
                let len = bytes.len();
                if !self.online.contains(&to) {
                    self.record(
                        time,
                        order,
                        format!(
                            "deliver_drop t={time} from={from} to={to} len={len} reason=offline"
                        ),
                    );
                    return Some(to);
                }
                let peers = self.inbox.entry(to).or_default();
                peers.entry(from).or_default().push_back(bytes);
                self.record(
                    time,
                    order,
                    format!("deliver t={time} from={from} to={to} len={len}"),
                );
                Some(to)
            }
        }
    }

    pub fn recv_from(&mut self, node: NodeId, peer: NodeId) -> Option<Vec<u8>> {
        self.inbox
            .get_mut(&node)
            .and_then(|peers| peers.get_mut(&peer))
            .and_then(|q| q.pop_front())
    }

    fn should_drop(&self, from: NodeId, to: NodeId, seq: u64) -> bool {
        if self.faults.drop_rate <= 0.0 {
            return false;
        }
        let sample = self.sample_u32(from, to, seq) % 10_000;
        sample < (self.faults.drop_rate * 10_000.0) as u32
    }

    fn should_dup(&self, from: NodeId, to: NodeId, seq: u64) -> bool {
        if self.faults.dup_rate <= 0.0 {
            return false;
        }
        let sample = self.sample_u32(from, to, seq ^ 0xfeed) % 10_000;
        sample < (self.faults.dup_rate * 10_000.0) as u32
    }

    fn compute_delay(&self, from: NodeId, to: NodeId, seq: u64) -> i64 {
        let min = self.faults.min_delay.min(self.faults.max_delay);
        let max = self.faults.min_delay.max(self.faults.max_delay);
        if max <= min {
            return min;
        }
        let span = (max - min + 1) as u64;
        let value = (self.sample_u64(from, to, seq) % span) as i64;
        min + value as i64
    }

    fn next_send_seq(&mut self, from: NodeId) -> u64 {
        let entry = self.send_seq.entry(from).or_default();
        let seq = *entry;
        *entry = entry.wrapping_add(1);
        seq
    }

    fn order_key(&self, from: NodeId, seq: u64) -> u64 {
        ((from & 0xffff_ffff) << 32) | (seq & 0xffff_ffff)
    }

    fn sample_u32(&self, from: NodeId, to: NodeId, seq: u64) -> u32 {
        (self.sample_u64(from, to, seq) & 0xffff_ffff) as u32
    }

    fn sample_u64(&self, from: NodeId, to: NodeId, seq: u64) -> u64 {
        let mut z = self.seed ^ from.rotate_left(13) ^ to.rotate_left(29) ^ seq;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    pub fn trace_snapshot(&self) -> Vec<String> {
        let mut lines = self.trace.clone();
        lines.sort_by(|a, b| {
            a.time
                .cmp(&b.time)
                .then_with(|| a.order.cmp(&b.order))
                .then_with(|| a.line.cmp(&b.line))
        });
        lines.into_iter().map(|line| line.line).collect()
    }

    fn record(&mut self, time: i64, order: u64, line: String) {
        self.trace.push(TraceLine { time, order, line });
    }
}

pub struct SimHub {
    net: Mutex<SimNet>,
    cv: Condvar,
}

impl SimHub {
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            net: Mutex::new(SimNet::new(seed)),
            cv: Condvar::new(),
        })
    }

    pub fn now(&self) -> i64 {
        let net = self.net.lock().unwrap();
        net.now()
    }

    pub fn set_faults(&self, faults: FaultConfig) {
        if let Ok(mut net) = self.net.lock() {
            net.set_faults(faults);
        }
    }

    pub fn set_partition(&self, a: NodeId, b: NodeId, blocked: bool) {
        if let Ok(mut net) = self.net.lock() {
            net.set_partition(a, b, blocked);
        }
        self.cv.notify_all();
    }

    pub fn register_node(&self) -> NodeId {
        let mut net = self.net.lock().unwrap();
        net.register_node()
    }

    pub fn set_online(&self, node: NodeId, online: bool) {
        if let Ok(mut net) = self.net.lock() {
            net.set_online(node, online);
        }
        self.cv.notify_all();
    }

    pub fn send(&self, from: NodeId, to: NodeId, bytes: Vec<u8>) {
        if let Ok(mut net) = self.net.lock() {
            net.send(from, to, bytes);
        }
        self.cv.notify_all();
    }

    pub fn recv_from(&self, node: NodeId, peer: NodeId) -> Option<Vec<u8>> {
        let mut net = self.net.lock().unwrap();
        net.recv_from(node, peer)
    }

    pub fn has_pending(&self) -> bool {
        let net = self.net.lock().unwrap();
        net.has_pending()
    }

    pub fn has_pending_between(&self, node: NodeId, peer: NodeId) -> bool {
        let net = self.net.lock().unwrap();
        net.has_pending_between(node, peer)
    }

    pub fn trace_snapshot(&self) -> Vec<String> {
        let net = self.net.lock().unwrap();
        net.trace_snapshot()
    }

    pub fn step(&self) -> bool {
        let mut net = self.net.lock().unwrap();
        let delivered = net.step().is_some();
        if delivered {
            self.cv.notify_all();
        }
        delivered
    }

    fn wait(&self) {
        let guard = self.net.lock().unwrap();
        let _guard = self.cv.wait(guard).unwrap();
    }

    fn notify(&self) {
        self.cv.notify_all();
    }
}

pub struct SimStream {
    hub: Arc<SimHub>,
    node: NodeId,
    peer: NodeId,
    buffer: VecDeque<u8>,
    closed: Arc<AtomicBool>,
}

pub struct SimStreamControl {
    closed: Arc<AtomicBool>,
    hub: Arc<SimHub>,
}

impl SimStreamControl {
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.hub.notify();
    }
}

impl SimStream {
    pub fn pair(hub: Arc<SimHub>, a: NodeId, b: NodeId) -> (Self, Self, SimStreamControl) {
        let closed = Arc::new(AtomicBool::new(false));
        let control = SimStreamControl {
            closed: closed.clone(),
            hub: hub.clone(),
        };
        let left = Self {
            hub: hub.clone(),
            node: a,
            peer: b,
            buffer: VecDeque::new(),
            closed: closed.clone(),
        };
        let right = Self {
            hub,
            node: b,
            peer: a,
            buffer: VecDeque::new(),
            closed,
        };
        (left, right, control)
    }
}

impl Drop for SimStream {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.hub.notify();
    }
}

impl Read for SimStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if !self.buffer.is_empty() {
                let mut n = 0;
                while n < buf.len() && !self.buffer.is_empty() {
                    if let Some(byte) = self.buffer.pop_front() {
                        buf[n] = byte;
                        n += 1;
                    }
                }
                return Ok(n);
            }
            if let Some(chunk) = self.hub.recv_from(self.node, self.peer) {
                self.buffer.extend(chunk);
                continue;
            }
            if self.closed.load(Ordering::SeqCst) {
                if self.hub.has_pending_between(self.node, self.peer) {
                    self.hub.wait();
                    continue;
                }
                return Ok(0);
            }
            self.hub.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::thread;

    #[test]
    fn close_waits_for_pending_pair_delivery() {
        let hub = SimHub::new(123);
        let a = hub.register_node();
        let b = hub.register_node();
        let (mut stream_a, mut stream_b, control) = SimStream::pair(hub.clone(), a, b);

        stream_a.write_all(b"ping").unwrap();
        control.close();

        let reader = thread::spawn(move || {
            let mut buf = [0u8; 4];
            stream_b.read_exact(&mut buf).unwrap();
            buf
        });

        while hub.step() {}

        let buf = reader.join().unwrap();
        assert_eq!(&buf, b"ping");
    }
}

impl Write for SimStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hub.send(self.node, self.peer, buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
