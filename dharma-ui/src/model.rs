use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BufferId(u64);

impl BufferId {
    pub fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BufferKind {
    Text,
    Data,
    Logic,
}

impl fmt::Display for BufferKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufferKind::Text => write!(f, "text"),
            BufferKind::Data => write!(f, "data"),
            BufferKind::Logic => write!(f, "logic"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Buffer {
    pub id: BufferId,
    pub title: String,
    pub kind: BufferKind,
    pub contents: String,
}

pub struct BufferModel {
    next_id: u64,
    buffers: Vec<Buffer>,
}

impl BufferModel {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            buffers: Vec::new(),
        }
    }

    pub fn create(&mut self, title: impl Into<String>, kind: BufferKind, contents: impl Into<String>) -> BufferId {
        let id = BufferId(self.next_id);
        self.next_id += 1;
        let buffer = Buffer {
            id,
            title: title.into(),
            kind,
            contents: contents.into(),
        };
        self.buffers.push(buffer);
        id
    }

    pub fn list(&self) -> &[Buffer] {
        &self.buffers
    }

    pub fn get(&self, id: BufferId) -> Option<&Buffer> {
        self.buffers.iter().find(|b| b.id == id)
    }
}
