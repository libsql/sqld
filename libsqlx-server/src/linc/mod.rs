use uuid::Uuid;

use self::proto::StreamId;

pub mod bus;
pub mod connection;
pub mod connection_pool;
pub mod net;
pub mod proto;
pub mod server;

type NodeId = Uuid;
type DatabaseId = Uuid;

const CURRENT_PROTO_VERSION: u32 = 1;
const MAX_STREAM_MSG: usize = 64;

#[derive(Debug)]
pub struct StreamIdAllocator {
    direction: i32,
    next_id: i32,
}

impl StreamIdAllocator {
    fn new(positive: bool) -> Self {
        let direction = if positive { 1 } else { -1 };
        Self {
            direction,
            next_id: direction,
        }
    }

    pub fn allocate(&mut self) -> Option<StreamId> {
        let id = self.next_id;
        self.next_id = id.checked_add(self.direction)?;
        Some(StreamId::new(id))
    }
}
