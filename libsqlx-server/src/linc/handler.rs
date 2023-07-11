use super::{bus::{Bus}, Inbound};

pub trait Handler: Sized + Send + Sync + 'static {
    fn handle(&self, bus: &Bus<Self>, msg: Inbound);
}

