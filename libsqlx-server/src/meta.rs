use uuid::Uuid;

use crate::allocation::config::AllocConfig;

pub struct MetaStore {}

impl MetaStore {
    pub async fn allocate(&self, alloc_id: &str, meta: &AllocConfig) {}
    pub async fn deallocate(&self, alloc_id: Uuid) {}
}
