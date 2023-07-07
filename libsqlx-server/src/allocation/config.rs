use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum AllocConfig {
    Primary { },
    Replica {
        primary_node_id: String,
    }
}
