use std::collections::HashMap;

use super::DatabaseId;

pub enum Database {
    Replica,
    Primary,
}

pub struct DatabaseManager {
    databases: HashMap<DatabaseId, Database>,
}
