use heed_types::OwnedType;
use libsqlx::FrameNo;

use crate::meta::DatabaseId;

/// Stores replica last injected commit index
pub struct ReplicaCommitStore {
    env: heed::Env,
    database: heed::Database<OwnedType<DatabaseId>, OwnedType<FrameNo>>,
}

impl ReplicaCommitStore {
    const DB_NAME: &str = "replica-commit-store";
    pub fn new(env: heed::Env) -> Self {
        let mut txn = env.write_txn().unwrap();
        let database = env.create_database(&mut txn, Some(Self::DB_NAME)).unwrap();
        txn.commit().unwrap();

        Self { env, database }
    }

    pub fn commit(&self, database_id: DatabaseId, frame_no: FrameNo) {
        let mut txn = self.env.write_txn().unwrap();
        self.database
            .put(&mut txn, &database_id, &frame_no)
            .unwrap();
        txn.commit().unwrap();
    }

    pub fn get_commit_index(&self, database_id: DatabaseId) -> Option<FrameNo> {
        let txn = self.env.read_txn().unwrap();
        self.database.get(&txn, &database_id).unwrap()
    }
}
