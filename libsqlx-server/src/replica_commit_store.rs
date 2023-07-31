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
    pub fn new(env: heed::Env) -> crate::Result<Self> {
        let mut txn = env.write_txn()?;
        let database = env.create_database(&mut txn, Some(Self::DB_NAME))?;
        txn.commit()?;

        Ok(Self { env, database })
    }

    pub fn commit(&self, database_id: DatabaseId, frame_no: FrameNo) -> crate::Result<()> {
        let mut txn = self.env.write_txn()?;
        self.database.put(&mut txn, &database_id, &frame_no)?;
        txn.commit()?;

        Ok(())
    }

    pub fn get_commit_index(&self, database_id: DatabaseId) -> crate::Result<Option<FrameNo>> {
        let txn = self.env.read_txn()?;
        Ok(self.database.get(&txn, &database_id)?)
    }
}
