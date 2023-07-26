use std::mem::size_of;
use std::path::PathBuf;

use bytemuck::{Pod, Zeroable};
use heed_types::{CowType, SerdeBincode};
use libsqlx::FrameNo;
use serde::{Deserialize, Serialize};
use tokio::task::block_in_place;
use uuid::Uuid;

use crate::meta::DatabaseId;

#[derive(Clone, Copy, Zeroable, Pod, Debug)]
#[repr(transparent)]
struct BEU64([u8; size_of::<u64>()]);

impl From<u64> for BEU64 {
    fn from(value: u64) -> Self {
        Self(value.to_be_bytes())
    }
}

impl From<BEU64> for u64 {
    fn from(value: BEU64) -> Self {
        u64::from_be_bytes(value.0)
    }
}

#[derive(Clone, Copy, Zeroable, Pod, Debug)]
#[repr(C)]
struct SnapshotKey {
    database_id: DatabaseId,
    start_frame_no: BEU64,
    end_frame_no: BEU64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub snapshot_id: Uuid,
}

pub struct SnapshotStore {
    env: heed::Env,
    database: heed::Database<CowType<SnapshotKey>, SerdeBincode<SnapshotMeta>>,
    db_path: PathBuf,
}

impl SnapshotStore {
    const SNAPSHOT_STORE_NAME: &str = "snapshot-store-db";

    pub fn new(db_path: PathBuf, env: heed::Env) -> color_eyre::Result<Self> {
        let mut txn = env.write_txn().unwrap();
        let database = env.create_database(&mut txn, Some(Self::SNAPSHOT_STORE_NAME))?;
        txn.commit()?;

        Ok(Self {
            database,
            db_path,
            env,
        })
    }

    pub fn register(
        &self,
        txn: &mut heed::RwTxn,
        database_id: DatabaseId,
        start_frame_no: FrameNo,
        end_frame_no: FrameNo,
        snapshot_id: Uuid,
    ) {
        let key = SnapshotKey {
            database_id,
            start_frame_no: start_frame_no.into(),
            end_frame_no: end_frame_no.into(),
        };

        let data = SnapshotMeta { snapshot_id };

        block_in_place(|| self.database.put(txn, &key, &data).unwrap());
    }

    /// Locate a snapshot for `database_id` that contains `frame_no`
    pub fn locate(&self, database_id: DatabaseId, frame_no: FrameNo) -> Option<SnapshotMeta> {
        let txn = self.env.read_txn().unwrap();
        // Snapshot keys being lexicographically ordered, looking for the first key less than of
        // equal to (db_id, frame_no, FrameNo::MAX) will always return the entry we're looking for
        // if it exists.
        let key = SnapshotKey {
            database_id,
            start_frame_no: frame_no.into(),
            end_frame_no: u64::MAX.into(),
        };

        match self
            .database
            .get_lower_than_or_equal_to(&txn, &key)
            .transpose()?
        {
            Ok((key, v)) => {
                if key.database_id != database_id {
                    return None;
                } else if frame_no >= key.start_frame_no.into()
                    && frame_no <= key.end_frame_no.into()
                {
                    return Some(v);
                } else {
                    None
                }
            }
            Err(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insert_and_locate() {
        let temp = tempfile::tempdir().unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(1000 * 4096)
            .open(temp.path())
            .unwrap();
        let store = SnapshotStore::new(temp.path().to_path_buf(), env).unwrap();
        let mut txn = store.env.write_txn().unwrap();
        let db_id = DatabaseId::random();
        let snapshot_id = Uuid::new_v4();
        store.register(&mut txn, db_id, 0, 51, snapshot_id);
        txn.commit().unwrap();

        assert!(store.locate(db_id, 0).is_some());
        assert!(store.locate(db_id, 17).is_some());
        assert!(store.locate(db_id, 51).is_some());
        assert!(store.locate(db_id, 52).is_none());
    }

    #[test]
    fn multiple_snapshots() {
        let temp = tempfile::tempdir().unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(1000 * 4096)
            .open(temp.path())
            .unwrap();
        let store = SnapshotStore::new(temp.path().to_path_buf(), env).unwrap();
        let mut txn = store.env.write_txn().unwrap();
        let db_id = DatabaseId::random();
        let snapshot_1_id = Uuid::new_v4();
        store.register(&mut txn, db_id, 0, 51, snapshot_1_id);
        let snapshot_2_id = Uuid::new_v4();
        store.register(&mut txn, db_id, 52, 112, snapshot_2_id);
        txn.commit().unwrap();

        assert_eq!(store.locate(db_id, 0).unwrap().snapshot_id, snapshot_1_id);
        assert_eq!(store.locate(db_id, 17).unwrap().snapshot_id, snapshot_1_id);
        assert_eq!(store.locate(db_id, 51).unwrap().snapshot_id, snapshot_1_id);
        assert_eq!(store.locate(db_id, 52).unwrap().snapshot_id, snapshot_2_id);
        assert_eq!(store.locate(db_id, 100).unwrap().snapshot_id, snapshot_2_id);
        assert_eq!(store.locate(db_id, 112).unwrap().snapshot_id, snapshot_2_id);
        assert!(store.locate(db_id, 12345).is_none());
    }

    #[test]
    fn multiple_databases() {
        let temp = tempfile::tempdir().unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(1000 * 4096)
            .open(temp.path())
            .unwrap();
        let store = SnapshotStore::new(temp.path().to_path_buf(), env).unwrap();
        let mut txn = store.env.write_txn().unwrap();
        let db_id1 = DatabaseId::random();
        let db_id2 = DatabaseId::random();
        let snapshot_id1 = Uuid::new_v4();
        let snapshot_id2 = Uuid::new_v4();
        store.register(&mut txn, db_id1, 0, 51, snapshot_id1);
        store.register(&mut txn, db_id2, 0, 51, snapshot_id2);
        txn.commit().unwrap();

        assert_eq!(store.locate(db_id1, 0).unwrap().snapshot_id, snapshot_id1);
        assert_eq!(store.locate(db_id2, 0).unwrap().snapshot_id, snapshot_id2);

        assert_eq!(store.locate(db_id1, 12).unwrap().snapshot_id, snapshot_id1);
        assert_eq!(store.locate(db_id2, 18).unwrap().snapshot_id, snapshot_id2);

        assert_eq!(store.locate(db_id1, 51).unwrap().snapshot_id, snapshot_id1);
        assert_eq!(store.locate(db_id2, 51).unwrap().snapshot_id, snapshot_id2);

        assert!(store.locate(db_id1, 52).is_none());
        assert!(store.locate(db_id2, 52).is_none());
    }
}
