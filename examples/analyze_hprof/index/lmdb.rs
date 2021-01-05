use crate::index::{
    build_if_fingerprint_match, HprofFingerprint, Index, IndexBuilder, IndexSequence,
};

use std::{fs, io, path};

use anyhow::Context;
use itertools::Itertools;
use jvm_hprof::heap_dump::PrimitiveArrayType;
use jvm_hprof::Id;
use lmdb;
use lmdb::{Database, Error, Transaction};
use std::convert::TryInto;
use std::io::Write;

// fingerprint keys
const FP_TIMESTAMP: &str = "__hprof_header_fingerprint_timestamp";
const FP_RECORD_COUNT: &str = "__hprof_header_fingerprint_record_count";

// tree names
const DB_METADATA: &str = "metadata";
const DB_OBJ_ID_CLASS_ID: &str = "obj_id_class_id";
const DB_OBJ_ID_PRIM_TYPE: &str = "obj_id_prim_type";

pub(crate) struct LmdbIndex {
    env: lmdb::Environment,
    obj_id_class_id_db: lmdb::Database,
    obj_id_prim_array_type_db: lmdb::Database,
}

impl Index for LmdbIndex {
    fn open_with_fingerprint(
        fingerprint: &HprofFingerprint,
        index_path: &path::Path,
    ) -> Result<Self, anyhow::Error> {
        let mut lmdb_dir = index_path.to_path_buf();
        lmdb_dir.push("lmdb");

        let env = lmdb::Environment::new()
            .set_flags(lmdb::EnvironmentFlags::READ_ONLY)
            .set_max_dbs(3)
            .open(&lmdb_dir)?;

        let metadata_db = env
            .open_db(Some(DB_METADATA))
            .with_context(|| "Opening metadata DB")?;
        let obj_id_class_id_db = env.open_db(Some(DB_OBJ_ID_CLASS_ID))?;
        let obj_id_prim_array_type_db = env.open_db(Some(DB_OBJ_ID_PRIM_TYPE))?;

        let txn = env.begin_ro_txn()?;

        let ts = txn
            .get_opt(metadata_db, &FP_TIMESTAMP)?
            // clone the data so we can commit the txn before moving env into the LmdbIndex
            .map(|slice| slice.iter().map(|&b| b).collect_vec());
        let record_count = txn
            .get_opt(metadata_db, &FP_RECORD_COUNT)?
            .map(|slice| slice.iter().map(|&b| b).collect_vec());

        txn.commit()?;

        let res = build_if_fingerprint_match(fingerprint, ts, record_count, || {
            Ok(LmdbIndex {
                env,
                obj_id_class_id_db,
                obj_id_prim_array_type_db,
            })
        });

        res
    }

    fn get_class_id(&self, obj_id: Id) -> Result<Option<Id>, anyhow::Error> {
        let txn = self.env.begin_ro_txn()?;

        txn.get_opt(self.obj_id_class_id_db, &obj_id.id().to_be_bytes())
            .map(|opt| {
                opt.map(|bytes| {
                    Id::from(u64::from_be_bytes(
                        bytes.try_into().expect("Invalid index value"),
                    ))
                })
            })
            // txn will commit in its Drop impl but might as well be explicit if we haven't already errored out
            .and_then(|id| txn.commit().map(|_| id))
            .map_err(|e| anyhow::Error::from(e))
    }

    fn get_prim_array_type(&self, obj_id: Id) -> Result<Option<PrimitiveArrayType>, anyhow::Error> {
        let txn = self.env.begin_ro_txn()?;

        txn.get_opt(self.obj_id_prim_array_type_db, &obj_id.id().to_be_bytes())
            .map(|opt| {
                opt.map(|bytes| {
                    PrimitiveArrayType::from_type_code(bytes[0]).expect("Invalid index value")
                })
            })
            // txn will commit in its Drop impl but might as well be explicit if we haven't already errored out
            .and_then(|id| txn.commit().map(|_| id))
            .map_err(|e| anyhow::Error::from(e))
    }
}

impl IndexBuilder for LmdbIndex {
    fn build_index<S: IndexSequence>(
        seq: &S,
        fingerprint: &HprofFingerprint,
        index_path: &path::Path,
    ) -> Result<(), anyhow::Error> {
        let mut lmdb_dir = index_path.to_path_buf();
        lmdb_dir.push("lmdb");

        fs::create_dir_all(&lmdb_dir)?;

        let env = lmdb::Environment::new()
            // a terabyte would be a very big index indeed
            .set_map_size(1024 * 1024 * 1024 * 1024)
            .set_max_dbs(3)
            .open(&lmdb_dir)?;

        // TODO report bug: opening a db after opening a txn hangs
        let metadata_db = env.create_db(Some(DB_METADATA), lmdb::DatabaseFlags::default())?;
        let obj_id_class_id_db =
            env.create_db(Some(DB_OBJ_ID_CLASS_ID), lmdb::DatabaseFlags::default())?;
        let obj_id_prim_type_db =
            env.create_db(Some(DB_OBJ_ID_PRIM_TYPE), lmdb::DatabaseFlags::default())?;

        let mut txn = env.begin_rw_txn()?;

        // using big-endian to stay consistent with the rest of the numbers
        txn.put(
            metadata_db,
            &FP_TIMESTAMP,
            &fingerprint.timestamp.to_be_bytes(),
            lmdb::WriteFlags::default(),
        )?;
        txn.put(
            metadata_db,
            &FP_RECORD_COUNT,
            &fingerprint.record_count.to_be_bytes(),
            lmdb::WriteFlags::default(),
        )?;

        let mut count_since_last_print = 0_u64;
        let print_threshold = 1_000_000;

        {
            let mut cursor = txn.open_rw_cursor(obj_id_class_id_db)?;

            for res in seq.iter_obj_id_class_id()? {
                let (key, value): (u64, u64) = res?;
                cursor.put(
                    &key.to_be_bytes(),
                    &value.to_be_bytes(),
                    lmdb::WriteFlags::APPEND,
                )?;
                count_since_last_print += 1;

                if count_since_last_print == print_threshold {
                    print!(".");
                    io::stdout().flush()?;
                    count_since_last_print = 0;
                }
            }
        }

        {
            let mut cursor = txn.open_rw_cursor(obj_id_prim_type_db)?;

            for res in seq.iter_obj_id_prim_array_type()? {
                let (key, value): (u64, u8) = res?;
                cursor.put(&key.to_be_bytes(), &[value], lmdb::WriteFlags::APPEND)?;
                count_since_last_print += 1;

                if count_since_last_print == print_threshold {
                    print!(".");
                    io::stdout().flush()?;
                    count_since_last_print = 0;
                }
            }
        }

        txn.commit()?;

        Ok(())
    }
}

trait LmdbTxnExt {
    /// Express missing as Option instead of using the lmdb::Error::NotFound case.
    fn get_opt<K: AsRef<[u8]>>(
        &self,
        database: lmdb::Database,
        key: &K,
    ) -> Result<Option<&[u8]>, lmdb::Error>;
}

impl<T: lmdb::Transaction> LmdbTxnExt for T {
    fn get_opt<K: AsRef<[u8]>>(&self, database: Database, key: &K) -> Result<Option<&[u8]>, Error> {
        match self.get(database, key) {
            Ok(x) => Ok(Some(x)),
            Err(e) => match e {
                lmdb::Error::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow;
    use jvm_hprof::EnumIterable;
    use rand;
    use tempfile;

    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::io;

    #[test]
    fn build_index_from_seq() -> Result<(), anyhow::Error> {
        let mut obj_id_class_id = Vec::<(u64, u64)>::new();
        let mut obj_id_prim_array_type = Vec::<(u64, u8)>::new();

        let mut rng = rand::thread_rng();
        let array_types = PrimitiveArrayType::iter().collect_vec();

        for _ in 0..100_000 {
            obj_id_class_id.push((rng.gen(), rng.gen()));
            obj_id_prim_array_type
                .push((rng.gen(), array_types.choose(&mut rng).unwrap().type_code()));
        }

        // seq data must be sorted
        obj_id_class_id.sort_unstable_by_key(|&(obj_id, _)| obj_id);
        obj_id_prim_array_type.sort_unstable_by_key(|&(obj_id, _)| obj_id);

        let seq = VecIndexSeq {
            obj_id_class_id,
            obj_id_prim_array_type,
        };

        let fingerprint = HprofFingerprint {
            timestamp: 1000,
            record_count: 2000,
        };

        let index_dir = tempfile::tempdir()?;

        LmdbIndex::build_index(&seq, &fingerprint, index_dir.path())?;

        let index = LmdbIndex::open_with_fingerprint(&fingerprint, index_dir.path())?;

        for &(obj_id, class_id) in seq.obj_id_class_id.iter() {
            assert_eq!(
                Some(Id::from(class_id)),
                index.get_class_id(Id::from(obj_id))?,
                "obj id: {}",
                obj_id
            );
        }

        for &(obj_id, prim_type_code) in seq.obj_id_prim_array_type.iter() {
            assert_eq!(
                Some(PrimitiveArrayType::from_type_code(prim_type_code).unwrap()),
                index.get_prim_array_type(Id::from(obj_id))?,
                "obj id: {}",
                obj_id
            );
        }

        // don't wipe the tmp dir until we're done reading from it
        drop(index_dir);
        Ok(())
    }

    struct VecIndexSeq {
        obj_id_class_id: Vec<(u64, u64)>,
        obj_id_prim_array_type: Vec<(u64, u8)>,
    }

    impl IndexSequence for VecIndexSeq {
        // accept the dynamic dispatch overhead so we don't have some gnarly type here
        type ObjIdClassIdIterator = Box<dyn Iterator<Item = Result<(u64, u64), io::Error>>>;
        type ObjIdPrimArrayTypeIterator = Box<dyn Iterator<Item = Result<(u64, u8), io::Error>>>;

        fn iter_obj_id_class_id(&self) -> Result<Self::ObjIdClassIdIterator, anyhow::Error> {
            Ok(Box::new(
                self.obj_id_class_id
                    .clone()
                    .into_iter()
                    .map(|elem| Ok(elem)),
            ))
        }

        fn iter_obj_id_prim_array_type(
            &self,
        ) -> Result<Self::ObjIdPrimArrayTypeIterator, anyhow::Error> {
            Ok(Box::new(
                self.obj_id_prim_array_type
                    .clone()
                    .into_iter()
                    .map(|elem| Ok(elem)),
            ))
        }

        fn remove_tmp_files(self) -> Result<(), io::Error> {
            Ok(())
        }
    }
}
