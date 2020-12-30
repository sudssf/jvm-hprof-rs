use anyhow;
use sled;

use anyhow::Error;
use jvm_hprof::{heap_dump::*, *};
use std::convert::TryInto;
use std::{cmp, collections, fmt, path, time};

pub(crate) fn build_index<B: ObjClassIndexBuilder>(
    hprof: &Hprof,
    output: &path::Path,
) -> Result<(), anyhow::Error> {
    let fingerprint = HprofFingerprint::from_hprof(hprof);

    let mut builder = B::new_with_fingerprint(&fingerprint, output)?;

    // parsing is much, much faster than writing, so it does no good to parallelize parsing
    for r in hprof.records_iter().map(|r| r.unwrap()) {
        match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();

                let print_every = 1_000_000;
                let mut start = time::Instant::now();

                let mut count = 0_u64;
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    count += 1;
                    if count == print_every {
                        let elapsed = start.elapsed();
                        count = 0;
                        start = time::Instant::now();
                        println!(
                            "Indexed {} in {:?} ({}/s)",
                            print_every,
                            elapsed,
                            print_every as f64 / elapsed.as_secs_f64(),
                        );
                    }

                    match s {
                        SubRecord::Instance(instance) => {
                            builder.insert_class_id(instance.obj_id(), instance.class_obj_id())?;
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            builder.insert_class_id(
                                obj_array.obj_id(),
                                obj_array.array_class_obj_id(),
                            )?;
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            builder.insert_prim_array_type(pa.obj_id(), pa.primitive_type())?;
                        }
                        _ => {}
                    };
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub trait ObjClassIndexBuilder: Sized {
    type Index: ObjClassIndex;

    /// Create a new index builder
    /// - `fingerprint` - Fingerprint of the hprof
    /// - `path` - Path to write the index data to
    fn new_with_fingerprint(
        fingerprint: &HprofFingerprint,
        dest: &path::Path,
    ) -> Result<Self, anyhow::Error>;

    /// Insert a mapping from an object id (plain object or reference array, not Class object or primitive array) to a class id
    fn insert_class_id(&mut self, obj_id: Id, class_id: Id) -> Result<(), anyhow::Error>;

    fn insert_prim_array_type(
        &mut self,
        obj_id: Id,
        prim_array_type: PrimitiveArrayType,
    ) -> Result<(), anyhow::Error>;
}

pub trait ObjClassIndex: Sized {
    /// Open the index at the provided path, and make sure that its stored fingerprint matches
    /// `fingerprint`
    fn open_with_fingerprint(
        fingerprint: &HprofFingerprint,
        source: &path::Path,
    ) -> Result<Self, anyhow::Error>;

    /// Get the class id for an object id, if available.
    ///
    /// The object id must be for a normal object or a reference array type, not a java.lang.Class
    /// (which are represented separately in an hprof) or a primitive array (ditto).
    fn get_class_id(&self, obj_id: Id) -> Result<Option<Id>, anyhow::Error>;

    /// Get the primitive array type for an object id, if available.
    fn get_prim_array_type(&self, obj_id: Id) -> Result<Option<PrimitiveArrayType>, anyhow::Error>;
}

/// Easily acquired data about a particular hprof file to help avoid using the wrong index when
/// processing an hprof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HprofFingerprint {
    /// timestamp millis from header
    timestamp: u64,
    /// total number of top level records
    record_count: u64,
}

impl HprofFingerprint {
    pub(crate) fn from_hprof(hprof: &Hprof) -> HprofFingerprint {
        let timestamp = hprof.header().timestamp_millis();
        let record_count = crate::util::record_counts(hprof)
            .iter()
            .fold(0_u64, |acc, (_, count)| acc + count);

        HprofFingerprint {
            timestamp,
            record_count,
        }
    }
}

/// Compare the fingerprint with the timestamp and record count presumably loaded from the database,
/// and only invoke the builder if both data are present and match the fingerprint.
fn build_if_fingerprint_match<R, F, I>(
    fingerprint: &HprofFingerprint,
    ts: Option<R>,
    record_count: Option<R>,
    build: F,
) -> Result<I, anyhow::Error>
where
    R: AsRef<[u8]> + Clone + fmt::Debug + cmp::PartialEq,
    F: FnOnce() -> Result<I, anyhow::Error>,
    I: ObjClassIndex,
{
    ts.clone()
        .map(|bytes| bytes.as_ref() == &fingerprint.timestamp.to_le_bytes()[..])
        .zip(
            record_count
                .clone()
                .map(|bytes| bytes.as_ref() == &fingerprint.record_count.to_le_bytes()[..]),
        )
        .map(|(ts_match, count_match)| ts_match && count_match)
        .and_then(|matched| if matched { Some(build()) } else { None })
        .unwrap_or_else(|| {
            Err(anyhow::Error::msg(format!(
                "Fingerprint mismatch: expected {:?}, got timestamp {:?} count {:?}",
                fingerprint, ts, record_count
            )))
        })
}

pub(crate) struct SledIndex {
    obj_id_class_id: sled::Tree,
    obj_id_prim_type: sled::Tree,
}

// fingerprint keys
const FP_TIMESTAMP: &str = "__hprof_header_fingerprint_timestamp";
const FP_RECORD_COUNT: &str = "__hprof_header_fingerprint_record_count";

// tree names
const FINGERPRINT: &str = "fingerprint";
const OBJ_ID_CLASS_ID: &str = "obj_id_class_id";
const OBJ_ID_PRIM_TYPE: &str = "obj_id_prim_type";

impl ObjClassIndexBuilder for SledIndex {
    type Index = SledIndex;

    fn new_with_fingerprint(
        fingerprint: &HprofFingerprint,
        dest: &path::Path,
    ) -> Result<Self, anyhow::Error> {
        let db = sled::Config::new().create_new(true).path(dest).open()?;

        let fingerprint_tree = db.open_tree(FINGERPRINT)?;

        fingerprint_tree.insert(
            FP_TIMESTAMP.as_bytes(),
            &fingerprint.timestamp.to_le_bytes(),
        )?;
        fingerprint_tree.insert(
            FP_RECORD_COUNT.as_bytes(),
            &fingerprint.record_count.to_le_bytes(),
        )?;

        Ok(SledIndex {
            obj_id_class_id: db.open_tree(OBJ_ID_CLASS_ID)?,
            obj_id_prim_type: db.open_tree(OBJ_ID_PRIM_TYPE)?,
        })
    }

    fn insert_class_id(&mut self, obj_id: Id, class_id: Id) -> Result<(), Error> {
        self.obj_id_class_id
            .insert(&obj_id.id().to_le_bytes(), &class_id.id().to_le_bytes())?;

        Ok(())
    }

    fn insert_prim_array_type(
        &mut self,
        obj_id: Id,
        prim_array_type: PrimitiveArrayType,
    ) -> Result<(), Error> {
        self.obj_id_prim_type
            .insert(&obj_id.id().to_le_bytes(), &[prim_array_type.type_code()])?;

        Ok(())
    }
}

impl ObjClassIndex for SledIndex {
    fn open_with_fingerprint(
        fingerprint: &HprofFingerprint,
        source: &path::Path,
    ) -> Result<Self, Error> {
        let db = sled::open(source)?;

        let trees = db
            .tree_names()
            .into_iter()
            .collect::<collections::HashSet<_>>();

        for &tree_name in [FINGERPRINT, OBJ_ID_CLASS_ID, OBJ_ID_PRIM_TYPE].iter() {
            if !trees.contains(tree_name.as_bytes()) {
                return Err(anyhow::Error::msg(format!(
                    "Db did not contain the required tree {}",
                    tree_name
                )));
            }
        }

        let fingerprint_tree = db.open_tree(FINGERPRINT)?;

        let ts = fingerprint_tree.get(FP_TIMESTAMP)?;
        let record_count = fingerprint_tree.get(FP_RECORD_COUNT)?;

        build_if_fingerprint_match(fingerprint, ts, record_count, move || {
            Ok(SledIndex {
                obj_id_class_id: db.open_tree(OBJ_ID_CLASS_ID)?,
                obj_id_prim_type: db.open_tree(OBJ_ID_PRIM_TYPE)?,
            })
        })
    }

    fn get_class_id(&self, obj_id: Id) -> Result<Option<Id>, anyhow::Error> {
        self.obj_id_class_id
            .get(obj_id.id().to_le_bytes())
            .map(|value| {
                value.map(|vec| {
                    Id::from(u64::from_le_bytes(
                        vec.as_ref().try_into().expect("Invalid index entry"),
                    ))
                })
            })
            .map_err(|e| anyhow::Error::from(e))
    }

    fn get_prim_array_type(&self, obj_id: Id) -> Result<Option<PrimitiveArrayType>, Error> {
        self.obj_id_prim_type
            .get(obj_id.id().to_le_bytes())
            .map(|opt_data| {
                opt_data.map(|data| {
                    let byte = *data.as_ref().get(0).expect("Invalid index entry");
                    PrimitiveArrayType::from_type_code(byte).expect("Invalid array type code")
                })
            })
            .map_err(|e| anyhow::Error::from(e))
    }
}
