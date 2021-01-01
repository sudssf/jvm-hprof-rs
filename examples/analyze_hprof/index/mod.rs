use anyhow;
use is_sorted;

use crate::index::lmdb::LmdbIndex;
use index_chunks::*;
use is_sorted::IsSorted;
use itertools::Itertools;
use jvm_hprof::{heap_dump::*, *};
use merge::*;
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::io::Write;
use std::{cmp, fmt, fs, io, path};

mod index_chunks;
pub mod lmdb;
mod merge;

// subdir where obj id to class id mappings are written
const SUBDIR_OBJ_CLASS: &str = "obj-id-class-id";
// same, but for obj id to primitive array type
const SUBDIR_OBJ_PRIM_ARRAY_TYPE: &str = "obj-id-prim-array-type";

pub(crate) fn build_index(hprof: &Hprof, output: &path::Path) -> Result<(), anyhow::Error> {
    let fingerprint = HprofFingerprint::from_hprof(hprof);

    let builder = ChunkedIndexSeqBuilder::new(output.to_owned())?;

    println!("[1/3] Creating sorted chunks (. = 1,000,000 objects processed)");

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .enumerate()
        .par_bridge()
        .map(|(record_index, r)| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let mut record_writer = builder.record_writer(record_index)?;
                let segment = r.as_heap_dump_segment().unwrap().unwrap();

                let print_every = 1_000_000;

                let mut count = 0_u64;
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    count += 1;
                    if count == print_every {
                        count = 0;
                        print!(".");
                        io::stdout().flush()?;
                    }

                    match s {
                        SubRecord::Instance(instance) => {
                            record_writer
                                .write_class_id(instance.obj_id(), instance.class_obj_id())?;
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            record_writer.write_class_id(
                                obj_array.obj_id(),
                                obj_array.array_class_obj_id(),
                            )?;
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            record_writer
                                .write_prim_array_type(pa.obj_id(), pa.primitive_type())?;
                        }
                        _ => {}
                    };
                }

                // write any remaining partial chunk
                record_writer.flush()?;

                Ok(())
            }
            _ => Ok(()),
        })
        .for_each(|res: Result<(), anyhow::Error>| {
            res.unwrap();
        });

    println!("\n[2/3] Merge-sorting index data (. = 1 merged file written)");

    let mut index_seq = builder.finalize()?;

    println!("\n[3/3] Assembling final index structure (. = 1,000,000 index entries inserted)");

    LmdbIndex::build_index(&mut index_seq, fingerprint, output)?;

    index_seq.remove_tmp_files()?;

    Ok(())
}

// Sized so Self can be used in return types
pub trait Index: Sized {
    /// Open the index at the provided path, and make sure that its stored fingerprint matches
    /// `fingerprint`
    fn open_with_fingerprint(
        fingerprint: &HprofFingerprint,
        index_path: &path::Path,
    ) -> Result<Self, anyhow::Error>;

    /// Get the class id for an object id, if available.
    ///
    /// The object id must be for a normal object or a reference array type, not a java.lang.Class
    /// (which are represented separately in an hprof) or a primitive array (ditto).
    fn get_class_id(&self, obj_id: Id) -> Result<Option<Id>, anyhow::Error>;

    /// Get the primitive array type for an object id, if available.
    fn get_prim_array_type(&self, obj_id: Id) -> Result<Option<PrimitiveArrayType>, anyhow::Error>;
}

/// Consumes an [IndexSequence] to produce the final [Index].
pub trait IndexBuilder {
    fn build_index<S: IndexSequence>(
        seq: &mut S,
        fingerprint: HprofFingerprint,
        index_dir: &path::Path,
    ) -> Result<(), anyhow::Error>;
}

/// Accumulates a sorted intermediate stage of the index.
///
/// Creating a billion-key data structure out of a random key ordering is brutally slow. Depending
/// on the datastore, importing sorted keys can be several orders of magnitude faster, so we sort
/// the data first.
// Sized so Self can be used in return types
pub trait IndexSequenceBuilder: Sized {
    type RecWriter: RecordWriter;
    type Seq: IndexSequence;

    /// Create a new index builder
    /// - `path` - Path to write the index data to
    fn new(dest: path::PathBuf) -> Result<Self, anyhow::Error>;

    /// Create a RecordWriter for a particular record index.
    ///
    /// Multiple Records can writing to their own RecordWriter in parallel.
    fn record_writer(&self, record_index: usize) -> Result<Self::RecWriter, anyhow::Error>;

    /// Once all RecordWriters are finished, do any necessary conversion from intermediate formats
    /// to the final index structure.
    fn finalize(&self) -> Result<Self::Seq, anyhow::Error>;
}

/// The output of [IndexSequenceBuilder]. Provides in-order iteration over index data.
pub trait IndexSequence {
    type ObjIdClassIdIterator: Iterator<Item = Result<(u64, u64), io::Error>>;
    type ObjIdPrimArrayTypeIterator: Iterator<Item = Result<(u64, u8), io::Error>>;

    fn iter_obj_id_class_id(&mut self) -> Result<Self::ObjIdClassIdIterator, anyhow::Error>;
    fn iter_obj_id_prim_array_type(
        &mut self,
    ) -> Result<Self::ObjIdPrimArrayTypeIterator, anyhow::Error>;

    fn remove_tmp_files(self) -> Result<(), io::Error>;
}

/// To be used to write the data for one individual hprof record.
pub trait RecordWriter {
    /// Insert a mapping from an object id (plain object or reference array, not Class object or primitive array) to a class id
    fn write_class_id(&mut self, obj_id: Id, class_id: Id) -> Result<(), anyhow::Error>;

    /// Insert a mapping from an object id to a primitive array type
    fn write_prim_array_type(
        &mut self,
        obj_id: Id,
        prim_array_type: PrimitiveArrayType,
    ) -> Result<(), anyhow::Error>;

    /// Flush any buffered data
    fn flush(self) -> Result<(), anyhow::Error>;
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
    I: Index,
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
