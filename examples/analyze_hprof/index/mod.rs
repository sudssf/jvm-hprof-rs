use anyhow;

use jvm_hprof::{heap_dump::*, *};
use std::io::{Error, Write};
use std::path::Path;
use std::{cmp, fmt, io, path};

mod index_chunks;

pub mod lmdb;

use index_chunks::*;
use rayon::iter::{ParallelBridge, ParallelIterator};

pub(crate) fn build_index(hprof: &Hprof, output: &path::Path) -> Result<(), anyhow::Error> {
    let fingerprint = HprofFingerprint::from_hprof(hprof);

    let builder = ChunkedIndexBuilder::new_with_fingerprint(fingerprint, output.to_owned())?;

    println!("[1/2] Creating sorted chunks (each . = 1,000,000 objects processed)");

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
                                .insert_class_id(instance.obj_id(), instance.class_obj_id())?;
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            record_writer.insert_class_id(
                                obj_array.obj_id(),
                                obj_array.array_class_obj_id(),
                            )?;
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            record_writer
                                .insert_prim_array_type(pa.obj_id(), pa.primitive_type())?;
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

    println!("[2/2] Assembling final index structure");

    builder.finalize();

    Ok(())
}

pub trait IndexBuilder: Sized {
    type RecWriter: RecordWriter;

    /// Create a new index builder
    /// - `fingerprint` - Fingerprint of the hprof
    /// - `path` - Path to write the index data to
    fn new_with_fingerprint(
        fingerprint: HprofFingerprint,
        dest: path::PathBuf,
    ) -> Result<Self, anyhow::Error>;

    /// Create a RecordWriter for a particular record index
    fn record_writer(&self, record_index: usize) -> Result<Self::RecWriter, anyhow::Error>;

    /// Do any necessary conversion from intermediate formats to the final index structure.
    fn finalize(&self);
}

/// To be used to write the data for one individual hprof record.
pub trait RecordWriter {
    /// Insert a mapping from an object id (plain object or reference array, not Class object or primitive array) to a class id
    fn insert_class_id(&mut self, obj_id: Id, class_id: Id) -> Result<(), anyhow::Error>;

    /// Insert a mapping from an object id to a primitive array type
    fn insert_prim_array_type(
        &mut self,
        obj_id: Id,
        prim_array_type: PrimitiveArrayType,
    ) -> Result<(), anyhow::Error>;

    /// Flush any buffered data
    fn flush(&mut self) -> Result<(), anyhow::Error>;
}

pub trait Index: Sized {
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
