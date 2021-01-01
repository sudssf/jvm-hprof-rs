use super::*;
use std::convert::TryInto;
use std::{fs, io, marker, path};

/// An [IndexSequenceBuilder] that delegates to [ChunkedRecordWriter] for the actual work
pub(crate) struct ChunkedIndexSeqBuilder {
    dest: path::PathBuf,
}

impl IndexSequenceBuilder for ChunkedIndexSeqBuilder {
    type RecWriter = ChunkedRecordWriter<DirWriterFactory, U64PairData, U64U8Data>;
    type Seq = MergedFileIndexSequence;

    fn new(dest: path::PathBuf) -> Result<Self, anyhow::Error> {
        Ok(ChunkedIndexSeqBuilder { dest })
    }

    fn record_writer(&self, record_index: usize) -> Result<Self::RecWriter, anyhow::Error> {
        let mut class_dest = self.dest.clone();
        class_dest.push("chunks");
        class_dest.push(SUBDIR_OBJ_CLASS);
        fs::create_dir_all(&class_dest)?;
        let class_chunk_factory = DirWriterFactory { dest: class_dest };

        let mut prim_type_dest = self.dest.clone();
        prim_type_dest.push("chunks");
        prim_type_dest.push(SUBDIR_OBJ_PRIM_ARRAY_TYPE);
        fs::create_dir_all(&prim_type_dest)?;
        let prim_array_chunk_factory = DirWriterFactory {
            dest: prim_type_dest,
        };

        Ok(ChunkedRecordWriter {
            obj_class_chunk_writer: SortedChunkWriter::new(
                record_index,
                // 16M * 16 bytes per pair = 256MiB chunks
                16 * 1024 * 1024,
                class_chunk_factory,
            ),
            obj_prim_array_type_chunk_writer: SortedChunkWriter::new(
                record_index,
                // 28M * 9 bytes per pair = approx 256MiB chunks
                28 * 1024 * 1024,
                prim_array_chunk_factory,
            ),
        })
    }

    fn finalize(&self) -> Result<Self::Seq, anyhow::Error> {
        println!("Merging obj id to class id files");
        let merged_obj_class_file =
            merge_chunk_type::<_, U64PairData>(&self.dest, SUBDIR_OBJ_CLASS)?;
        println!("Merging obj id to primitive array type files");
        let merged_obj_prim_type_file =
            merge_chunk_type::<_, U64U8Data>(&self.dest, SUBDIR_OBJ_PRIM_ARRAY_TYPE)?;

        Ok(MergedFileIndexSequence {
            obj_id_class_id_file: merged_obj_class_file,
            obj_id_prim_array_type_file: merged_obj_prim_type_file,
        })
    }
}

/// An [IndexSequence] that reads from the merge-sorted files generated by [ChunkedIndexSeqBuilder].
pub(crate) struct MergedFileIndexSequence {
    obj_id_class_id_file: path::PathBuf,
    obj_id_prim_array_type_file: path::PathBuf,
}

impl IndexSequence for MergedFileIndexSequence {
    type ObjIdClassIdIterator =
        ChunkDatumIterator<io::BufReader<fs::File>, (u64, u64), U64PairData>;
    type ObjIdPrimArrayTypeIterator =
        ChunkDatumIterator<io::BufReader<fs::File>, (u64, u8), U64U8Data>;

    fn iter_obj_id_class_id(&mut self) -> Result<Self::ObjIdClassIdIterator, anyhow::Error> {
        Ok(ChunkDatumIterator::new(io::BufReader::new(fs::File::open(
            &self.obj_id_class_id_file,
        )?)))
    }

    fn iter_obj_id_prim_array_type(
        &mut self,
    ) -> Result<Self::ObjIdPrimArrayTypeIterator, anyhow::Error> {
        Ok(ChunkDatumIterator::new(io::BufReader::new(fs::File::open(
            &self.obj_id_prim_array_type_file,
        )?)))
    }

    fn remove_tmp_files(self) -> Result<(), io::Error> {
        fs::remove_file(&self.obj_id_class_id_file)
            .and_then(|_| fs::remove_file(&self.obj_id_prim_array_type_file))
    }
}

/// Write per-Record data into sorted chunks
pub(crate) struct ChunkedRecordWriter<F, D1, D2>
where
    F: ChunkWriterFactory,
    // obj id -> class id
    D1: DatumSerializer<(u64, u64)>,
    // obj id -> prim type
    D2: DatumSerializer<(u64, u8)>,
{
    obj_class_chunk_writer: SortedChunkWriter<F, (u64, u64), D1>,
    obj_prim_array_type_chunk_writer: SortedChunkWriter<F, (u64, u8), D2>,
}

impl<F, D1, D2> RecordWriter for ChunkedRecordWriter<F, D1, D2>
where
    F: ChunkWriterFactory,
    D1: DatumSerializer<(u64, u64)>,
    D2: DatumSerializer<(u64, u8)>,
{
    fn write_class_id(&mut self, obj_id: Id, class_id: Id) -> Result<(), anyhow::Error> {
        self.obj_class_chunk_writer
            .append((obj_id.id(), class_id.id()))
            .map_err(|e| anyhow::Error::from(e))
    }

    fn write_prim_array_type(
        &mut self,
        obj_id: Id,
        prim_array_type: PrimitiveArrayType,
    ) -> Result<(), anyhow::Error> {
        self.obj_prim_array_type_chunk_writer
            .append((obj_id.id(), prim_array_type.type_code()))
            .map_err(|e| anyhow::Error::from(e))
    }

    fn flush(mut self) -> Result<(), anyhow::Error> {
        self.obj_class_chunk_writer.flush()?;
        self.obj_prim_array_type_chunk_writer.flush()?;

        Ok(())
    }
}

/// Writes sorted chunks of a data stream so that they can be later merge-sorted into a unified,
/// globally sorted iteration.
struct SortedChunkWriter<F, T, S>
where
    // builds per-chunk writers
    F: ChunkWriterFactory,
    S: DatumSerializer<T>,
{
    data: Vec<T>,
    chunk_size: usize,
    record_index: usize,
    chunk_index: usize,
    writer_factory: F,
    phantom: marker::PhantomData<S>,
}

impl<F: ChunkWriterFactory, T, S: DatumSerializer<T>> SortedChunkWriter<F, T, S> {
    fn new(
        record_index: usize,
        chunk_size: usize,
        writer_factory: F,
    ) -> SortedChunkWriter<F, T, S> {
        SortedChunkWriter {
            data: Vec::new(),
            chunk_size,
            record_index,
            chunk_index: 0,
            writer_factory,
            phantom: marker::PhantomData,
        }
    }

    /// Append a datum to the internal buffer. If the buffer reaches the chunk size, it will be
    /// flushed.
    fn append(&mut self, datum: T) -> Result<(), io::Error> {
        self.data.push(datum);

        if self.data.len() == self.chunk_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Write the sorted current contents of the buffer.
    /// Must be called to ensure any leftovers that weren't auto-flushed get written.
    fn flush(&mut self) -> Result<(), io::Error> {
        if self.data.is_empty() {
            return Ok(());
        }

        self.data
            .sort_unstable_by_key(|datum| S::extract_key(datum));

        let mut writer = self
            .writer_factory
            .chunk_writer(self.record_index, self.chunk_index)?;

        for datum in self.data.iter() {
            S::serialize(datum, &mut writer)?;
        }
        writer.flush()?;

        self.chunk_index += 1;
        self.data.clear();

        Ok(())
    }
}

/// The boring details of how a particular data type that we might write in chunks is to be encoded
pub(crate) trait DatumSerializer<T> {
    type SortKey: Ord;

    /// The key that should be used to sort data inside a chunk
    fn extract_key(datum: &T) -> Self::SortKey;

    /// Serialize and write a datum to the writer
    fn serialize<W: io::Write>(datum: &T, writer: &mut W) -> Result<(), io::Error>;
}

pub(crate) trait DatumDeserializer<T> {
    /// Deserialize a datum from the reader, returning None if there is no data left to read
    fn deserialize<R: io::Read>(reader: &mut R) -> Option<Result<T, io::Error>>;
}

/// For (u64, u64)
pub(crate) struct U64PairData;

impl DatumSerializer<(u64, u64)> for U64PairData {
    type SortKey = u64;

    fn extract_key(datum: &(u64, u64)) -> Self::SortKey {
        datum.0
    }

    fn serialize<W: io::Write>(datum: &(u64, u64), writer: &mut W) -> Result<(), io::Error> {
        writer
            .write_all(&datum.0.to_le_bytes())
            .and_then(|_| writer.write_all(&datum.1.to_le_bytes()))
    }
}

impl DatumDeserializer<(u64, u64)> for U64PairData {
    fn deserialize<R: io::Read>(reader: &mut R) -> Option<Result<(u64, u64), io::Error>> {
        let mut buf = [0_u8; 16];
        match reader.read_exact(&mut buf[..]) {
            Ok(_) => { /* no op */ }
            Err(e) => {
                return match e.kind() {
                    // TODO error if there are leftover bytes
                    io::ErrorKind::UnexpectedEof => None,
                    _ => Some(Err(e)),
                };
            }
        }
        let key = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let value = u64::from_le_bytes(buf[8..].try_into().unwrap());

        Some(Ok((key, value)))
    }
}

/// For (u64, u8) as used for primitive array types, which use a u8 type code
pub(crate) struct U64U8Data;

impl DatumSerializer<(u64, u8)> for U64U8Data {
    type SortKey = u64;

    fn extract_key(datum: &(u64, u8)) -> Self::SortKey {
        datum.0
    }

    fn serialize<W: io::Write>(datum: &(u64, u8), writer: &mut W) -> Result<(), io::Error> {
        writer
            .write_all(&datum.0.to_le_bytes())
            .and_then(|_| writer.write_all(&datum.1.to_le_bytes()))
    }
}

impl DatumDeserializer<(u64, u8)> for U64U8Data {
    fn deserialize<R: io::Read>(reader: &mut R) -> Option<Result<(u64, u8), io::Error>> {
        let mut buf = [0_u8; 9];
        match reader.read_exact(&mut buf[..]) {
            Ok(_) => { /* no op */ }
            Err(e) => {
                return match e.kind() {
                    // TODO error if there are leftover bytes
                    io::ErrorKind::UnexpectedEof => None,
                    _ => Some(Err(e)),
                };
            }
        }
        let key = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let value = buf[8];

        Some(Ok((key, value)))
    }
}

pub(crate) trait ChunkWriterFactory {
    type Writer: io::Write;

    /// Return an underlying writer to be used for a chunk
    fn chunk_writer(
        &mut self,
        record_index: usize,
        chunk_index: usize,
    ) -> Result<Self::Writer, io::Error>;
}

// Implement ChunkWriterFactory for &mut ChunkWriterFactory for convenience in the tests below
impl<F: ChunkWriterFactory> ChunkWriterFactory for &mut F {
    type Writer = F::Writer;

    fn chunk_writer(
        &mut self,
        record_index: usize,
        chunk_index: usize,
    ) -> Result<Self::Writer, io::Error> {
        (**self).chunk_writer(record_index, chunk_index)
    }
}

/// A ChunkWriterFactory that puts each chunk into its own file in a directory
pub(crate) struct DirWriterFactory {
    dest: path::PathBuf,
}

impl ChunkWriterFactory for DirWriterFactory {
    type Writer = io::BufWriter<fs::File>;

    fn chunk_writer(
        &mut self,
        record_index: usize,
        chunk_index: usize,
    ) -> Result<Self::Writer, io::Error> {
        let mut path = self.dest.to_owned();
        path.push(format!(
            "record-{:010}-chunk-{:03}",
            record_index, chunk_index
        ));

        Ok(io::BufWriter::new(fs::File::create(path)?))
    }
}

/// Iterate over chunk data
pub(crate) struct ChunkDatumIterator<R: io::Read, T, D: DatumDeserializer<T>> {
    reader: R,
    done: bool,
    phantom_t: marker::PhantomData<T>,
    phantom_d: marker::PhantomData<D>,
}

impl<R: io::Read, T, D: DatumDeserializer<T>> ChunkDatumIterator<R, T, D> {
    pub(crate) fn new(reader: R) -> ChunkDatumIterator<R, T, D> {
        ChunkDatumIterator {
            reader,
            done: false,
            phantom_t: marker::PhantomData,
            phantom_d: marker::PhantomData,
        }
    }
}

impl<R: io::Read, T, D: DatumDeserializer<T>> Iterator for ChunkDatumIterator<R, T, D> {
    type Item = Result<T, io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match D::deserialize(&mut self.reader) {
            None => {
                self.done = true;
                None
            }
            Some(r) => Some(r),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkWriterFactory, SortedChunkWriter, U64PairData};
    use crate::index::index_chunks::ChunkDatumIterator;
    use anyhow;
    use itertools::Itertools;
    use std::convert::TryInto;
    use std::{cell, io, rc};

    #[test]
    fn flush_no_writes_with_no_data() -> Result<(), anyhow::Error> {
        let mut stub_factory = StubChunkWriterFactory::new();
        let mut chunk_writer = stub_pair_writer(&mut stub_factory, 100);

        chunk_writer.flush()?;

        assert_eq!(0, stub_factory.cells.len());

        Ok(())
    }

    #[test]
    fn write_one_partial_chunk_with_flush() -> Result<(), anyhow::Error> {
        let mut stub_factory = StubChunkWriterFactory::new();
        let mut chunk_writer = stub_pair_writer(&mut stub_factory, 100);

        chunk_writer.append((1, 100))?;
        chunk_writer.append((3, 300))?;
        chunk_writer.append((2, 200))?;

        chunk_writer.flush()?;

        assert_eq!(1, stub_factory.cells.len());

        let (record_index, chunk_index, data) = &stub_factory.cells[0];
        assert_eq!(42, *record_index);
        assert_eq!(0, *chunk_index);
        assert_eq!(
            vec![1_u64, 100, 2, 200, 3, 300],
            data.borrow()
                .as_slice()
                .chunks(8)
                .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("slice is of size 8")))
                .collect::<Vec<u64>>()
        );

        Ok(())
    }

    #[test]
    fn write_one_full_chunk_then_one_partial_chunk_with_flush() -> Result<(), anyhow::Error> {
        let mut stub_factory = StubChunkWriterFactory::new();
        let mut chunk_writer = stub_pair_writer(&mut stub_factory, 2);

        chunk_writer.append((1, 100))?;
        chunk_writer.append((2, 200))?;
        // should have flushed at 2, so this should be in its own chunk
        chunk_writer.append((3, 300))?;

        chunk_writer.flush()?;

        assert_eq!(2, stub_factory.cells.len());

        {
            let (record_index, chunk_index, data) = &stub_factory.cells[0];
            assert_eq!(42, *record_index);
            assert_eq!(0, *chunk_index);
            assert_eq!(vec![1_u64, 100, 2, 200], to_u64s(data.borrow().as_slice()));
        }

        {
            let (record_index, chunk_index, data) = &stub_factory.cells[1];
            assert_eq!(42, *record_index);
            assert_eq!(1, *chunk_index);
            assert_eq!(vec![3_u64, 300], to_u64s(data.borrow().as_slice()));
        }

        Ok(())
    }

    #[test]
    fn write_chunks_then_read_them() -> Result<(), anyhow::Error> {
        let mut stub_factory = StubChunkWriterFactory::new();
        let mut chunk_writer = stub_pair_writer(&mut stub_factory, 100);

        chunk_writer.append((1, 100))?;
        chunk_writer.append((3, 300))?;
        chunk_writer.append((2, 200))?;

        chunk_writer.flush()?;

        assert_eq!(1, stub_factory.cells.len());

        let (_, _, data) = &stub_factory.cells[0];

        let borrowed_data = data.borrow();
        let cursor = io::Cursor::new(borrowed_data.as_slice());

        let iterator = ChunkDatumIterator::<_, _, U64PairData>::new(cursor);

        let items = iterator.map(|r| r.unwrap()).collect_vec();

        assert_eq!(vec![(1, 100), (2, 200), (3, 300)], items);

        Ok(())
    }

    fn stub_pair_writer(
        stub_factory: &mut StubChunkWriterFactory,
        chunk_size: usize,
    ) -> SortedChunkWriter<&mut StubChunkWriterFactory, (u64, u64), U64PairData> {
        SortedChunkWriter::new(42, chunk_size, stub_factory)
    }

    fn to_u64s(slice: &[u8]) -> Vec<u64> {
        slice
            .chunks(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("slice is of size 8")))
            .collect::<Vec<u64>>()
    }

    struct StubChunkWriterFactory {
        cells: Vec<(usize, usize, rc::Rc<cell::RefCell<Vec<u8>>>)>,
    }

    impl StubChunkWriterFactory {
        fn new() -> StubChunkWriterFactory {
            StubChunkWriterFactory { cells: Vec::new() }
        }
    }

    impl ChunkWriterFactory for StubChunkWriterFactory {
        type Writer = CellWriter;

        fn chunk_writer(
            &mut self,
            record_index: usize,
            chunk_index: usize,
        ) -> Result<CellWriter, io::Error> {
            let cell = rc::Rc::new(cell::RefCell::new(Vec::new()));
            self.cells.push((record_index, chunk_index, cell.clone()));
            Ok(CellWriter { cell })
        }
    }

    struct CellWriter {
        cell: rc::Rc<cell::RefCell<Vec<u8>>>,
    }

    impl io::Write for CellWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut borrowed_vec = self.cell.borrow_mut();
            borrowed_vec.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
