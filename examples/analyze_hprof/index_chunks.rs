use super::*;
use std::{fs, io, marker, path};

/// An IndexBuilder that delegates to ChunkedrecordWriter for the actual work
pub(crate) struct ChunkedIndexBuilder {
    fingerprint: HprofFingerprint,
    dest: path::PathBuf,
}

impl IndexBuilder for ChunkedIndexBuilder {
    type RecWriter = ChunkedRecordWriter<DirWriterFactory, U64PairHandler, U64PrimTypeHandler>;

    fn new_with_fingerprint(
        fingerprint: HprofFingerprint,
        dest: path::PathBuf,
    ) -> Result<Self, anyhow::Error> {
        Ok(ChunkedIndexBuilder { fingerprint, dest })
    }

    fn record_writer(&self, record_index: usize) -> Result<Self::RecWriter, anyhow::Error> {
        let mut class_dest = self.dest.clone();
        class_dest.push("chunks");
        class_dest.push("obj-id-class-id");
        fs::create_dir_all(&class_dest)?;
        let class_chunk_factory = DirWriterFactory { dest: class_dest };

        let mut prim_type_dest = self.dest.clone();
        prim_type_dest.push("chunks");
        prim_type_dest.push("obj-id-prim-array-type");
        fs::create_dir_all(&prim_type_dest)?;
        let prim_array_chunk_factory = DirWriterFactory {
            dest: prim_type_dest,
        };

        Ok(ChunkedRecordWriter {
            obj_class_chunk_writer: SortedChunkWriter::new(
                record_index,
                // 8M * 16 = 128MiB chunks
                8_000_000,
                class_chunk_factory,
                U64PairHandler,
            ),
            obj_prim_array_type_chunk_writer: SortedChunkWriter::new(
                record_index,
                // 96MiB chunks
                8_000_000,
                prim_array_chunk_factory,
                U64PrimTypeHandler,
            ),
        })
    }

    fn finalize(&self) {
        // TODO merge sort into lmdb
    }
}

/// Write per-Record data into sorted chunks
pub(crate) struct ChunkedRecordWriter<F, D1, D2>
where
    F: ChunkWriterFactory,
    // obj id -> class id
    D1: DatumHandler<(u64, u64), F::Writer>,
    // obj id -> prim type
    D2: DatumHandler<(u64, u8), F::Writer>,
{
    obj_class_chunk_writer: SortedChunkWriter<F, (u64, u64), D1>,
    obj_prim_array_type_chunk_writer: SortedChunkWriter<F, (u64, u8), D2>,
}

impl<F, D1, D2> RecordWriter for ChunkedRecordWriter<F, D1, D2>
where
    F: ChunkWriterFactory,
    D1: DatumHandler<(u64, u64), F::Writer>,
    D2: DatumHandler<(u64, u8), F::Writer>,
{
    fn insert_class_id(&mut self, obj_id: Id, class_id: Id) -> Result<(), anyhow::Error> {
        self.obj_class_chunk_writer
            .append((obj_id.id(), class_id.id()))
            .map_err(|e| anyhow::Error::from(e))
    }

    fn insert_prim_array_type(
        &mut self,
        obj_id: Id,
        prim_array_type: PrimitiveArrayType,
    ) -> Result<(), anyhow::Error> {
        self.obj_prim_array_type_chunk_writer
            .append((obj_id.id(), prim_array_type.type_code()))
            .map_err(|e| anyhow::Error::from(e))
    }

    fn flush(&mut self) -> Result<(), anyhow::Error> {
        self.obj_class_chunk_writer.flush()?;
        self.obj_prim_array_type_chunk_writer.flush()?;

        Ok(())
    }
}

/// Writes sorted chunks of a data stream so that they can be later merge-sorted into a unified,
/// globally sorted iteration.
struct SortedChunkWriter<F, T, D>
where
    // builds per-chunk writers
    F: ChunkWriterFactory,
    D: DatumHandler<T, F::Writer>,
{
    data: Vec<T>,
    chunk_size: usize,
    record_index: usize,
    chunk_index: usize,
    writer_factory: F,
    phantom: marker::PhantomData<D>,
}

impl<F: ChunkWriterFactory, T, D: DatumHandler<T, F::Writer>> SortedChunkWriter<F, T, D> {
    fn new(
        record_index: usize,
        chunk_size: usize,
        writer_factory: F,
        // so that you can pass the type you want to use without having to specify the whole
        // generic signature
        // TODO just use instance methods?
        _handler: D,
    ) -> SortedChunkWriter<F, T, D> {
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
            .sort_unstable_by_key(|datum| D::extract_key(datum));

        let mut writer = self
            .writer_factory
            .chunk_writer(self.record_index, self.chunk_index)?;

        for datum in self.data.iter() {
            D::encode_and_write(datum, &mut writer)?;
        }
        writer.flush()?;

        self.chunk_index += 1;
        self.data.clear();

        Ok(())
    }
}

/// The boring details of how a particular data type that we might write in chunks is to be encoded
pub(crate) trait DatumHandler<T, W: io::Write> {
    type SortKey: Ord;

    fn extract_key(datum: &T) -> Self::SortKey;

    fn encode_and_write(datum: &T, writer: &mut W) -> Result<(), io::Error>;
}

pub(crate) struct U64PairHandler;

impl<W: io::Write> DatumHandler<(u64, u64), W> for U64PairHandler {
    type SortKey = u64;

    fn extract_key(datum: &(u64, u64)) -> Self::SortKey {
        datum.0
    }

    fn encode_and_write(datum: &(u64, u64), writer: &mut W) -> Result<(), io::Error> {
        writer
            .write_all(&datum.0.to_le_bytes())
            .and_then(|_| writer.write_all(&datum.1.to_le_bytes()))
    }
}

pub(crate) struct U64PrimTypeHandler;

impl<W: io::Write> DatumHandler<(u64, u8), W> for U64PrimTypeHandler {
    type SortKey = u64;

    fn extract_key(datum: &(u64, u8)) -> Self::SortKey {
        datum.0
    }

    fn encode_and_write(datum: &(u64, u8), writer: &mut W) -> Result<(), io::Error> {
        writer
            .write_all(&datum.0.to_le_bytes())
            .and_then(|_| writer.write_all(&datum.1.to_le_bytes()))
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
    ) -> Result<Self::Writer, Error> {
        let mut path = self.dest.to_owned();
        path.push(format!("record-{}-chunk-{}", record_index, chunk_index));

        Ok(io::BufWriter::new(fs::File::create(path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkWriterFactory, SortedChunkWriter, U64PairHandler};
    use anyhow;
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

    fn stub_pair_writer(
        stub_factory: &mut StubChunkWriterFactory,
        chunk_size: usize,
    ) -> SortedChunkWriter<&mut StubChunkWriterFactory, (u64, u64), U64PairHandler> {
        SortedChunkWriter::new(42, chunk_size, stub_factory, U64PairHandler)
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
