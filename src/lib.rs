use getset::{CopyGetters, Getters};
use nom::bytes::complete as bytes;
use nom::number::complete as number;
use std::cmp::Ordering;
use std::fmt::{Error, Formatter};
use std::{cmp, fmt, hash};

mod heap_dump;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct Id {
    // inflate 4-byte ids to 8-byte since if we have a small 32-bit heap, no worries about memory anyway
    id: u64,
}

impl Id {
    fn parse(input: &[u8], size: IdSize) -> nom::IResult<&[u8], Id> {
        let (input, id) = match size {
            IdSize::U32 => number::be_u32(input).map(|(i, id)| (i, id as u64))?,
            IdSize::U64 => number::be_u64(input)?,
        };

        Ok((input, Id { id }))
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.id)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IdSize {
    U32,
    U64,
}

// https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp

#[derive(CopyGetters)]
pub struct Hprof<'a> {
    #[get_copy = "pub"]
    header: Header<'a>,
    records: &'a [u8],
}

impl<'a> Hprof<'a> {
    pub fn records_iter<'i>(&self) -> RecordIterator<'i>
    where
        'a: 'i,
    {
        RecordIterator {
            remaining: self.records,
            id_size: self.header.id_size,
        }
    }
}

#[derive(CopyGetters, Copy, Clone)]
pub struct Header<'a> {
    label: &'a [u8],
    #[get_copy = "pub"]
    id_size: IdSize,
    /// The timestamp for the hprof as the number of millis since epoch
    #[get_copy = "pub"]
    timestamp_millis: u64,
}

impl<'a> Header<'a> {
    pub fn label(&self) -> Result<&'a str, std::str::Utf8Error> {
        std::str::from_utf8(self.label)
    }

    fn parse(input: &[u8]) -> nom::IResult<&[u8], Header> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L63
        let (input, label) = bytes::take_until(&b"\0"[..])(input)?;
        let (input, _) = bytes::take_while_m_n(1, 1, |b| b == 0)(input)?;

        // TODO confirm endianness
        let (input, id_size_num) = number::be_u32(input)?;
        let (input, epoch_hi) = number::be_u32(input)?;
        let (input, epoch_lo) = number::be_u32(input)?;

        let epoch_timestamp = ((epoch_hi as u64) << 32) + (epoch_lo as u64);

        let id_size = match id_size_num {
            4 => IdSize::U32,
            8 => IdSize::U64,
            _ => panic!("unexpected size {}", id_size_num), // TODO
        };

        Ok((
            input,
            Header {
                label,
                id_size,
                timestamp_millis: epoch_timestamp,
            },
        ))
    }
}

impl<'a> fmt::Debug for Header<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_struct("Header")
            .field("label", &self.label())
            .field("timestamp_millis", &self.timestamp_millis())
            .field("id_size", &self.id_size())
            .finish()
    }
}

pub struct RecordIterator<'a> {
    remaining: &'a [u8],
    id_size: IdSize,
}

type ParseResult<'e, T> = Result<T, nom::Err<(&'e [u8], nom::error::ErrorKind)>>;

impl<'a> Iterator for RecordIterator<'a> {
    type Item = ParseResult<'a, Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }

        let res = Record::parse(self.remaining, self.id_size);
        match res {
            Ok((input, record)) => {
                self.remaining = input;
                Some(Ok(record))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(CopyGetters, Copy, Clone)]
pub struct Record<'a> {
    #[get_copy = "pub"]
    tag: RecordTag,
    #[get_copy = "pub"]
    micros_since_header_ts: u32,
    id_size: IdSize,
    body: &'a [u8],
}

impl<'a> Record<'a> {
    pub fn as_utf_8(&self) -> Option<ParseResult<Utf8<'a>>> {
        match self.tag {
            RecordTag::Utf8 => Some(Utf8::parse(self.body, self.id_size)),
            _ => None,
        }
    }

    pub fn as_load_class(&self) -> Option<ParseResult<LoadClass>> {
        match self.tag {
            RecordTag::LoadClass => Some(LoadClass::parse(self.body, self.id_size)),
            _ => None,
        }
    }

    fn parse<'i: 'r, 'r>(input: &'i [u8], id_size: IdSize) -> nom::IResult<&'i [u8], Record<'r>> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L76
        let (input, tag_byte) = bytes::take(1_usize)(input)?;

        let tag = match tag_byte[0] {
            0x01 => RecordTag::Utf8,
            0x02 => RecordTag::LoadClass,
            0x03 => RecordTag::UnloadClass,
            0x04 => RecordTag::StackFrame,
            0x05 => RecordTag::Trace,
            0x06 => RecordTag::AllocSites,
            0x07 => RecordTag::HeapSummary,
            0x0A => RecordTag::StartThread,
            0x0B => RecordTag::EndThread,
            0x0C => RecordTag::HeapDump,
            0x0D => RecordTag::CpuSamples,
            0x0E => RecordTag::ControlSettings,
            0x1C => RecordTag::HeapDumpSegment,
            0x2C => RecordTag::HeapDumpEnd,
            _ => panic!("unexpected tag"),
        };

        let (input, micros) = number::be_u32(input)?;
        let (input, len) = number::be_u32(input)?;
        let (input, body) = bytes::take(len)(input)?;

        Ok((
            input,
            Record {
                tag,
                micros_since_header_ts: micros,
                id_size,
                body,
            },
        ))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd)]
pub enum RecordTag {
    Utf8,
    LoadClass,
    UnloadClass,
    StackFrame,
    Trace,
    AllocSites,
    StartThread,
    EndThread,
    HeapSummary,
    HeapDump,
    CpuSamples,
    ControlSettings,
    HeapDumpSegment,
    HeapDumpEnd,
}

impl RecordTag {
    fn tag_byte(&self) -> u8 {
        match self {
            RecordTag::Utf8 => 0x01,
            RecordTag::LoadClass => 0x02,
            RecordTag::UnloadClass => 0x03,
            RecordTag::StackFrame => 0x04,
            RecordTag::Trace => 0x05,
            RecordTag::AllocSites => 0x06,
            RecordTag::HeapSummary => 0x07,
            RecordTag::StartThread => 0x0A,
            RecordTag::EndThread => 0x0B,
            RecordTag::HeapDump => 0x0C,
            RecordTag::CpuSamples => 0x0D,
            RecordTag::ControlSettings => 0x0E,
            RecordTag::HeapDumpSegment => 0x1C,
            RecordTag::HeapDumpEnd => 0x2C,
        }
    }
}

impl cmp::Ord for RecordTag {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag_byte().cmp(&other.tag_byte())
    }
}

#[derive(CopyGetters, Copy, Clone)]
pub struct Utf8<'a> {
    #[get_copy = "pub"]
    name_id: Id,
    #[get_copy = "pub"]
    text: &'a [u8],
}

impl<'a> Utf8<'a> {
    fn parse(input: &[u8], id_size: crate::IdSize) -> ParseResult<Utf8> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L88
        let (input, id) = Id::parse(input, id_size)?;

        Ok(Utf8 {
            name_id: id,
            text: input,
        })
    }

    /// Note that in practice, there are nonzero Utf8 records with invalid UTF-8 bytes.
    pub fn text_as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.text)
    }
}

#[derive(CopyGetters, Copy, Clone)]
pub struct LoadClass {
    #[get_copy = "pub"]
    class_serial: u32,
    #[get_copy = "pub"]
    class_obj_id: Id,
    #[get_copy = "pub"]
    stack_trace_serial: u32,
    #[get_copy = "pub"]
    class_name_id: Id,
}

impl LoadClass {
    fn parse(input: &[u8], id_size: crate::IdSize) -> ParseResult<LoadClass> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L93
        let (input, class_serial) = number::be_u32(input)?;
        let (input, class_obj_id) = Id::parse(input, id_size)?;
        let (input, stack_trace_serial) = number::be_u32(input)?;
        let (input, class_name_id) = Id::parse(input, id_size)?;

        Ok(LoadClass {
            class_serial,
            class_obj_id,
            stack_trace_serial,
            class_name_id,
        })
    }
}

struct UnloadClass {
    class_serial: u32,
}

struct StackFrame {
    id: Id,
    method_name_id: Id,
    method_signature_id: Id,
    source_file_name_id: Id,
    class_serial: u32,
    line_num: LineNum,
}

struct Trace {
    stack_trace_serial: u32,
    thread_serial: u32,
    // num_frames: u32,
    // TODO iterator over following stack frame ids
}

/// Heap allocation sites, obtained after GC
struct AllocSites {
    flags: AllocSitesFlags,
    cutoff_ratio: u32,
    total_live_bytes: u32,
    total_live_instances: u32,
    total_bytes_allocated: u64,
    total_instances_allocated: u64,
    // num_sites: u4
    // TODO iterator over following AllocSite instances
}

struct StartThread {
    thread_serial: u32,
    thread_id: Id,
    stack_trace_serial: u32,
    thread_name_id: Id,
    thread_group_name_id: Id,
    thread_group_parent_name_id: Id,
}

struct EndThread {
    thread_serial: u32,
}

struct HeapSummary {
    total_live_bytes: u32,
}

/// Represents either a HPROF_HEAP_DUMP or HPROF_HEAP_DUMP_SEGMENT
struct HeapDump {
    // TODO iterator over heap dump sub records
}

struct CpuSamples {
    num_samples: u32,
    num_traces: u32,
    // TODO iterator over samples
}

struct ControlSettings {
    bits: u32,
    stack_trace_depth: u16,
}

#[derive(Copy, Clone, Debug)]
enum LineNum {
    Normal(u32),
    Unknown,
    CompiledMethod,
    NativeMethod,
}

#[derive(Copy, Clone, Debug)]
struct AllocSitesFlags {
    bits: u16,
}

impl AllocSitesFlags {
    fn mode(&self) -> AllocSitesFlagsMode {
        // TODO naming, correctness?
        if self.bits & 0x001 > 0 {
            AllocSitesFlagsMode::Incremental
        } else {
            AllocSitesFlagsMode::Complete
        }
    }

    fn sorting(&self) -> AllocSitesFlagsSorting {
        // TODO
        if self.bits & 0x002 > 0 {
            AllocSitesFlagsSorting::Allocation
        } else {
            AllocSitesFlagsSorting::Live
        }
    }

    fn force_gc(&self) -> bool {
        self.bits & 0x0004 > 0
    }
}

enum AllocSitesFlagsMode {
    Incremental,
    Complete,
}

enum AllocSitesFlagsSorting {
    Allocation,
    Live,
}

enum ObjOrArrayType {
    Object,
    ObjectArray,
    BooleanArray,
    CharArray,
    FloatArray,
    DoubleArray,
    ByteArray,
    ShortArray,
    IntArray,
    LongArray,
}

impl ObjOrArrayType {
    fn from_num(num: u8) -> ObjOrArrayType {
        match num {
            0 => ObjOrArrayType::Object,
            2 => ObjOrArrayType::ObjectArray,
            4 => ObjOrArrayType::BooleanArray,
            5 => ObjOrArrayType::CharArray,
            6 => ObjOrArrayType::FloatArray,
            7 => ObjOrArrayType::DoubleArray,
            8 => ObjOrArrayType::ByteArray,
            9 => ObjOrArrayType::ShortArray,
            10 => ObjOrArrayType::IntArray,
            11 => ObjOrArrayType::LongArray,
            _ => panic!("Unknown type num {}", num),
        }
    }
}

struct AllocSite {
    is_array: ObjOrArrayType,
    /// May be zero during startup
    class_serial: u32,
    stack_trace_serial: u32,
    num_bytes_alive: u32,
    num_instances_alive: u32,
    num_bytes_allocated: u32,
    num_instances_allocated: u32,
}

pub fn parse_hprof(input: &[u8]) -> ParseResult<Hprof> {
    let (input, header) = Header::parse(input)?;

    Ok(Hprof {
        header,
        records: input,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
