use getset::CopyGetters;

use crate::*;

pub enum SubRecord<'a> {
    GcRootUnknown(GcRootUnknown),
    GcRootThreadObj(GcRootThreadObj),
    GcRootJniGlobal(GcRootJniGlobal),
    GcRootJniLocalRef(GcRootJniLocalRef),
    GcRootJavaStackFrame(GcRootJavaStackFrame),
    GcRootNativeStack(GcRootNativeStack),
    GcRootSystemClass(GcRootSystemClass),
    GcRootThreadBlock(GcRootThreadBlock),
    GcRootBusyMonitor(GcRootBusyMonitor),
    Class(Class<'a>),
    Instance(Instance<'a>),
    ObjectArray(ObjectArray<'a>),
    PrimitiveArray(PrimitiveArray<'a>),
}

impl<'a> fmt::Debug for SubRecord<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "{}",
            match self {
                SubRecord::GcRootUnknown(_) => "GcRootUnknown",
                SubRecord::GcRootThreadObj(_) => "GcRootThreadObj",
                SubRecord::GcRootJniGlobal(_) => "GcRootJniGlobal",
                SubRecord::GcRootJniLocalRef(_) => "GcRootJniLocalRef",
                SubRecord::GcRootJavaStackFrame(_) => "GcRootJavaStackFrame",
                SubRecord::GcRootNativeStack(_) => "GcRootNativeStack",
                SubRecord::GcRootSystemClass(_) => "GcRootSystemClass",
                SubRecord::GcRootThreadBlock(_) => "GcRootThreadBlock",
                SubRecord::GcRootBusyMonitor(_) => "GcRootBusyMonitor",
                SubRecord::Class(_) => "Class",
                SubRecord::Instance(_) => "Instance",
                SubRecord::ObjectArray(_) => "ObjectArray",
                SubRecord::PrimitiveArray(_) => "PrimitiveArray",
            }
        )
    }
}

impl<'a> SubRecord<'a> {
    pub fn as_gc_root_unknown(&self) -> Option<GcRootUnknown> {
        match self {
            SubRecord::GcRootUnknown(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_class(&self) -> Option<&Class> {
        match self {
            SubRecord::Class(v) => Some(v),
            _ => None,
        }
    }

    pub(crate) fn parse<'i: 'r, 'r>(
        input: &'i [u8],
        id_size: IdSize,
    ) -> nom::IResult<&'i [u8], SubRecord<'r>> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L178
        let (input, tag_byte) = number::be_u8(input)?;

        // have to parse now since ClassObject, etc, have variable size

        let (input, variant) = match tag_byte {
            0xFF => GcRootUnknown::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootUnknown(r))),
            0x08 => GcRootThreadObj::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootThreadObj(r))),
            0x01 => GcRootJniGlobal::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootJniGlobal(r))),
            0x02 => GcRootJniLocalRef::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootJniLocalRef(r))),
            0x03 => GcRootJavaStackFrame::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootJavaStackFrame(r))),
            0x04 => GcRootNativeStack::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootNativeStack(r))),
            0x05 => GcRootSystemClass::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootSystemClass(r))),
            0x06 => GcRootThreadBlock::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootThreadBlock(r))),
            0x07 => GcRootBusyMonitor::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::GcRootBusyMonitor(r))),
            0x20 => Class::parse(input, id_size).map(|(input, r)| (input, SubRecord::Class(r))),
            0x21 => {
                Instance::parse(input, id_size).map(|(input, r)| (input, SubRecord::Instance(r)))
            }
            0x22 => ObjectArray::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::ObjectArray(r))),
            0x23 => PrimitiveArray::parse(input, id_size)
                .map(|(input, r)| (input, SubRecord::PrimitiveArray(r))),
            _ => panic!("Unexpected sub-record type {:#X}", tag_byte), // TODO
        }?;

        Ok((input, variant))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootUnknown {
    #[get_copy = "pub"]
    obj_id: Id,
}

impl GcRootUnknown {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L180
        let (input, id) = Id::parse(input, id_size)?;

        Ok((input, GcRootUnknown { obj_id: id }))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootThreadObj {
    /// May be missing for a thread newly attached through JNI
    #[get_copy = "pub"]
    thread_obj_id: Option<Id>,
    #[get_copy = "pub"]
    thread_serial: Serial,
    #[get_copy = "pub"]
    stack_trace_serial: Serial,
}

impl GcRootThreadObj {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L184
        let (input, thread_obj_id) = parse_optional_id(input, id_size)?;
        let (input, thread_serial) = number::be_u32(input)?;
        let (input, stack_trace_serial) = number::be_u32(input)?;

        Ok((
            input,
            GcRootThreadObj {
                thread_obj_id,
                thread_serial,
                stack_trace_serial,
            },
        ))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootJniGlobal {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    jni_global_ref_id: Id,
}

impl GcRootJniGlobal {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L191
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, jni_global_ref_id) = Id::parse(input, id_size)?;

        Ok((
            input,
            GcRootJniGlobal {
                obj_id,
                jni_global_ref_id,
            },
        ))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootJniLocalRef {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    thread_serial: Serial,
    #[get_copy = "pub"]
    frame_index: Option<u32>,
}

impl GcRootJniLocalRef {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L196
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, thread_serial) = number::be_u32(input)?;
        let (input, frame_index) = parse_optional_serial(input)?;

        Ok((
            input,
            GcRootJniLocalRef {
                obj_id,
                thread_serial,
                frame_index,
            },
        ))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootJavaStackFrame {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    thread_serial: Serial,
    #[get_copy = "pub"]
    frame_index: Option<u32>,
}

impl GcRootJavaStackFrame {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L202
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, thread_serial) = number::be_u32(input)?;
        let (input, frame_index) = parse_optional_serial(input)?;

        Ok((
            input,
            GcRootJavaStackFrame {
                obj_id,
                thread_serial,
                frame_index,
            },
        ))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootNativeStack {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    thread_serial: Serial,
}

impl GcRootNativeStack {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L208
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, thread_serial) = number::be_u32(input)?;

        Ok((
            input,
            GcRootNativeStack {
                obj_id,
                thread_serial,
            },
        ))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootSystemClass {
    #[get_copy = "pub"]
    obj_id: Id,
}

impl GcRootSystemClass {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L213
        let (input, obj_id) = Id::parse(input, id_size)?;

        Ok((input, GcRootSystemClass { obj_id }))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootThreadBlock {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    thread_serial: Serial,
}

impl GcRootThreadBlock {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L217
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, thread_serial) = number::be_u32(input)?;

        Ok((
            input,
            GcRootThreadBlock {
                obj_id,
                thread_serial,
            },
        ))
    }
}

#[derive(CopyGetters, Copy, Clone, Debug)]
pub struct GcRootBusyMonitor {
    #[get_copy = "pub"]
    obj_id: Id,
}

impl GcRootBusyMonitor {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L222
        let (input, obj_id) = Id::parse(input, id_size)?;

        Ok((input, GcRootBusyMonitor { obj_id }))
    }
}

#[derive(CopyGetters)]
pub struct Class<'a> {
    id_size: IdSize,
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    stack_trace_serial: Serial,
    #[get_copy = "pub"]
    super_class_obj_id: Option<Id>,
    // TODO optional
    #[get_copy = "pub"]
    class_loader_obj_id: Option<Id>,
    // TODO optional
    #[get_copy = "pub"]
    signers_obj_id: Option<Id>,
    // TODO optional
    #[get_copy = "pub"]
    protection_domain_obj_id: Option<Id>,
    #[get_copy = "pub"]
    instance_size_bytes: u32,
    static_fields: Vec<FieldEntry>,
    num_instance_fields: u16,
    instance_fields: &'a [u8],
}

impl<'a> Class<'a> {
    pub fn static_fields(&self) -> &[FieldEntry] {
        self.static_fields.as_slice()
    }

    pub fn instance_fields(&self) -> FieldDescriptors {
        FieldDescriptors {
            iter: ParsingIteratorWithId {
                id_size: self.id_size,
                num_remaining: self.num_instance_fields as u32,
                remaining: self.instance_fields,
                phantom: marker::PhantomData,
            },
        }
    }

    fn parse<'i: 'r, 'r>(input: &'i [u8], id_size: IdSize) -> nom::IResult<&'i [u8], Class<'r>> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L226
        // dump_class_and_array_classes https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L995
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, stack_trace_serial) = number::be_u32(input)?;
        let (input, super_class_obj_id) = parse_optional_id(input, id_size)?;
        let (input, class_loader_obj_id) = parse_optional_id(input, id_size)?;
        let (input, signers_obj_id) = parse_optional_id(input, id_size)?;
        let (input, protection_domain_obj_id) = parse_optional_id(input, id_size)?;
        // 2x Id reserved
        let (input, _) = Id::parse(input, id_size)?;
        let (input, _) = Id::parse(input, id_size)?;
        let (input, instance_size_bytes) = number::be_u32(input)?;
        let (input, constant_pool_len) = number::be_u16(input)?;
        // constant pool len always 0 as per
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L1031
        // TODO parse failure
        assert_eq!(0, constant_pool_len);

        let (input, static_fields_len) = number::be_u16(input)?;

        // since we get a _number of fields_ not a length in bytes, we have to parse now :(
        // Fortunately, the number of classes << number of objects, so we only will have to do this
        // tens of thousands of times, not billions.
        let mut static_fields = Vec::with_capacity(static_fields_len as usize);

        // need to keep track of input outside the loop scope
        let mut input_after_static_fields = input;
        for _ in 0..static_fields_len {
            let (input, name_id) = Id::parse(input_after_static_fields, id_size)?;
            let (input, tag) = number::be_u8(input)?;

            // dump_field_value https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L769
            // tags https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L709
            let (input, value) = match tag {
                0x01 | 0x02 => {
                    Id::parse(input, id_size).map(|(input, id)| (input, FieldValue::ObjectId(id)))
                }
                0x04 => number::be_u8(input).map(|(input, b)| (input, FieldValue::Boolean(b != 0))),
                0x05 => number::be_u16(input).map(|(input, c)| (input, FieldValue::Char(c))),
                0x06 => number::be_f32(input).map(|(input, f)| (input, FieldValue::Float(f))),
                0x07 => number::be_f64(input).map(|(input, f)| (input, FieldValue::Double(f))),
                0x08 => number::be_i8(input).map(|(input, b)| (input, FieldValue::Byte(b))),
                0x09 => number::be_i16(input).map(|(input, s)| (input, FieldValue::Short(s))),
                0x0A => number::be_i32(input).map(|(input, i)| (input, FieldValue::Int(i))),
                0x0B => number::be_i64(input).map(|(input, l)| (input, FieldValue::Long(l))),
                _ => panic!("Unexpected field value type {:#X}", tag), // TODO
            }?;
            static_fields.push(FieldEntry { name_id, value });
            input_after_static_fields = input;
        }

        // instance field descriptors https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L964
        let (input, num_instance_fields) = number::be_u16(input_after_static_fields)?;

        // descriptors are a (name id, tag) pair, so we don't have to parse now
        let instance_fields_byte_len = num_instance_fields as usize * (id_size.size_in_bytes() + 1);

        let (input, instance_fields) = bytes::take(instance_fields_byte_len)(input)?;

        Ok((
            input,
            Class {
                id_size,
                obj_id,
                stack_trace_serial,
                super_class_obj_id,
                class_loader_obj_id,
                signers_obj_id,
                protection_domain_obj_id,
                instance_size_bytes,
                static_fields,
                num_instance_fields,
                instance_fields,
            },
        ))
    }
}

pub struct FieldDescriptors<'a> {
    iter: ParsingIteratorWithId<'a, FieldDescriptor>,
}

impl<'a> Iterator for FieldDescriptors<'a> {
    type Item = ParseResult<'a, FieldDescriptor>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(CopyGetters)]
pub struct Instance<'a> {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    stack_trace_serial: Serial,
    #[get_copy = "pub"]
    class_obj_id: Id,
    fields: &'a [u8],
}

impl<'a> Instance<'a> {
    fn parse<'i: 'r, 'r>(input: &'i [u8], id_size: IdSize) -> nom::IResult<&'i [u8], Instance<'r>> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L262
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, stack_trace_serial) = number::be_u32(input)?;
        let (input, class_obj_id) = Id::parse(input, id_size)?;
        let (input, fields_byte_len) = number::be_u32(input)?;
        let (input, fields) = bytes::take(fields_byte_len)(input)?;

        Ok((
            input,
            Instance {
                obj_id,
                stack_trace_serial,
                class_obj_id,
                fields,
            },
        ))
    }
}

#[derive(CopyGetters)]
pub struct ObjectArray<'a> {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    stack_trace_serial: Serial,
    #[get_copy = "pub"]
    array_class_id: Id,
    contents: &'a [u8],
}

impl<'a> ObjectArray<'a> {
    fn parse<'i: 'r, 'r>(
        input: &'i [u8],
        id_size: IdSize,
    ) -> nom::IResult<&'i [u8], ObjectArray<'r>> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L271
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, stack_trace_serial) = number::be_u32(input)?;
        let (input, num_elements) = number::be_u32(input)?;
        let (input, array_class_id) = Id::parse(input, id_size)?;

        let id_bytes_len = num_elements as usize * id_size.size_in_bytes();

        let (input, contents) = bytes::take(id_bytes_len)(input)?;

        Ok((
            input,
            ObjectArray {
                obj_id,
                stack_trace_serial,
                array_class_id,
                contents,
            },
        ))
    }
}

#[derive(CopyGetters)]
pub struct PrimitiveArray<'a> {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    stack_trace_serial: Serial,
    array_type: PrimitiveArrayType,
    contents: &'a [u8], // TODO iterate over primitives
}

impl<'a> PrimitiveArray<'a> {
    fn parse<'i: 'r, 'r>(
        input: &'i [u8],
        id_size: IdSize,
    ) -> nom::IResult<&'i [u8], PrimitiveArray<'r>> {
        // https://github.com/openjdk/jdk/blob/08822b4e0526fe001c39fe08e241b849eddf481d/src/hotspot/share/services/heapDumper.cpp#L279
        let (input, obj_id) = Id::parse(input, id_size)?;
        let (input, stack_trace_serial) = number::be_u32(input)?;
        let (input, num_elements) = number::be_u32(input)?;
        let (input, type_byte) = number::be_u8(input)?;

        let (array_type, size) = match type_byte {
            0x04 => (PrimitiveArrayType::Boolean, 1),
            0x05 => (PrimitiveArrayType::Char, 2),
            0x06 => (PrimitiveArrayType::Float, 4),
            0x07 => (PrimitiveArrayType::Double, 8),
            0x08 => (PrimitiveArrayType::Byte, 1),
            0x09 => (PrimitiveArrayType::Short, 2),
            0x0A => (PrimitiveArrayType::Int, 4),
            0x0B => (PrimitiveArrayType::Long, 8),
            _ => panic!("Unexpected primitive array type {:#X}", type_byte), // TODO
        };

        let (input, contents) = bytes::take(num_elements * size)(input)?;

        Ok((
            input,
            PrimitiveArray {
                obj_id,
                stack_trace_serial,
                array_type,
                contents,
            },
        ))
    }
}

enum PrimitiveArrayType {
    Boolean,
    Char,
    Float,
    Double,
    Byte,
    Short,
    Int,
    Long,
}

enum ConstantPoolEntry {}

#[derive(CopyGetters, Clone, Copy, Debug)]
pub struct FieldEntry {
    #[get_copy = "pub"]
    name_id: Id,
    #[get_copy = "pub"]
    value: FieldValue,
}

#[derive(Clone, Copy, Debug)]
pub enum FieldValue {
    ObjectId(Id),
    Boolean(bool),
    Char(u16),
    Float(f32),
    Double(f64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
}

#[derive(CopyGetters, Clone, Copy, Debug)]
pub struct FieldDescriptor {
    #[get_copy = "pub"]
    name_id: Id,
    #[get_copy = "pub"]
    field_type: FieldType,
}

impl ParsableWithId for FieldDescriptor {
    fn parse(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Self> {
        let (input, name_id) = Id::parse(input, id_size)?;
        let (input, type_byte) = number::be_u8(input)?;

        let field_type = match type_byte {
            0x02 => FieldType::Object,
            0x04 => FieldType::Boolean,
            0x05 => FieldType::Char,
            0x06 => FieldType::Float,
            0x07 => FieldType::Double,
            0x08 => FieldType::Byte,
            0x09 => FieldType::Short,
            0x0A => FieldType::Int,
            0x0B => FieldType::Long,
            _ => panic!("Unexpected field type {:#X}", type_byte), // TODO
        };

        Ok((
            input,
            FieldDescriptor {
                name_id,
                field_type,
            },
        ))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FieldType {
    Object,
    Boolean,
    Char,
    Float,
    Double,
    Byte,
    Short,
    Int,
    Long,
}

fn parse_optional_id(input: &[u8], id_size: IdSize) -> nom::IResult<&[u8], Option<Id>> {
    Id::parse(input, id_size).map(|(input, id)| {
        if id.id == 0 {
            (input, None)
        } else {
            (input, Some(id))
        }
    })
}

fn parse_optional_serial(input: &[u8]) -> nom::IResult<&[u8], Option<Serial>> {
    number::be_u32(input).map(|(input, index)| {
        if index == u32::max_value() {
            (input, None)
        } else {
            (input, Some(index))
        }
    })
}
