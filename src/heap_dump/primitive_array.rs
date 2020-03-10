use crate::*;

/// Methods for accessing the contents of the array always return Some for method matching the array type and None otherwise (e.g. if it's a PrimitiveArrayType::Float, floats() will always be Some.
#[derive(CopyGetters)]
pub struct PrimitiveArray<'a> {
    #[get_copy = "pub"]
    obj_id: Id,
    #[get_copy = "pub"]
    stack_trace_serial: Serial,
    #[get_copy = "pub"]
    primitive_type: PrimitiveArrayType,
    num_elements: u32,
    contents: &'a [u8],
}

macro_rules! iterator_method {
    ($method_name:tt, $type_variant:tt, $iter_struct:tt) => {
        pub fn $method_name(&self) -> Option<$iter_struct> {
            match self.primitive_type {
                PrimitiveArrayType::$type_variant => Some($iter_struct {
                    iter: ParsingIterator::new_stateless(self.contents, self.num_elements),
                }),
                _ => None,
            }
        }
    };
}

impl<'a> PrimitiveArray<'a> {
    pub(crate) fn parse<'i: 'r, 'r>(
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
            _ => panic!("Unexpected primitive array type {:#X}", type_byte),
        };

        let (input, contents) = bytes::take(num_elements * size)(input)?;

        Ok((
            input,
            PrimitiveArray {
                obj_id,
                stack_trace_serial,
                primitive_type: array_type,
                num_elements,
                contents,
            },
        ))
    }

    iterator_method!(booleans, Boolean, Booleans);
    iterator_method!(chars, Char, Chars);
    iterator_method!(floats, Float, Floats);
    iterator_method!(doubles, Double, Doubles);
    iterator_method!(bytes, Byte, Bytes);
    iterator_method!(shorts, Short, Shorts);
    iterator_method!(ints, Int, Ints);
    iterator_method!(longs, Long, Longs);
}

impl StatelessParser for bool {
    fn parse(input: &[u8]) -> nom::IResult<&[u8], bool> {
        number::be_u8(input).map(|(input, b)| (input, b != 0))
    }
}

macro_rules! parser_impl {
    ($prim_type:tt, $parser_method:tt) => {
        impl StatelessParser for $prim_type {
            fn parse(input: &[u8]) -> nom::IResult<&[u8], $prim_type> {
                number::$parser_method(input).map(|(input, c)| (input, c))
            }
        }
    };
}

parser_impl!(u16, be_u16);
parser_impl!(f32, be_f32);
parser_impl!(f64, be_f64);
parser_impl!(i8, be_i8);
parser_impl!(i16, be_i16);
parser_impl!(i32, be_i32);
parser_impl!(i64, be_i64);

macro_rules! iter_struct {
    ($struct_name:ident, $item_type:ty) => {
        pub struct $struct_name<'a> {
            iter: ParsingIterator<'a, $item_type, StatelessParserWrapper<$item_type>>,
        }

        impl<'a> Iterator for $struct_name<'a> {
            type Item = ParseResult<'a, $item_type>;

            fn next(&mut self) -> Option<Self::Item> {
                self.iter.next()
            }
        }
    };
}

iter_struct!(Booleans, bool);
iter_struct!(Chars, u16);
iter_struct!(Floats, f32);
iter_struct!(Doubles, f64);
iter_struct!(Bytes, i8);
iter_struct!(Shorts, i16);
iter_struct!(Ints, i32);
iter_struct!(Longs, i64);

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum PrimitiveArrayType {
    Boolean,
    Char,
    Float,
    Double,
    Byte,
    Short,
    Int,
    Long,
}

impl PrimitiveArrayType {
    pub fn java_type_name(&self) -> &'static str {
        match self {
            PrimitiveArrayType::Boolean => "boolean",
            PrimitiveArrayType::Char => "char",
            PrimitiveArrayType::Float => "float",
            PrimitiveArrayType::Double => "double",
            PrimitiveArrayType::Byte => "byte",
            PrimitiveArrayType::Short => "short",
            PrimitiveArrayType::Int => "int",
            PrimitiveArrayType::Long => "long",
        }
    }
}
