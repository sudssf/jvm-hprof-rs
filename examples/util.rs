use jvm_hprof::{heap_dump::*, *};
use std::collections;

// TODO need to figure out lifetimes so I can just keep the original Class with its slice
pub struct MiniClass {
    pub super_class_obj_id: Option<Id>,
    pub static_fields: Vec<StaticFieldEntry>,
    pub instance_field_descriptors: Vec<FieldDescriptor>,
    pub name: String,
}

pub fn utf8_by_id<'a>(hprof: &'a Hprof) -> collections::HashMap<Id, Utf8<'a>> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::Utf8)
        .map(|r| r.as_utf_8().unwrap().unwrap())
        .map(|u| (u.name_id(), u))
        .collect::<collections::HashMap<_, _>>()
}

pub fn classes_by_serial(hprof: &Hprof) -> collections::HashMap<Serial, LoadClass> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .map(|f| (f.class_serial(), f))
        .collect::<collections::HashMap<_, _>>()
}

pub fn classes_by_obj_id(hprof: &Hprof) -> collections::HashMap<Id, LoadClass> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .map(|f| (f.class_obj_id(), f))
        .collect::<collections::HashMap<_, _>>()
}

pub fn get_utf8_if_available<'a>(utf8: &'a collections::HashMap<Id, Utf8<'a>>, id: Id) -> &'a str {
    utf8.get(&id)
        .map(|u| u.text_as_str().unwrap_or("(invalid utf8)"))
        .unwrap_or("(utf8 not found)")
}
