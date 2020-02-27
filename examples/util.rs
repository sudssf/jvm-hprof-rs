use jvm_hprof::{heap_dump::*, *};

// TODO need to figure out lifetimes so I can just keep the original Class with its slice
pub struct MiniClass {
    pub super_class_obj_id: Option<Id>,
    pub static_fields: Vec<StaticFieldEntry>,
    pub instance_field_descriptors: Vec<FieldDescriptor>,
    pub name: String,
}
