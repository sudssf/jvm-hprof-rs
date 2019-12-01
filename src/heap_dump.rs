use crate::*;

enum HeapDumpRecord {
    UnknownGcRoot {
        obj_id: Id,
    },
    ThreadGcRoot {
        /// May be missing for a thread newly attached through JNI
        thread_obj_id: Option<Id>,
        thread_serial: u32,
        stack_trace_serial: u32,
    },
    JniGlobalGcRoot {
        obj_id: Id,
        jni_global_ref_id: Id,
    },
    JniLocalRef {
        obj_id: Id,
        thread_serial: u32,
        frame_index: Option<u32>,
    },
    JavaStackFrame {
        obj_id: Id,
        thread_serial: u32,
        frame_index: Option<u32>,
    },
    NativeStack {
        obj_id: Id,
        thread_serial: u32,
    },
    SystemClass {
        obj_id: Id,
    },
    ThreadBlock {
        obj_id: Id,
        thread_serial: u32,
    },
    BusyMonitor {
        obj_id: Id,
    },
    ClassObject {
        obj_id: Id,
        thread_serial: u32,
        // TODO option for super class?
        super_class_obj_id: Id,
        class_loader_obj_id: Id,
        signers_obj_id: Id,
        protection_domain_obj_id: Id,
        // 2x Id reserved
        instance_size: u32,
        // TODO constant pool entries
        // TODO static fields
        // TODO instance fields
    },
    Object {
        obj_id: Id,
        stack_trace_serial: u32,
        class_obj_id: Id,
        // TODO instance field values
    },
    ObjectArray {
        obj_id: Id,
        stack_trace_serial: u32,
        array_class_id: Id,
        // TODO list of objects
    },
    PrimitiveArray {
        obj_id: Id,
        stack_trace_serial: u32,
        // TODO iterate over primitives
    },
}

enum ConstantPoolEntry {}

struct FieldEntry {
    name_id: Id,
    value: FieldValue,
}

enum FieldValue {
    Object {},
    Boolean(bool),
    Char(u16),
    Float(f32),
    Double(f64),
    Byte(u8),
    Int(i32),
    Long(i64),
}
