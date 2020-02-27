use crate::util::*;
use jvm_hprof::{heap_dump::*, *};

use std::collections;

pub fn gc_root_paths(hprof: &Hprof) {
    // assemble a graph of counts between _types_, not instances, as a way of compressing huge
    // object tangles for easier visual analysis

    // class obj id -> LoadClass
    let mut load_classes = collections::HashMap::new();
    // name id -> String
    let mut utf8 = collections::HashMap::new();

    // get all the gc root ids so we can tell when we reach them later

    let mut gc_root_unknown_ids = collections::HashSet::<Id>::new();
    let mut gc_root_thread_obj_ids = collections::HashSet::<Id>::new();
    let mut gc_root_jni_global_ids = collections::HashSet::<Id>::new();
    let mut gc_root_jni_local_ref_ids = collections::HashSet::<Id>::new();
    let mut gc_root_java_stack_frame_ids = collections::HashSet::<Id>::new();
    let mut gc_root_native_stack_ids = collections::HashSet::<Id>::new();
    let mut gc_root_system_class_ids = collections::HashSet::<Id>::new();
    let mut gc_root_thread_block_ids = collections::HashSet::<Id>::new();
    let mut gc_root_busy_monitor_ids = collections::HashSet::<Id>::new();
    let mut classes: collections::HashMap<Id, MiniClass> = collections::HashMap::new();
    // instance obj id to class obj id
    // TODO if this gets big, could use lmdb or similar to get it off-heap
    let mut obj_id_to_class_obj_id: collections::HashMap<Id, Id> = collections::HashMap::new();
    let mut prim_array_obj_id_to_type = collections::HashMap::new();

    let missing_utf8 = String::from("(missing utf8)");

    let mut instances = 0_u64;
    let mut object_arrays = 0_u64;
    let mut prim_arrays = 0_u64;
    hprof.records_iter().map(|r| r.unwrap()).for_each(|r| {
        match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        SubRecord::GcRootUnknown(v) => {
                            gc_root_unknown_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootThreadObj(v) => match v.thread_obj_id() {
                            Some(id) => {
                                gc_root_thread_obj_ids.insert(id);
                            }
                            None => {}
                        },
                        SubRecord::GcRootJniGlobal(v) => {
                            gc_root_jni_global_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootJniLocalRef(v) => {
                            gc_root_jni_local_ref_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootJavaStackFrame(v) => {
                            gc_root_java_stack_frame_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootNativeStack(v) => {
                            gc_root_native_stack_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootSystemClass(v) => {
                            gc_root_system_class_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootThreadBlock(v) => {
                            gc_root_thread_block_ids.insert(v.obj_id());
                        }
                        SubRecord::GcRootBusyMonitor(v) => {
                            gc_root_busy_monitor_ids.insert(v.obj_id());
                        }
                        SubRecord::Class(c) => {
                            classes.insert(
                                c.obj_id(),
                                MiniClass {
                                    super_class_obj_id: c.super_class_obj_id(),
                                    static_fields: c.static_fields().map(|r| r.unwrap()).collect(),
                                    instance_field_descriptors: c
                                        .instance_field_descriptors()
                                        .map(|r| r.unwrap())
                                        .collect(),
                                    // TODO lifetimes to avoid allocation
                                    name: load_classes
                                        .get(&c.obj_id())
                                        .map(|lc: &LoadClass| {
                                            utf8.get(&lc.class_name_id())
                                                .map(|s: &String| s.to_owned())
                                        })
                                        .unwrap_or(Some(String::from("missing LoadClass")))
                                        .unwrap_or(missing_utf8.clone()),
                                },
                            );
                        }
                        SubRecord::Instance(instance) => {
                            instances += 1;

                            // classes are dumped before instances, so we should be able to look up
                            match classes.get(&instance.class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                ),
                                Some(c) => {
                                    obj_id_to_class_obj_id
                                        .insert(instance.obj_id(), instance.class_obj_id());
                                }
                            };
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            object_arrays += 1;
                            match classes.get(&obj_array.array_class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for object array {}",
                                    obj_array.array_class_obj_id(),
                                    obj_array.obj_id()
                                ),
                                Some(c) => {
                                    obj_id_to_class_obj_id
                                        .insert(obj_array.obj_id(), obj_array.array_class_obj_id());
                                }
                            };
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            prim_arrays += 1;
                            prim_array_obj_id_to_type.insert(pa.obj_id(), pa.primitive_type());
                        }
                    };
                }
            }
            RecordTag::Utf8 => {
                let u = r.as_utf_8().unwrap().unwrap();
                // TODO lifetimes -- nice to not allocate here
                utf8.insert(
                    u.name_id(),
                    u.text_as_str()
                        .map(|s| s.to_string())
                        .unwrap_or(String::from("(invalid UTF-8)")),
                );
            }
            RecordTag::LoadClass => {
                let lc = r.as_load_class().unwrap().unwrap();
                load_classes.insert(lc.class_obj_id(), lc);
            }
            _ => {}
        }
    });

    // class obj id => vec of all instance field descriptors (the class, then super class, then ...)
    let mut class_instance_field_descriptors = collections::HashMap::new();

    // classes are not laid down super class first, so have to wait until the end to be able to
    // navigate the class hierarchy
    for (id, mc) in &classes {
        let mut opt_scid = mc.super_class_obj_id;
        let mut field_descriptors = Vec::<FieldDescriptor>::new();
        field_descriptors.extend(mc.instance_field_descriptors.iter());
        while let Some(scid) = opt_scid {
            let sc = classes
                .get(&scid)
                .expect("Corrupt heap dump? Could not find superclass");

            field_descriptors.extend(sc.instance_field_descriptors.iter());
            opt_scid = sc.super_class_obj_id;
        }

        class_instance_field_descriptors.insert(id, field_descriptors);
    }

    // now, iterate over objects again and accumulate edge counts

    let mut graph_edges = collections::HashMap::new();

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        // TODO class - static fields
                        SubRecord::Instance(instance) => {
                            let mc = match classes.get(&instance.class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                ),
                                Some(c) => {
                                    c
                                }
                            };

                            let field_descriptors = class_instance_field_descriptors
                                .get(&instance.class_obj_id())
                                .expect("Should have all classes available");

                            let mut field_val_input: &[u8] = instance.fields();
                            for (index, fd) in field_descriptors.iter().enumerate() {
                                let (input, field_val) = fd
                                    .field_type()
                                    .parse_value(field_val_input, hprof.header().id_size())
                                    .unwrap();
                                field_val_input = input;

                                match field_val {
                                    FieldValue::ObjectId(Some(field_ref_id)) => {
                                        let source = HeapGraphSource::InstanceField {
                                            class_id: instance.class_obj_id(),
                                            field_offset: index,
                                        };

                                        let dest_opt = obj_id_to_class_obj_id.get(&field_ref_id)
                                            .map(|class_obj_id| HeapGraphDest::InstanceOfClass { class_obj_id: *class_obj_id })
                                            .or_else(|| {
                                                prim_array_obj_id_to_type.get(&field_ref_id)
                                                    .map(|prim_type| {
                                                        HeapGraphDest::PrimitiveArray { prim_type: *prim_type }
                                                    })
                                            })
                                            .or_else(|| {
                                                classes.get(&field_ref_id)
                                                    .map(|dest_class| {
                                                        HeapGraphDest::ClassObj { class_obj_id: field_ref_id }
                                                    })
                                            });

                                        match dest_opt {
                                            None => {
                                                eprintln!(
                                                    "Could not find any match for obj {:?}: {} in field {}",
                                                    field_ref_id,
                                                    mc.name,
                                                    utf8.get(&fd.name_id()).unwrap_or(&missing_utf8)
                                                );
                                            }
                                            Some(dest) => {
                                                graph_edges.entry(GraphEdge { source, dest })
                                                    .and_modify(|c| *c += 1)
                                                    .or_insert(1_u64);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        // TODO obj arrays
                        // TODO prim arrays
                        _ => {}
                    }
                }
            }
            _ => {}
        });

    println!("unknown: {}", gc_root_unknown_ids.len());
    println!("thread obj: {}", gc_root_thread_obj_ids.len());
    println!("jni global: {}", gc_root_jni_global_ids.len());
    println!("jni local: {}", gc_root_jni_local_ref_ids.len());
    println!("java stack frame: {}", gc_root_java_stack_frame_ids.len());
    println!("native stack: {}", gc_root_native_stack_ids.len());
    println!("system class: {}", gc_root_system_class_ids.len());
    println!("thread block: {}", gc_root_thread_block_ids.len());
    println!("busy monitor: {}", gc_root_busy_monitor_ids.len());

    println!("classes: {}", classes.len());
    println!("instances: {}", instances);
    println!("object arrays: {}", object_arrays);
    println!("prim arrays: {}", prim_arrays);
}

#[derive(Hash, Eq, PartialEq)]
struct GraphEdge {
    source: HeapGraphSource,
    dest: HeapGraphDest,
}

#[derive(Hash, Eq, PartialEq)]
enum HeapGraphSource {
    GcRootUnknown,
    GcRootThreadObj,
    GcRootJniGlobal,
    GcRootJniLocalRef,
    GcRootJavaStackFrame,
    GcRootNativeStack,
    GcRootSystemClass,
    GcRootThreadBlock,
    GcRootBusyMonitor,
    StaticField { class_id: Id, field_offset: usize },
    InstanceField { class_id: Id, field_offset: usize },
}

#[derive(Hash, Eq, PartialEq)]
enum HeapGraphDest {
    InstanceOfClass { class_obj_id: Id },
    ClassObj { class_obj_id: Id },
    PrimitiveArray { prim_type: PrimitiveArrayType },
}
