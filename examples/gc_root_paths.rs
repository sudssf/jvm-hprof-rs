use crate::dot;
use crate::util::*;
use jvm_hprof::{heap_dump::*, *};

use std::io::{self, Write};
use std::{collections, fs, path};

pub fn gc_root_paths(hprof: &Hprof, output: &path::Path, min_edge_count: u64) {
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
                                MiniClass::from_class(&c, &load_classes, &utf8),
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
    let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

    // now, iterate over objects again and accumulate edge counts

    let mut graph_edges: collections::HashMap<GraphEdge, u64> = collections::HashMap::new();

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        // TODO GC roots
                        SubRecord::Class(c) => {
                            let mc = classes.get(&c.obj_id())
                                // already know the class exists
                                .unwrap();
                            // TODO class - static fields
                        }

                        SubRecord::Instance(instance) => {
                            let mc = classes.get(&instance.class_obj_id())
                                // already know the class exists
                                .unwrap();

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
                                            class_obj_id: instance.class_obj_id(),
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
                        SubRecord::ObjectArray(obj_array) => {
                            let mc = classes.get(&obj_array.array_class_obj_id())
                                // already know the class exists
                                .unwrap();
                            // TODO obj arrays
                        }
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

    graph_edges.retain(|edge, count| *count >= min_edge_count);

    let mut output_file = fs::File::create(output).unwrap();

    writeln!(output_file, "digraph G {{").unwrap();

    // for each class referenced, add a node with all the fields
    graph_edges
        .keys()
        .filter_map(|edge| match edge.source {
            HeapGraphSource::StaticField { class_obj_id, .. } => Some(class_obj_id),
            HeapGraphSource::InstanceField { class_obj_id, .. } => Some(class_obj_id),
            _ => None,
        })
        .chain(graph_edges.keys().filter_map(|edge| match edge.dest {
            HeapGraphDest::InstanceOfClass { class_obj_id } => Some(class_obj_id),
            HeapGraphDest::ClassObj { class_obj_id } => Some(class_obj_id),
            HeapGraphDest::PrimitiveArray { .. } => None,
        }))
        // uniqueify
        .collect::<collections::HashSet<Id>>()
        .iter()
        .for_each(|class_obj_id| {
            let class = classes.get(class_obj_id).unwrap();
            dot::write_class_node(
                class,
                class_instance_field_descriptors
                    .get(class_obj_id)
                    .expect("Should have fields for all classes"),
                &utf8,
                &mut output_file,
            )
            .unwrap();
        });

    // TODO when would gc root records show up?
    // gc roots
    graph_edges
        .keys()
        .filter(|edge| match edge.source {
            HeapGraphSource::GcRootUnknown => true,
            HeapGraphSource::GcRootThreadObj => true,
            HeapGraphSource::GcRootJniGlobal => true,
            HeapGraphSource::GcRootJniLocalRef => true,
            HeapGraphSource::GcRootJavaStackFrame => true,
            HeapGraphSource::GcRootNativeStack => true,
            HeapGraphSource::GcRootSystemClass => true,
            HeapGraphSource::GcRootThreadBlock => true,
            HeapGraphSource::GcRootBusyMonitor => true,
            HeapGraphSource::StaticField { .. } => false,
            HeapGraphSource::InstanceField { .. } => false,
        })
        .map(|edge| write_to_string(|s| edge.source.write_node_name(s)).unwrap())
        .collect::<collections::HashSet<String>>()
        .iter()
        .for_each(|node_name| {
            writeln!(
                output_file,
                "\t{}[shape=box, label=\"{}\"]",
                node_name, node_name
            )
            .unwrap()
        });

    // primitive arrays
    graph_edges
        .keys()
        .filter_map(|edge| match edge.dest {
            HeapGraphDest::InstanceOfClass { .. } => None,
            HeapGraphDest::ClassObj { .. } => None,
            HeapGraphDest::PrimitiveArray { prim_type } => Some(prim_type),
        })
        .collect::<collections::HashSet<PrimitiveArrayType>>()
        .iter()
        .for_each(|&prim_type| {
            writeln!(
                output_file,
                "\t{}[shape=box, label=\"{}[]\"]",
                write_to_string(|s| HeapGraphDest::PrimitiveArray { prim_type }.write_node_name(s))
                    .unwrap(),
                prim_type.java_type_name()
            )
            .unwrap()
        });

    // now, write all the edges

    graph_edges.iter().for_each(|(edge, &count)| {
        edge.write_dot_edge(count, &mut output_file).unwrap();
    });

    writeln!(output_file, "}}").unwrap();
}

#[derive(Hash, Eq, PartialEq)]
struct GraphEdge {
    source: HeapGraphSource,
    dest: HeapGraphDest,
}

impl GraphEdge {
    fn write_dot_edge<W: Write>(&self, count: u64, writer: &mut W) -> io::Result<()> {
        write!(writer, "\t")?;
        self.source.write_node_name(writer)?;
        write!(writer, " -> ")?;
        self.dest.write_node_name(writer)?;
        write!(writer, "[")?;
        write!(writer, "taillabel=\"x{}\"", count)?;
        write!(writer, "penwidth=\"{}\"", (count as f64).log10().powi(2))?;
        write!(writer, "]")?;
        writeln!(writer, ";")
    }
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
    StaticField {
        class_obj_id: Id,
        field_offset: usize,
    },
    InstanceField {
        class_obj_id: Id,
        field_offset: usize,
    },
}

impl HeapGraphSource {
    /// Returns the dot node name, with port specifier if applicable
    fn write_node_name<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        write!(writer, "\"")?;
        match self {
            HeapGraphSource::GcRootUnknown => write!(writer, "gc-root-unknown"),
            HeapGraphSource::GcRootThreadObj => write!(writer, "gc-root-thread-obj"),
            HeapGraphSource::GcRootJniGlobal => write!(writer, "gc-root-jni-global"),
            HeapGraphSource::GcRootJniLocalRef => write!(writer, "gc-root-jni-local-ref"),
            HeapGraphSource::GcRootJavaStackFrame => write!(writer, "gc-root-java-stack-frame"),
            HeapGraphSource::GcRootNativeStack => write!(writer, "gc-root-native-stack"),
            HeapGraphSource::GcRootSystemClass => write!(writer, "gc-root-system-class"),
            HeapGraphSource::GcRootThreadBlock => write!(writer, "gc-root-thread-block"),
            HeapGraphSource::GcRootBusyMonitor => write!(writer, "gc-root-busy-monitor"),
            HeapGraphSource::StaticField {
                class_obj_id,
                field_offset,
            } => write!(writer, "class-{}", class_obj_id),
            HeapGraphSource::InstanceField {
                class_obj_id,
                field_offset,
            } => write!(writer, "class-{}", class_obj_id),
        }?;
        write!(writer, "\"")
    }
}

#[derive(Hash, Eq, PartialEq)]
enum HeapGraphDest {
    InstanceOfClass { class_obj_id: Id },
    ClassObj { class_obj_id: Id },
    PrimitiveArray { prim_type: PrimitiveArrayType },
}

impl HeapGraphDest {
    fn write_node_name<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        write!(writer, "\"")?;

        match self {
            HeapGraphDest::InstanceOfClass { class_obj_id } => {
                write!(writer, "class-{}", class_obj_id)
            }
            HeapGraphDest::ClassObj { class_obj_id } => write!(writer, "class-{}", class_obj_id),
            HeapGraphDest::PrimitiveArray { prim_type } => {
                write!(writer, "prim-array-{}", prim_type.java_type_name())
            }
        }?;

        write!(writer, "\"")
    }
}

fn write_to_string<F: FnOnce(&mut Vec<u8>) -> io::Result<()>>(writer: F) -> io::Result<String> {
    let mut v = Vec::new();

    writer(&mut v)?;

    std::str::from_utf8(v.as_slice())
        .map(|s| s.to_owned())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
