use crate::dot;
use crate::util::*;
use jvm_hprof::{heap_dump::*, *};

use crate::counter::Counter;
use crate::index::Index;
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::io::{self, Write};
use std::{collections, fs, path};

/// Assemble a graph of counts between _types_, not instances, as a way of compressing huge
/// object tangles for easier visual analysis
pub fn ref_count_graph<I: Index>(
    hprof: &Hprof,
    index: &I,
    output: &path::Path,
    min_edge_count: u64,
) {
    // class obj id -> LoadClass
    let mut load_classes = collections::HashMap::new();
    // name id -> String
    let mut utf8 = collections::HashMap::new();
    let mut classes: collections::HashMap<Id, EzClass> = collections::HashMap::new();

    let missing_utf8 = "(missing utf8)";

    println!("Loading classes");

    // this pass already goes at about 1G/s on one core, so the available speedup from parallelizing
    // isn't very high before i/o is saturated anyway
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        SubRecord::Class(c) => {
                            classes
                                .insert(c.obj_id(), EzClass::from_class(&c, &load_classes, &utf8));
                        }
                        _ => {}
                    };
                }
            }
            RecordTag::Utf8 => {
                let u = r.as_utf_8().unwrap().unwrap();
                utf8.insert(u.name_id(), u.text_as_str().unwrap_or("(invalid UTF-8)"));
            }
            RecordTag::LoadClass => {
                let lc = r.as_load_class().unwrap().unwrap();
                load_classes.insert(lc.class_obj_id(), lc);
            }
            _ => {}
        });

    // class obj id => vec of all instance field descriptors (the class, then super class, then ...)
    let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

    // iterate over objects and accumulate edge counts

    let id_size = hprof.header().id_size();

    // look in all the possible places an object id might be to build the right type of destination
    let edge_dest_for_obj_id = |obj_id: Id| {
        index
            .get_class_id(obj_id)
            .map(|class_obj_id_opt| {
                class_obj_id_opt.map(|class_obj_id| HeapGraphDest::InstanceOfClass { class_obj_id })
            })
            .and_then(|dest_opt| match dest_opt {
                // didn't find it in the normal index lookup, so see if it's a primitive array
                None => index.get_prim_array_type(obj_id).map(|prim_type_opt| {
                    prim_type_opt.map(|prim_type| HeapGraphDest::PrimitiveArray { prim_type })
                }),
                Some(d) => Ok(Some(d)),
            })
            .map(|dest_opt|
                // neither index lookup worked, so try classes
                match dest_opt {
                    None => classes
                        .get(&obj_id)
                        .map(|_dest_class| HeapGraphDest::ClassObj {
                            class_obj_id: obj_id,
                        }),
                    Some(d) => Some(d)
                })
            // error is unrecoverable anyway, might as well just crash
            .expect("Error when reading index")
    };

    println!("Calculating reference counts");
    println!(". = 1,000,000 heap dump segment sub records");
    let mut all_graph_edges = hprof
        .records_iter()
        .par_bridge()
        .panic_fuse()
        .map(|r| r.unwrap())
        .map(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                let mut sub_records = 0_u64;

                let mut graph_edges: Counter<GraphEdge> = Counter::new();

                for p in segment.sub_records() {
                    let s = p.unwrap();

                    sub_records += 1;

                    if sub_records == 1_000_000 {
                        sub_records = 0;
                        // we won't print a . for leftover sub records beyond multiples of 1M, but meh
                        print!(".");
                        // TODO unwrap
                        io::stdout().flush().unwrap();
                    }

                    match s {
                        SubRecord::GcRootUnknown(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootUnknown",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootUnknown, dest })
                        },
                        SubRecord::GcRootThreadObj(gc_root) => gc_root.thread_obj_id().iter().for_each(|obj_id| {
                            match edge_dest_for_obj_id(*obj_id) {
                                None => eprintln!(
                                    "Could not find any match for obj {:?} in GcRootThreadObj",
                                    obj_id,
                                ),
                                Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootThreadObj, dest })
                            }
                        }),
                        SubRecord::GcRootJniGlobal(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootJniGlobal",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootJniGlobal, dest })
                        },
                        SubRecord::GcRootJniLocalRef(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootJniLocalRef",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootJniLocalRef, dest })
                        },
                        SubRecord::GcRootJavaStackFrame(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootJavaStackFrame",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootJavaStackFrame, dest })
                        },
                        SubRecord::GcRootNativeStack(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootNativeStack",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootNativeStack, dest })
                        },
                        SubRecord::GcRootSystemClass(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootSystemClass",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootSystemClass, dest })
                        },
                        SubRecord::GcRootThreadBlock(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootThreadBlock",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootThreadBlock, dest })
                        },
                        SubRecord::GcRootBusyMonitor(gc_root) => match edge_dest_for_obj_id(gc_root.obj_id()) {
                            None => eprintln!(
                                "Could not find any match for obj {:?} in GcRootBusyMonitor",
                                gc_root.obj_id(),
                            ),
                            Some(dest) => graph_edges.increment(GraphEdge { source: HeapGraphSource::GcRootBusyMonitor, dest })
                        },
                        SubRecord::PrimitiveArray(_) => { /* primitive arrays have no refs */ }
                        SubRecord::Class(c) => {
                            let mc = classes.get(&c.obj_id())
                                // already know the class exists
                                .unwrap();

                            mc.static_fields.iter()
                                .enumerate()
                                .for_each(|(index, sf)| {
                                    match sf.value() {
                                        FieldValue::ObjectId(Some(field_ref_id)) => {
                                            let source = HeapGraphSource::StaticField {
                                                class_obj_id: c.obj_id(),
                                                field_offset: index,
                                            };

                                            match edge_dest_for_obj_id(field_ref_id) {
                                                None => eprintln!(
                                                    "Could not find any match for obj {:?}: {} in static field {}",
                                                    field_ref_id,
                                                    mc.name,
                                                    utf8.get(&sf.name_id()).unwrap_or(&missing_utf8)
                                                ),
                                                Some(dest) => graph_edges.increment(GraphEdge { source, dest })
                                            }
                                        }
                                        _ => {}
                                    }
                                });
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
                                    .parse_value(field_val_input, id_size)
                                    .unwrap();
                                field_val_input = input;

                                match field_val {
                                    FieldValue::ObjectId(Some(field_ref_id)) => {
                                        let source = HeapGraphSource::InstanceField {
                                            class_obj_id: instance.class_obj_id(),
                                            field_offset: index,
                                        };

                                        match edge_dest_for_obj_id(field_ref_id) {
                                            None => eprintln!(
                                                "Could not find any match for obj {:?}: {} in field {}",
                                                field_ref_id,
                                                mc.name,
                                                utf8.get(&fd.name_id()).unwrap_or(&missing_utf8)
                                            ),
                                            Some(dest) => graph_edges.increment(GraphEdge { source, dest })
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
                            obj_array.elements(id_size)
                                .filter_map(|res| res.unwrap())
                                .for_each(|id| {
                                    let source = HeapGraphSource::ObjectArray {
                                        class_obj_id: mc.obj_id
                                    };

                                    match edge_dest_for_obj_id(id) {
                                        None => eprintln!(
                                            "Could not find any match for obj {:?} in array {:?} ({})",
                                            id,
                                            obj_array.array_class_obj_id(),
                                            mc.name
                                        ),
                                        Some(dest) => graph_edges.increment(GraphEdge { source, dest })
                                    }
                                })
                        }
                    }
                }

                graph_edges
            }
            // empty counter for other cases
            _ => Counter::new()
        })
        .reduce(|| Counter::new(),
                |mut acc, x| {
                    acc += x;
                    acc
                });

    println!();

    all_graph_edges.retain(|_edge, count| *count >= min_edge_count);

    let mut output_file = fs::File::create(output).unwrap();

    writeln!(output_file, "digraph G {{").unwrap();

    // for each class referenced, add a node with all the fields
    all_graph_edges
        .iter()
        .map(|(k, _v)| k)
        .filter_map(|edge| match edge.source {
            HeapGraphSource::StaticField { class_obj_id, .. } => Some(class_obj_id),
            HeapGraphSource::InstanceField { class_obj_id, .. } => Some(class_obj_id),
            HeapGraphSource::ObjectArray { class_obj_id } => Some(class_obj_id),
            _ => None,
        })
        .chain(
            all_graph_edges
                .iter()
                .map(|(k, _v)| k)
                .filter_map(|edge| match edge.dest {
                    HeapGraphDest::InstanceOfClass { class_obj_id } => Some(class_obj_id),
                    HeapGraphDest::ClassObj { class_obj_id } => Some(class_obj_id),
                    HeapGraphDest::PrimitiveArray { .. } => None,
                }),
        )
        // uniqueify -- each id will only have one source mode
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
            .unwrap()
        });

    // gc roots
    all_graph_edges
        .iter()
        .map(|(k, _v)| k)
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
            HeapGraphSource::ObjectArray { .. } => false,
        })
        .map(|edge| write_to_string(|s| edge.source.write_node_name(s)).unwrap())
        .collect::<collections::HashSet<String>>()
        .iter()
        .for_each(|node_name| {
            writeln!(
                output_file,
                "\t{}[shape=box, label={}]",
                node_name, node_name
            )
            .unwrap()
        });

    // primitive arrays
    all_graph_edges
        .iter()
        .map(|(k, _v)| k)
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

    all_graph_edges.iter().for_each(|(edge, &count)| {
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
        write!(writer, "label=\"x{}\"", count)?;
        // arbitrary aesthetic scaling
        write!(
            writer,
            "penwidth=\"{}\"",
            (count as f64).log10().powi(2) / 3.0
        )?;
        if let Some(port) = self.source.node_port() {
            write!(writer, "tailport=\"{}\"", port)?;
        }
        write!(writer, "]")?;
        writeln!(writer, ";")
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
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
    ObjectArray {
        class_obj_id: Id,
    },
}

impl HeapGraphSource {
    /// Returns the dot node name
    fn write_node_name<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        write!(writer, "\"")?;
        // Matches naming convention in dot:: functions
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
            HeapGraphSource::StaticField { class_obj_id, .. } => {
                write!(writer, "class-{}", class_obj_id)
            }
            HeapGraphSource::InstanceField { class_obj_id, .. } => {
                write!(writer, "class-{}", class_obj_id)
            }
            // edges from an object array appear from the class for the [L type
            HeapGraphSource::ObjectArray { class_obj_id } => {
                write!(writer, "class-{}", class_obj_id)
            }
        }?;
        write!(writer, "\"")
    }

    fn node_port(&self) -> Option<String> {
        // Matches naming convention in dot::write_class_node
        match self {
            HeapGraphSource::StaticField { field_offset, .. } => {
                Some(format!("static-field-val-{}", field_offset))
            }
            HeapGraphSource::InstanceField { field_offset, .. } => {
                Some(format!("instance-field-val-{}", field_offset))
            }
            HeapGraphSource::ObjectArray { .. } => Some(String::from("array-contents")),
            _ => None,
        }
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
