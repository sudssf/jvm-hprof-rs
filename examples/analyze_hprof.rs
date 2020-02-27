use base64;
use chrono;
use chrono::offset::TimeZone;
use clap;
use escaper;
use itertools::Itertools;

use jvm_hprof::heap_dump::{
    FieldDescriptor, FieldValue, PrimitiveArrayType, StaticFieldEntry, SubRecord,
};
use jvm_hprof::{Hprof, Id, LoadClass, RecordTag, Serial, Utf8};
use memmap;
use std::io::Write;
use std::{collections, fs, path};

mod dump_objects;
mod util;

use util::*;

fn main() {
    let app = clap::App::new("Analyze hprof")
        .arg(
            clap::Arg::with_name("file")
                .short("f")
                .long("file")
                .required(true)
                .takes_value(true),
        )
        .subcommand(clap::SubCommand::with_name("header"))
        .subcommand(clap::SubCommand::with_name("tag-counts"))
        .subcommand(clap::SubCommand::with_name("dump-utf8"))
        .subcommand(clap::SubCommand::with_name("dump-load-class"))
        .subcommand(clap::SubCommand::with_name("dump-stack-trace"))
        .subcommand(clap::SubCommand::with_name("dump-classes"))
        .subcommand(clap::SubCommand::with_name("dump-objects"))
        .subcommand(
            clap::SubCommand::with_name("class-hierarchy").arg(
                clap::Arg::with_name("output")
                    .short("o")
                    .long("output")
                    .help("path to output dot file")
                    .required(true)
                    .takes_value(true),
            ),
        )
        .subcommand(
            clap::SubCommand::with_name("gc-root-paths").arg(
                clap::Arg::with_name("output")
                    .short("o")
                    .long("output")
                    .help("path to output dot file")
                    .required(true)
                    .takes_value(true),
            ),
        );
    let matches = app.get_matches();

    let file_path = matches.value_of("file").expect("file must be specified");

    let file = fs::File::open(file_path).unwrap();

    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();

    let hprof = jvm_hprof::parse_hprof(&memmap[..]).unwrap();

    match matches.subcommand() {
        ("header", _) => header(&hprof),
        ("tag-counts", _) => tag_counts(&hprof),
        ("dump-utf8", _) => dump_utf8(&hprof),
        ("dump-load-class", _) => dump_load_class(&hprof),
        ("dump-stack-trace", _) => dump_stack_trace(&hprof),
        ("dump-classes", _) => dump_classes(&hprof),
        ("dump-objects", _) => dump_objects::dump_objects(&hprof),
        ("class-hierarchy", arg_matches) => class_hierarchy_dot(
            &hprof,
            arg_matches
                .expect("must provide args")
                .value_of("output")
                .map(|s| path::Path::new(s))
                .expect("must provide output path"),
        ),
        ("gc-root-paths", arg_matches) => gc_root_paths(&hprof),
        _ => panic!("Unknown subcommand"),
    };
}

fn header(hprof: &Hprof) {
    println!("Label: {}", hprof.header().label().unwrap());
    println!("Id size: {:?}", hprof.header().id_size());
    let ts = chrono::Utc.timestamp_millis(hprof.header().timestamp_millis() as i64);
    println!("Timestamp: {}", ts);
}

fn tag_counts(hprof: &Hprof) {
    let mut tag_counts: Vec<(RecordTag, usize)> = hprof
        .records_iter()
        .map(|r| r.unwrap())
        // sort first because group_by only groups contiguous runs
        .sorted_by_key(|r| r.tag())
        .group_by(|r| r.tag())
        .into_iter()
        .map(|(tag, group)| (tag, group.count()))
        .sorted_by_key(|&(_, count)| count)
        .collect::<Vec<(jvm_hprof::RecordTag, usize)>>();

    // highest count on top
    tag_counts.reverse();

    for (tag, count) in tag_counts {
        println!("{:?}: {}", tag, count);
    }
}

fn dump_utf8(hprof: &Hprof) {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::Utf8)
        .map(|r| r.as_utf_8().unwrap().unwrap())
        .for_each(|u| match u.text_as_str() {
            Ok(s) => println!("name id {} -> {:?}", u.name_id(), s),
            Err(e) => {
                eprintln!(
                    "name id {} parse error {:?} - base64 of invalid utf8: {}",
                    u.name_id(),
                    e,
                    base64::encode(u.text())
                );
            }
        });
}

fn dump_load_class(hprof: &Hprof) {
    let utf8 = utf8_by_id(hprof);

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .for_each(|l| {
            println!("Class serial: {}", l.class_serial());
            println!("Class obj id: {}", l.class_obj_id());
            println!("Stack trace serial: {}", l.stack_trace_serial());
            println!(
                "Class name id: {} -> {}",
                l.class_name_id(),
                get_utf8_if_available(&utf8, l.class_name_id())
            );
            println!();
        })
}

fn dump_stack_trace(hprof: &Hprof) {
    let utf8 = utf8_by_id(hprof);

    let frames = hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::StackFrame)
        .map(|r| r.as_stack_frame().unwrap().unwrap())
        .map(|f| (f.id(), f))
        .collect::<collections::HashMap<_, _>>();

    let load_classes_by_serial = classes_by_serial(hprof);

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == RecordTag::StackTrace)
        .for_each(|r| {
            let t = r.as_stack_trace().unwrap().unwrap();
            println!("Trace serial: {}", t.stack_trace_serial());
            println!("Thread serial: {}", t.thread_serial());

            for id in t.frame_ids().map(|r| r.unwrap()) {
                print!("{}\t", id);

                match frames.get(&id) {
                    None => println!("(no frame found)"),
                    Some(f) => println!(
                        "{}:{}\n\t↪ {}#{}({})",
                        get_utf8_if_available(&utf8, f.source_file_name_id()),
                        f.line_num(),
                        load_classes_by_serial
                            .get(&f.class_serial())
                            .map(|lc| get_utf8_if_available(&utf8, lc.class_name_id()))
                            .unwrap_or("(class not found)"),
                        get_utf8_if_available(&utf8, f.method_name_id()),
                        get_utf8_if_available(&utf8, f.method_signature_id())
                    ),
                }
            }

            println!();
        })
}

fn class_hierarchy_dot(hprof: &Hprof, output: &path::Path) {
    let utf8 = utf8_by_id(hprof);
    let load_classes_by_obj_id = classes_by_obj_id(hprof);

    let mut dot = fs::File::create(output).unwrap();

    writeln!(dot, "digraph G {{").unwrap();

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == RecordTag::HeapDump || r.tag() == RecordTag::HeapDumpSegment)
        .for_each(|r| {
            let segment = r.as_heap_dump_segment().unwrap().unwrap();
            for p in segment.sub_records() {
                let s = p.unwrap();

                match s {
                    SubRecord::Class(_) => {
                        let class = s.as_class().unwrap();

                        // dot supports html-ish tables
                        writeln!(dot, "\t{} [shape=box, label=<", class.obj_id()).unwrap();
                        writeln!(dot, "<TABLE BORDER=\"0\" CELLBORDER=\"1\">").unwrap();

                        writeln!(
                            dot,
                            "<TR><TD COLSPAN=\"2\">{} ({})</TD></TR>",
                            escaper::encode_minimal(
                                load_classes_by_obj_id
                                    .get(&class.obj_id())
                                    .map(|lc| get_utf8_if_available(&utf8, lc.class_name_id()))
                                    .unwrap_or("(LoadClass not found)")
                            ),
                            escaper::encode_minimal(&format!("{:#018X}", class.obj_id()))
                        )
                        .unwrap();

                        writeln!(
                            dot,
                            "<TR><TD>Instance size (bytes)</TD><TD>{}</TD></TR>",
                            class.instance_size_bytes()
                        )
                        .unwrap();

                        if class.static_fields().count() > 0 {
                            writeln!(dot, "<TR><TD COLSPAN=\"2\">Static fields</TD></TR>").unwrap();
                            for pr in class.static_fields() {
                                let sf = pr.unwrap();
                                writeln!(
                                    dot,
                                    "<TR><TD>{}</TD><TD>{}</TD></TR>",
                                    escaper::encode_minimal(get_utf8_if_available(
                                        &utf8,
                                        sf.name_id(),
                                    )),
                                    escaper::encode_minimal(&format!("{:?}", sf.value()))
                                )
                                .unwrap();
                            }
                        }

                        if class.instance_field_descriptors().count() > 0 {
                            writeln!(
                                dot,
                                "<TR><TD COLSPAN=\"2\">Instance field descriptors</TD></TR>"
                            )
                            .unwrap();
                            for pr in class.instance_field_descriptors() {
                                let fd = pr.unwrap();
                                writeln!(
                                    dot,
                                    "<TR><TD>{}</TD><TD>{}</TD></TR>",
                                    escaper::encode_minimal(get_utf8_if_available(
                                        &utf8,
                                        fd.name_id(),
                                    )),
                                    escaper::encode_minimal(&format!("{:?}", fd.field_type()))
                                )
                                .unwrap();
                            }
                        }

                        writeln!(dot, "</TABLE>").unwrap();
                        writeln!(dot, "\t>];").unwrap();

                        if let Some(super_id) = class.super_class_obj_id() {
                            writeln!(dot, "\t{} -> {};", class.obj_id(), super_id).unwrap();
                        }
                    }
                    _ => {}
                }
            }
        });

    writeln!(dot, "}}").unwrap();
}

fn dump_classes(hprof: &Hprof) {
    let utf8 = utf8_by_id(hprof);
    let load_classes_by_obj_id = classes_by_obj_id(hprof);

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == RecordTag::HeapDump || r.tag() == RecordTag::HeapDumpSegment)
        .for_each(|r| {
            let segment = r.as_heap_dump_segment().unwrap().unwrap();
            for p in segment.sub_records() {
                let s = p.unwrap();

                match s {
                    SubRecord::Class(_) => {
                        let class = s.as_class().unwrap();

                        println!("Obj id: {:#018X} = {}", class.obj_id(), class.obj_id());
                        println!(
                            "\tName (via LoadClass): {}",
                            load_classes_by_obj_id
                                .get(&class.obj_id())
                                .map(|lc| get_utf8_if_available(&utf8, lc.class_name_id()))
                                .unwrap_or("(LoadClass not found)")
                        );
                        println!("Stack trace serial: {:#010X}", class.stack_trace_serial());
                        println!(
                            "Super class obj id: {:#018X}",
                            class.super_class_obj_id().map(|i| i.id()).unwrap_or(0)
                        );
                        println!(
                            "Class loader obj id: {:#018X}",
                            class.class_loader_obj_id().map(|i| i.id()).unwrap_or(0)
                        );
                        println!(
                            "Signers obj id: {:#018X}",
                            class.signers_obj_id().map(|i| i.id()).unwrap_or(0)
                        );
                        println!(
                            "Protection domain obj id: {:#018X}",
                            class
                                .protection_domain_obj_id()
                                .map(|i| i.id())
                                .unwrap_or(0)
                        );
                        println!("Instance size: {}", class.instance_size_bytes());

                        if class.static_fields().count() > 0 {
                            println!("Static fields:");

                            for pr in class.static_fields() {
                                let sf = pr.unwrap();
                                println!(
                                    "\t{:#018X} ({}): {:?}",
                                    sf.name_id(),
                                    get_utf8_if_available(&utf8, sf.name_id()),
                                    sf.value()
                                );

                                println!();
                            }
                        }

                        if class.instance_field_descriptors().count() > 0 {
                            println!("Instance fields:");

                            for ifd_result in class.instance_field_descriptors() {
                                let ifd = ifd_result.unwrap();

                                println!(
                                    "\t{}: {:?}",
                                    get_utf8_if_available(&utf8, ifd.name_id()),
                                    ifd.field_type()
                                );

                                println!();
                            }
                        }

                        println!();
                    }
                    _ => {}
                }
            }
        });
}

fn gc_root_paths(hprof: &Hprof) {
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

fn utf8_by_id<'a>(hprof: &'a Hprof) -> collections::HashMap<Id, Utf8<'a>> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::Utf8)
        .map(|r| r.as_utf_8().unwrap().unwrap())
        .map(|u| (u.name_id(), u))
        .collect::<collections::HashMap<_, _>>()
}

fn classes_by_serial(hprof: &Hprof) -> collections::HashMap<Serial, LoadClass> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .map(|f| (f.class_serial(), f))
        .collect::<collections::HashMap<_, _>>()
}

fn classes_by_obj_id(hprof: &Hprof) -> collections::HashMap<Id, LoadClass> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .map(|f| (f.class_obj_id(), f))
        .collect::<collections::HashMap<_, _>>()
}

fn get_utf8_if_available<'a>(utf8: &'a collections::HashMap<Id, Utf8<'a>>, id: Id) -> &'a str {
    utf8.get(&id)
        .map(|u| u.text_as_str().unwrap_or("(invalid utf8)"))
        .unwrap_or("(utf8 not found)")
}
