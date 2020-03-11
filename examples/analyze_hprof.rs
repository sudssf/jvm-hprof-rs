use base64;
use chrono;
use chrono::offset::TimeZone;
use clap;
use itertools::Itertools;

use jvm_hprof::heap_dump::SubRecord;
use jvm_hprof::{Hprof, RecordTag};
use memmap;
use std::{collections, fs, path};

mod class_hierarchy_dot;
mod dot;
mod dump_objects;
mod gc_root_paths;
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
            clap::SubCommand::with_name("gc-root-paths")
                .arg(
                    clap::Arg::with_name("output")
                        .short("o")
                        .long("output")
                        .help("path to output dot file")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    clap::Arg::with_name("min-edge-count")
                        .long("min-edge-count")
                        .help("minimum count for an edge to be included")
                        .required(false)
                        .default_value("1")
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
        ("class-hierarchy", arg_matches) => class_hierarchy_dot::class_hierarchy_dot(
            &hprof,
            arg_matches
                .expect("must provide args")
                .value_of("output")
                .map(|s| path::Path::new(s))
                .expect("must provide output path"),
        ),
        ("gc-root-paths", arg_matches) => {
            let matches = arg_matches.expect("must provide args");
            gc_root_paths::gc_root_paths(
                &hprof,
                matches
                    .value_of("output")
                    .map(|s| path::Path::new(s))
                    .expect("must provide output path"),
                matches
                    .value_of("min-edge-count")
                    .map(|s| s.parse::<u64>().unwrap())
                    .expect("must provide output path"),
            )
        }
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
