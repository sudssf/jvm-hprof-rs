use anyhow;
use base64;
use chrono;
use chrono::offset::TimeZone;
use clap;
use csv;
use memmap;
use num_cpus;
use rayon;

use itertools::Itertools;
use jvm_hprof::heap_dump::SubRecord;
use jvm_hprof::{Hprof, RecordTag};
use std::{collections, fs, io, path};

#[path = "analyze_hprof/class_hierarchy_dot.rs"]
mod class_hierarchy_dot;
#[path = "analyze_hprof/counter.rs"]
mod counter;
#[path = "analyze_hprof/dot.rs"]
mod dot;
#[path = "analyze_hprof/dump_objects.rs"]
mod dump_objects;
#[path = "analyze_hprof/index/mod.rs"]
mod index;
#[path = "analyze_hprof/instance_counts.rs"]
mod instance_counts;
#[path = "analyze_hprof/ref_count_graph.rs"]
mod ref_count_graph;
#[path = "analyze_hprof/util.rs"]
mod util;

use crate::index::{lmdb::LmdbIndex, HprofFingerprint, Index};
use util::*;

fn main() -> Result<(), anyhow::Error> {
    let app = clap::App::new("Analyze hprof")
        .arg(
            clap::Arg::with_name("file")
                .short("f")
                .long("file")
                .required(true)
                .takes_value(true)
                .help("Heap dump file to read"),
        )
        .arg(
            clap::Arg::with_name("threads")
                .short("t")
                .long("threads")
                .required(false)
                .takes_value(true)
                .help("Number of threads to use, if subcommand is multithreaded. Defaults to 4 or the number of cores, whichever is smaller."),
        )
        .subcommand(clap::SubCommand::with_name("header")
            .about("Display metadata from the hprof header"))
        .subcommand(clap::SubCommand::with_name("record-counts")
            .about("Display the number of each of the top level hprof record types"))
        .subcommand(clap::SubCommand::with_name("dump-utf8")
            .about("Display Utf8 records as CSV"))
        .subcommand(clap::SubCommand::with_name("dump-load-class")
            .about("Display LoadClass records as CSV"))
        .subcommand(clap::SubCommand::with_name("dump-stack-trace")
            .about("Display StackTrace records"))
        .subcommand(clap::SubCommand::with_name("dump-classes")
            .about("Display Class heap dump subrecords"))
        .subcommand(clap::SubCommand::with_name("dump-objects")
            .about("Display Object (and other associated) heap dump subrecords"))
        .subcommand(
            clap::SubCommand::with_name("class-hierarchy")
                .about("Generate a GraphViz dot file of class hierarchy")
                .arg(
                    clap::Arg::with_name("output")
                        .short("o")
                        .long("output")
                        .help("path to output dot file")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("ref-count-graph")
                .about("Generate a GraphViz dot file of class fields to what types are pointed to by those fields")
                .arg(
                    clap::Arg::with_name("index")
                        .short("i")
                        .long("index")
                        .help("path index for the hprof file (created with the build-index subcommand)")
                        .required(true)
                        .takes_value(true),
                )
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
                        .help("minimum count for an edge to be included -- useful to filter down an overly busy graph")
                        .required(false)
                        .default_value("1")
                        .takes_value(true),
                ),
        )
        .subcommand(clap::SubCommand::with_name("instance-counts")
            .about("Display the instance count for each class as CSV"))
        .subcommand(clap::SubCommand::with_name("build-index")
            .about("Build an index on disk for subsequent use with other commands")
            .arg(clap::Arg::with_name("output")
                .short("o")
                .long("output")
                .help("path to output index at")
                .required(true)
                .takes_value(true))
        );
    let matches = app.get_matches();

    let file_path = matches.value_of("file").expect("file must be specified");

    let file = fs::File::open(file_path).unwrap();

    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();

    let hprof = jvm_hprof::parse_hprof(&memmap[..]).unwrap();

    let threads = matches
        .value_of("threads")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or_else(|| {
            let cores = num_cpus::get();

            // 4 is a reasonable default because most systems probably don't have fast enough
            // storage to be able to keep all their cores busy
            std::cmp::min(cores, 4)
        });

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;

    match matches.subcommand() {
        ("header", _) => header(&hprof),
        ("record-counts", _) => dump_record_counts(&hprof),
        ("dump-utf8", _) => dump_utf8(&hprof)?,
        ("dump-load-class", _) => dump_load_class(&hprof)?,
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
        ("ref-count-graph", arg_matches) => {
            let matches = arg_matches.expect("must provide args");
            let index = matches
                .value_of("index")
                .map(|s| {
                    LmdbIndex::open_with_fingerprint(
                        &HprofFingerprint::from_hprof(&hprof),
                        path::Path::new(s),
                    )
                })
                .unwrap()?;
            let min_edge_count = matches
                .value_of("min-edge-count")
                .map(|s| s.parse::<u64>().unwrap())
                .unwrap();
            let output = matches
                .value_of("output")
                .map(|s| path::Path::new(s))
                .unwrap();
            ref_count_graph::ref_count_graph(&hprof, &index, output, min_edge_count)
        }
        ("instance-counts", _) => instance_counts::instance_counts(&hprof)?,
        ("build-index", arg_matches) => index::build_index(
            &hprof,
            arg_matches
                .expect("must provide args")
                .value_of("output")
                .map(|s| path::Path::new(s))
                .expect("must provide output path"),
        )?,
        _ => panic!("Unknown subcommand"),
    };

    Ok(())
}

fn header(hprof: &Hprof) {
    println!("Label: {}", hprof.header().label().unwrap());
    println!("Id size: {:?}", hprof.header().id_size());
    let ts = chrono::Utc.timestamp_millis(hprof.header().timestamp_millis() as i64);
    println!("Timestamp: {}", ts);
}

fn dump_record_counts(hprof: &Hprof) {
    let counts = record_counts(hprof);

    let mut tag_counts: Vec<(RecordTag, u64)> = counts
        .into_iter()
        .sorted_by_key(|&(_, count)| count)
        .collect::<Vec<(jvm_hprof::RecordTag, u64)>>();

    // highest count on top
    tag_counts.reverse();

    for (tag, count) in tag_counts {
        println!("{:?}: {}", tag, count);
    }
}

fn dump_utf8(hprof: &Hprof) -> Result<(), anyhow::Error> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    wtr.write_record(&[
        "Name id",
        "Contents (valid utf8)",
        "Error (if invalid utf-8)",
        "Contents (base64 of invalid utf8)",
    ])?;

    for u in hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::Utf8)
        .map(|r| r.as_utf_8().unwrap().unwrap())
    {
        match u.text_as_str() {
            Ok(s) => wtr.write_record(&[
                format!("{}", u.name_id()),
                s.to_string(),
                String::from(""),
                String::from(""),
            ]),
            Err(e) => wtr.write_record(&[
                format!("{}", u.name_id()),
                String::from(""),
                format!("{:?}", e),
                base64::encode(u.text()),
            ]),
        }?;
    }

    Ok(())
}

fn dump_load_class(hprof: &Hprof) -> Result<(), anyhow::Error> {
    let utf8 = utf8_by_id(hprof);

    let mut wtr = csv::Writer::from_writer(io::stdout());
    wtr.write_record(&[
        "Class serial",
        "Class obj id",
        "Stack trace serial",
        "Class name id",
        "Class name",
    ])?;

    for l in hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
    {
        wtr.write_record(&[
            format!("{}", l.class_serial()),
            format!("{}", l.class_obj_id()),
            format!("{}", l.stack_trace_serial()),
            format!("{}", l.class_name_id()),
            get_utf8_if_available(&utf8, l.class_name_id()).to_string(),
        ])?;
    }

    Ok(())
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
                    SubRecord::Class(class) => {
                        println!("Obj id: {:#018X} = {}", class.obj_id(), class.obj_id());
                        println!(
                            "Name (via LoadClass): {}",
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
