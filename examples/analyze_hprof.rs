use base64;
use chrono;
use chrono::offset::TimeZone;
use clap;
use itertools::Itertools;

use jvm_hprof::{Hprof, RecordTag};
use memmap;
use std::fs;

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
        .subcommand(clap::SubCommand::with_name("dump-utf8"));
    let matches = app.get_matches();

    let file_path = matches.value_of("file").expect("file must be specified");

    let file = fs::File::open(file_path).unwrap();

    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();

    let hprof = jvm_hprof::parse_hprof(&memmap[..]).unwrap();

    match matches.subcommand() {
        ("header", _) => header(&hprof),
        ("tag-counts", _) => tag_counts(&hprof),
        ("dump-utf8", _) => dump_utf8(&hprof),
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
