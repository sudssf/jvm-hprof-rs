use crate::util::*;
use jvm_hprof::{heap_dump::*, *};

use std::io::Write;
use std::{fs, path};

pub fn class_hierarchy_dot(hprof: &Hprof, output: &path::Path) {
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
