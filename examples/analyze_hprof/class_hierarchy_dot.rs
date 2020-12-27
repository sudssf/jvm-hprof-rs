use crate::dot;
use crate::util::*;
use jvm_hprof::{heap_dump::*, *};

use std::io::Write;
use std::{fs, path};

pub fn class_hierarchy_dot(hprof: &Hprof, output: &path::Path) {
    let utf8 = utf8_strings_by_id(hprof);
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
                    SubRecord::Class(class) => {
                        let class = EzClass::from_class(&class, &load_classes_by_obj_id, &utf8);
                        // here, only show each type's own instance fields
                        dot::write_class_node(
                            &class,
                            &class.instance_field_descriptors,
                            &utf8,
                            &mut dot,
                        )
                        .unwrap();

                        if let Some(super_id) = class.super_class_obj_id {
                            writeln!(
                                dot,
                                "\t\"class-{}\" -> \"class-{}\";",
                                class.obj_id, super_id
                            )
                            .unwrap();
                        }
                    }
                    _ => {}
                }
            }
        });

    writeln!(dot, "}}").unwrap();
}
