use crate::util::*;
use jvm_hprof::{heap_dump::*, *};
use std::collections;

pub fn dump_objects(hprof: &Hprof) {
    // class obj id -> LoadClass
    let mut load_classes = collections::HashMap::new();
    // name id -> String
    let mut utf8 = collections::HashMap::new();

    let mut classes: collections::HashMap<Id, EzClass> = collections::HashMap::new();
    // instance obj id to class obj id
    // TODO if this gets big, could use lmdb or similar to get it off-heap
    let mut obj_id_to_class_obj_id: collections::HashMap<Id, Id> = collections::HashMap::new();
    let mut prim_array_obj_id_to_type = collections::HashMap::new();

    let missing_utf8 = "(missing utf8)";

    // build obj -> class and class id -> class metadata maps

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
                        SubRecord::Instance(instance) => {
                            obj_id_to_class_obj_id
                                .insert(instance.obj_id(), instance.class_obj_id());
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            obj_id_to_class_obj_id
                                .insert(obj_array.obj_id(), obj_array.array_class_obj_id());
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            prim_array_obj_id_to_type.insert(pa.obj_id(), pa.primitive_type());
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

    let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        SubRecord::Class(class) => {
                            let mc = match classes.get(&class.obj_id()) {
                                None => panic!("Could not find class {}", class.obj_id()),
                                Some(c) => c,
                            };

                            println!("\nid {}: class {}", class.obj_id(), mc.name);
                            for sf in &mc.static_fields {
                                let field_name =
                                    utf8.get(&sf.name_id()).unwrap_or_else(|| &missing_utf8);

                                print_field_val(
                                    &sf.value(),
                                    field_name,
                                    sf.field_type(),
                                    &obj_id_to_class_obj_id,
                                    &classes,
                                    &prim_array_obj_id_to_type,
                                );
                            }
                        }
                        SubRecord::Instance(instance) => {
                            let mc = match classes.get(&instance.class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                ),
                                Some(c) => c,
                            };

                            println!("\nid {}: {}", instance.obj_id(), mc.name);

                            let field_descriptors = class_instance_field_descriptors
                                .get(&instance.class_obj_id())
                                .expect("Should have all classes available");

                            let mut field_val_input: &[u8] = instance.fields();
                            for fd in field_descriptors.iter() {
                                let (input, field_val) = fd
                                    .field_type()
                                    .parse_value(field_val_input, hprof.header().id_size())
                                    .unwrap();
                                field_val_input = input;

                                let field_name =
                                    utf8.get(&fd.name_id()).unwrap_or_else(|| &missing_utf8);

                                print_field_val(
                                    &field_val,
                                    field_name,
                                    fd.field_type(),
                                    &obj_id_to_class_obj_id,
                                    &classes,
                                    &prim_array_obj_id_to_type,
                                );
                            }
                        }
                        SubRecord::ObjectArray(oa) => {
                            let mc = match classes.get(&oa.array_class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    oa.array_class_obj_id(),
                                    oa.obj_id()
                                ),
                                Some(c) => c,
                            };

                            println!("\nid {}: {} = [", oa.obj_id(), mc.name);

                            for pr in oa.elements(hprof.header().id_size()) {
                                match pr.unwrap() {
                                    Some(id) => {
                                        let element_class_name = obj_id_to_class_obj_id
                                            .get(&id)
                                            .and_then(|class_id| classes.get(class_id))
                                            .map(|c| c.name)
                                            .unwrap_or_else(|| "(could not resolve class)");

                                        println!("  - id {}: {}", id, element_class_name);
                                    }
                                    None => {
                                        println!("  - null");
                                    }
                                }
                            }

                            println!("]");
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            print!(
                                "\n{}: {}[] = [",
                                pa.obj_id(),
                                pa.primitive_type().java_type_name()
                            );

                            match pa.primitive_type() {
                                PrimitiveArrayType::Boolean => {
                                    pa.booleans()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Char => {
                                    pa.chars()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Float => {
                                    pa.floats()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Double => {
                                    pa.doubles()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Byte => {
                                    pa.bytes()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{:#X}, ", e));
                                }
                                PrimitiveArrayType::Short => {
                                    pa.shorts()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Int => {
                                    pa.ints()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Long => {
                                    pa.longs()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                            }

                            println!("]");
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        });
}

fn print_field_val(
    field_val: &FieldValue,
    field_name: &str,
    field_type: FieldType,
    obj_id_to_class_obj_id: &collections::HashMap<Id, Id>,
    classes: &collections::HashMap<Id, EzClass>,
    prim_array_obj_id_to_type: &collections::HashMap<Id, PrimitiveArrayType>,
) {
    match field_val {
        FieldValue::ObjectId(Some(field_ref_id)) => {
            obj_id_to_class_obj_id
                .get(&field_ref_id)
                .map(|class_obj_id| {
                    println!(
                        "  - {} = id {} ({})",
                        field_name,
                        field_ref_id,
                        classes
                            .get(class_obj_id)
                            .map(|c| c.name)
                            .unwrap_or("(class not found)"),
                    );
                })
                .or_else(|| {
                    prim_array_obj_id_to_type
                        .get(&field_ref_id)
                        .map(|prim_type| {
                            println!(
                                "  - {} = id {} ({}[])",
                                field_name,
                                field_ref_id,
                                prim_type.java_type_name()
                            );
                        })
                })
                .or_else(|| {
                    classes.get(&field_ref_id).map(|dest_class| {
                        println!(
                            "  - {} = id {} (class {})",
                            field_name, field_ref_id, dest_class.name
                        );
                    })
                })
                .unwrap_or_else(|| {
                    println!(
                        "  - {} = id {} (type for obj id not found)",
                        field_name, field_ref_id
                    );
                });
        }
        FieldValue::ObjectId(None) => {
            println!("  - {} = null", field_name,);
        }
        FieldValue::Boolean(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Char(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Float(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Double(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Byte(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Short(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Int(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Long(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
    }
}
