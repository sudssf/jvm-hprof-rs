use anyhow;
use csv;
use rayon;

use crate::counter::Counter;
use itertools::Itertools;
use jvm_hprof::{heap_dump::*, *};
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::{collections, io, ops};

pub(crate) fn instance_counts(hprof: &Hprof) -> Result<(), anyhow::Error> {
    let accumulated_state: InstanceCountRecordState = hprof
        .records_iter()
        .map(|r| r.unwrap())
        // parallelize -- loses ordering, but we don't need ordering
        // inefficient for the smaller records like LoadClass but great for HeapDumpSegment
        // some quick benchmarking indicates the cost of merging state for tiny records like
        // LoadClass is low since there generally only tens of thousands of them
        .par_bridge()
        .map(|r| {
            let mut state = InstanceCountRecordState::default();

            match r.tag() {
                RecordTag::Utf8 => {
                    let u = r.as_utf_8().unwrap().unwrap();
                    state.utf8.insert(
                        u.name_id(),
                        u.text_as_str().unwrap_or_else(|_| "(invalid UTF-8)"),
                    );
                }
                RecordTag::LoadClass => {
                    let lc = r.as_load_class().unwrap().unwrap();
                    state.load_classes.insert(lc.class_obj_id(), lc);
                }
                RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                    let segment = r.as_heap_dump_segment().unwrap().unwrap();
                    for p in segment.sub_records() {
                        let s = p.unwrap();

                        match s {
                            SubRecord::Class(c) => {
                                state.classes.insert(c.obj_id(), c);
                            }
                            SubRecord::Instance(instance) => {
                                state.instance_counts.increment(instance.class_obj_id())
                            }
                            SubRecord::ObjectArray(obj_array) => state
                                .instance_counts
                                .increment(obj_array.array_class_obj_id()),
                            SubRecord::PrimitiveArray(pa) => {
                                state.prim_array_counts.increment(pa.primitive_type())
                            }

                            _ => {}
                        };
                    }
                }
                _ => {}
            }

            state
        })
        .reduce(
            || InstanceCountRecordState::default(),
            |mut acc, x| {
                acc += x;
                acc
            },
        );

    let mut wtr = csv::Writer::from_writer(io::stdout());
    wtr.write_record(&[
        "Instance count",
        "Instance size (bytes)",
        "Total shallow instance size (bytes)",
        "Class name",
        "Class obj id",
    ])?;

    for (class_obj_id, count) in accumulated_state.instance_counts.iter().sorted_by(
        |(_left_id, left_count), (_right_id, right_count)| {
            // reverse order to put highest counts on top
            Ord::cmp(right_count, left_count)
        },
    ) {
        let class = accumulated_state.classes.get(class_obj_id);
        let load_class = accumulated_state.load_classes.get(class_obj_id);

        let instance_size = class.map(|c| c.instance_size_bytes());
        let total_instance_size = instance_size
            .map(|s| (s as u64) * count)
            .map(|s| s.to_string());

        wtr.write_record(&[
            format!("{}", count),
            instance_size
                .map(|s| s.to_string())
                .unwrap_or_else(|| String::from("")),
            total_instance_size.unwrap_or_else(|| String::from("")),
            load_class
                .map(|lc| lc.class_name_id())
                .and_then(|id| accumulated_state.utf8.get(&id))
                .map(|&s| s)
                .unwrap_or_else(|| &"(unknown utf8)")
                .to_owned(),
            format!("{}", class_obj_id),
        ])?;
    }

    wtr.flush()?;

    Ok(())
}

#[derive(Default)]
struct InstanceCountRecordState<'a> {
    // class obj id -> LoadClass
    load_classes: collections::HashMap<Id, LoadClass>,
    utf8: collections::HashMap<Id, &'a str>,
    classes: collections::HashMap<Id, Class<'a>>,

    // class id -> count
    instance_counts: Counter<Id>,
    prim_array_counts: Counter<PrimitiveArrayType>,
}

/// Merges the underlying data
impl<'a> ops::AddAssign for InstanceCountRecordState<'a> {
    fn add_assign(&mut self, rhs: Self) {
        // union the maps that store records
        self.load_classes.extend(rhs.load_classes.into_iter());
        self.utf8.extend(rhs.utf8.into_iter());
        self.classes.extend(rhs.classes.into_iter());

        // sum the counts for count maps
        self.instance_counts += rhs.instance_counts;
        self.prim_array_counts += rhs.prim_array_counts;
    }
}
