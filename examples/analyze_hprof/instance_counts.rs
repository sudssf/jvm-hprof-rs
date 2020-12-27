use anyhow;
use csv;
use rayon;

use itertools::Itertools;
use jvm_hprof::{heap_dump::*, *};
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::{collections, hash, io, ops};

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
                                state
                                    .instance_counts
                                    .entry(instance.class_obj_id())
                                    .and_modify(|count| *count += 1)
                                    .or_insert(1_u64);
                            }
                            SubRecord::ObjectArray(obj_array) => {
                                state
                                    .instance_counts
                                    .entry(obj_array.array_class_obj_id())
                                    .and_modify(|count| *count += 1)
                                    .or_insert(1_u64);
                            }
                            SubRecord::PrimitiveArray(pa) => {
                                state
                                    .prim_array_counts
                                    .entry(pa.primitive_type())
                                    .and_modify(|count| *count += 1)
                                    .or_insert(1_u64);
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
    instance_counts: collections::HashMap<Id, u64>,
    prim_array_counts: collections::HashMap<PrimitiveArrayType, u64>,
}

/// Merges the underlying data
impl<'a> ops::AddAssign for InstanceCountRecordState<'a> {
    fn add_assign(&mut self, rhs: Self) {
        // union the maps that store records
        rhs.load_classes.drain_into(&mut self.load_classes);
        rhs.utf8.drain_into(&mut self.utf8);
        rhs.classes.drain_into(&mut self.classes);

        // sum the counts for count maps
        rhs.instance_counts
            .drain_into_sum(&mut self.instance_counts, |l, r| l + r);
        rhs.prim_array_counts
            .drain_into_sum(&mut self.prim_array_counts, |l, r| l + r);
    }
}

trait DrainInto<T> {
    fn drain_into(self, dest: &mut T);
}

/// Insert the entries of `self` into `dest`, overwriting on duplicate keys
impl<K: Eq + hash::Hash, V> DrainInto<collections::HashMap<K, V>> for collections::HashMap<K, V> {
    fn drain_into(self, dest: &mut collections::HashMap<K, V>) {
        self.into_iter().for_each(|(k, v)| {
            dest.insert(k, v);
        });
    }
}

trait DrainIntoSum<T, E, S: Fn(E, E) -> E> {
    /// Drain `self` into `dest`, combining elements of type `E` via sum function `S`
    fn drain_into_sum(self, dest: &mut T, sum: S);
}

/// Impl for maps with u64 values that just adds the values
impl<K: Eq + hash::Hash, S: Fn(u64, u64) -> u64> DrainIntoSum<collections::HashMap<K, u64>, u64, S>
    for collections::HashMap<K, u64>
{
    fn drain_into_sum(self, dest: &mut collections::HashMap<K, u64>, sum: S) {
        self.into_iter().for_each(|(k, v)| {
            dest.entry(k)
                .and_modify(|count| *count = sum(*count, v))
                .or_insert(v);
        })
    }
}
