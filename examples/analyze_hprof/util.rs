use jvm_hprof::{heap_dump::*, *};
use std::collections;

/// A somewhat more convenient representation of a Class together with its name from the corresponding LoadClass
pub struct EzClass<'a> {
    pub obj_id: Id,
    pub super_class_obj_id: Option<Id>,
    pub static_fields: Vec<StaticFieldEntry>,
    /// Just the instance fields for this class, not including superclasses
    pub instance_field_descriptors: Vec<FieldDescriptor>,
    pub name: &'a str,
    pub instance_size_bytes: u32,
}

impl<'a> EzClass<'a> {
    pub(crate) fn from_class<'i>(
        c: &Class,
        load_classes: &'i collections::HashMap<Id, LoadClass>,
        utf8: &'i collections::HashMap<Id, &'a str>,
    ) -> EzClass<'a>
    // input collections maybe shorter lived than the str data they represent
    where
        'a: 'i,
    {
        EzClass {
            obj_id: c.obj_id(),
            super_class_obj_id: c.super_class_obj_id(),
            static_fields: c.static_fields().map(|r| r.unwrap()).collect(),
            instance_field_descriptors: c
                .instance_field_descriptors()
                .map(|r| r.unwrap())
                .collect(),
            name: load_classes
                .get(&c.obj_id())
                .and_then(|lc: &LoadClass| utf8.get(&lc.class_name_id()))
                .map(|s| s.to_owned())
                .unwrap_or_else(|| "missing LoadClass"),
            instance_size_bytes: c.instance_size_bytes(),
        }
    }
}

pub fn utf8_by_id<'a>(hprof: &'a Hprof) -> collections::HashMap<Id, Utf8<'a>> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::Utf8)
        .map(|r| r.as_utf_8().unwrap().unwrap())
        .map(|u| (u.name_id(), u))
        .collect::<collections::HashMap<_, _>>()
}

pub fn utf8_strings_by_id<'a>(hprof: &'a Hprof) -> collections::HashMap<Id, &'a str> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::Utf8)
        .map(|r| r.as_utf_8().unwrap().unwrap())
        .map(|u| {
            (
                u.name_id(),
                u.text_as_str().unwrap_or_else(|_| "(invalid UTF-8)"),
            )
        })
        .collect::<collections::HashMap<_, _>>()
}

pub fn classes_by_serial(hprof: &Hprof) -> collections::HashMap<Serial, LoadClass> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .map(|f| (f.class_serial(), f))
        .collect::<collections::HashMap<_, _>>()
}

pub fn classes_by_obj_id(hprof: &Hprof) -> collections::HashMap<Id, LoadClass> {
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .filter(|r| r.tag() == jvm_hprof::RecordTag::LoadClass)
        .map(|r| r.as_load_class().unwrap().unwrap())
        .map(|f| (f.class_obj_id(), f))
        .collect::<collections::HashMap<_, _>>()
}

pub fn get_utf8_if_available<'a>(utf8: &'a collections::HashMap<Id, Utf8<'a>>, id: Id) -> &'a str {
    utf8.get(&id)
        .map(|u| u.text_as_str().unwrap_or("(invalid utf8)"))
        .unwrap_or("(utf8 not found)")
}

/// Counts for each record tag, with zero-count entries for missing tags.
pub fn record_counts(hprof: &Hprof) -> collections::HashMap<RecordTag, u64> {
    // start with zero counts for all types
    let mut counts = RecordTag::iter()
        .map(|r| (r, 0_u64))
        .collect::<collections::HashMap<_, _>>();

    // overwrite zeros with real counts, if any
    hprof
        .records_iter()
        .map(|r| r.unwrap().tag())
        .for_each(|tag| {
            counts.entry(tag).and_modify(|c| *c += 1).or_insert(1);
        });

    counts
}

/// Walk the class hierarchy and build a per-class list of field descriptors, root type's fields last.
///
/// Classes are not laid down super class first, so have to wait until the end to be able to
/// navigate the class hierarchy
pub fn build_type_hierarchy_field_descriptors(
    classes: &collections::HashMap<Id, EzClass>,
) -> collections::HashMap<Id, Vec<FieldDescriptor>> {
    // class obj id => vec of all instance field descriptors (the class, then super class, then ...)
    let mut class_instance_field_descriptors = collections::HashMap::new();

    for (id, mc) in classes {
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

        class_instance_field_descriptors.insert(*id, field_descriptors);
    }

    class_instance_field_descriptors
}
