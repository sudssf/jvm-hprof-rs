use crate::util::*;
use jvm_hprof::heap_dump::FieldDescriptor;
use jvm_hprof::*;
use std::{collections, io};

/// Write a node for the class with a table of instance field descriptors and static fields.
///
/// `instance_field_descriptors` are the descriptors to show on this type.
/// Depending on how you want it to display, this could be the descriptors for just this type, or
/// all descriptors up through the supertype hierarchy.
///
/// The node name for class id 123 is `class-123`.
/// Static fields have a port `static-field-val-<offset>` assigned to the values for each static field offset in
/// the list of static fields.
/// Instance field descriptors have a port `instance-field-val-<offset>`
pub fn write_class_node<W: io::Write>(
    class: &EzClass,
    instance_field_descriptors: &[FieldDescriptor],
    utf8: &collections::HashMap<Id, &str>,
    writer: &mut W,
) -> io::Result<()> {
    // dot supports html-ish tables
    writeln!(writer, "\t\"class-{}\"[shape=box, label=<", class.obj_id).unwrap();
    writeln!(writer, "<TABLE BORDER=\"0\" CELLBORDER=\"1\">").unwrap();

    writeln!(
        writer,
        "<TR><TD COLSPAN=\"2\">{} ({})</TD></TR>",
        escaper::encode_minimal(&class.name),
        escaper::encode_minimal(&format!("{:#018X}", class.obj_id))
    )?;
    if let Some(super_id) = class.super_class_obj_id {
        writeln!(
            writer,
            "<TR><TD COLSPAN=\"2\">Superclass: {}</TD></TR>",
            escaper::encode_minimal(&format!("{:#018X}", super_id))
        )?;
    }

    writeln!(
        writer,
        "<TR><TD>Instance size (bytes)</TD><TD>{}</TD></TR>",
        class.instance_size_bytes
    )?;

    if class.static_fields.len() > 0 {
        writeln!(writer, "<TR><TD COLSPAN=\"2\">Static fields</TD></TR>")?;
        for (index, sf) in class.static_fields.iter().enumerate() {
            writeln!(
                writer,
                "<TR><TD>{}</TD><TD PORT=\"{}\">{}</TD></TR>",
                escaper::encode_minimal(utf8.get(&sf.name_id()).unwrap_or(&"(utf8 not found)")),
                &format!("static-field-val-{}", index),
                escaper::encode_minimal(&format!("{:?}", sf.value()))
            )?;
        }
    }

    if instance_field_descriptors.len() > 0 {
        writeln!(
            writer,
            "<TR><TD COLSPAN=\"2\">Instance field descriptors</TD></TR>"
        )?;
        for (index, fd) in instance_field_descriptors.iter().enumerate() {
            writeln!(
                writer,
                "<TR><TD>{}</TD><TD PORT=\"{}\">{}</TD></TR>",
                escaper::encode_minimal(utf8.get(&fd.name_id()).unwrap_or(&"(utf8 not found)")),
                &format!("instance-field-val-{}", index),
                escaper::encode_minimal(&format!("{:?}", fd.field_type()))
            )?;
        }
    }

    // Array classes have JVM names starting with [. A class descriptor doesn't say whether or not
    // it's an array class directly, so the only other way we could detect them is looking for
    // classes that have SubRecord::ObjectArray instances, which seems even jankier.
    // We write a separate column for the array contents so that outgoing edges have a specific
    // exit point rather than going from anywhere on the node.

    // TODO Specifically we only care about [L, [[L, etc and not primitive arrays like [Z, [[J, etc
    // since primitive arrays will never be the source of an edge but it probably doesn't do too
    // much harm to add this row for primitive arrays too
    if class.name.starts_with("[") {
        writeln!(
            writer,
            "<TR><TD COLSPAN=\"2\" PORT=\"array-contents\">(array contents)</TD></TR>",
        )?;
    }

    writeln!(writer, "</TABLE>")?;
    writeln!(writer, "\t>];")?;

    Ok(())
}
