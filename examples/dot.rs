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
    class: &MiniClass,
    instance_field_descriptors: &[FieldDescriptor],
    utf8: &collections::HashMap<Id, String>,
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
                escaper::encode_minimal(
                    utf8.get(&sf.name_id())
                        .map(|s| s.as_str())
                        .unwrap_or("(utf8 not found)")
                ),
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
                escaper::encode_minimal(
                    utf8.get(&fd.name_id())
                        .map(|s| s.as_str())
                        .unwrap_or("(utf8 not found)")
                ),
                &format!("instance-field-val-{}", index),
                escaper::encode_minimal(&format!("{:?}", fd.field_type()))
            )?;
        }
    }

    writeln!(writer, "</TABLE>")?;
    writeln!(writer, "\t>];")?;

    Ok(())
}
