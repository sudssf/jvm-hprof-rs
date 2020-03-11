use crate::util::*;
use jvm_hprof::*;
use std::{collections, io};

/// Write a node for the class with a table of instance field descriptors and static fields
pub fn write_class_node<W: io::Write>(
    class: &MiniClass,
    utf8: &collections::HashMap<Id, String>,
    writer: &mut W,
) -> io::Result<()> {
    // dot supports html-ish tables
    writeln!(writer, "\t{} [shape=box, label=<", class.obj_id).unwrap();
    writeln!(writer, "<TABLE BORDER=\"0\" CELLBORDER=\"1\">").unwrap();

    writeln!(
        writer,
        "<TR><TD COLSPAN=\"2\">{} ({})</TD></TR>",
        escaper::encode_minimal(&class.name),
        escaper::encode_minimal(&format!("{:#018X}", class.obj_id))
    )?;

    writeln!(
        writer,
        "<TR><TD>Instance size (bytes)</TD><TD>{}</TD></TR>",
        class.instance_size_bytes
    )?;

    if class.static_fields.len() > 0 {
        writeln!(writer, "<TR><TD COLSPAN=\"2\">Static fields</TD></TR>")?;
        for sf in &class.static_fields {
            writeln!(
                writer,
                "<TR><TD>{}</TD><TD>{}</TD></TR>",
                escaper::encode_minimal(
                    utf8.get(&sf.name_id())
                        .map(|s| s.as_str())
                        .unwrap_or("(utf8 not found)")
                ),
                escaper::encode_minimal(&format!("{:?}", sf.value()))
            )?;
        }
    }

    // TODO use whole class hierarchy's descriptors
    if class.instance_field_descriptors.len() > 0 {
        writeln!(
            writer,
            "<TR><TD COLSPAN=\"2\">Instance field descriptors</TD></TR>"
        )?;
        for fd in &class.instance_field_descriptors {
            writeln!(
                writer,
                "<TR><TD>{}</TD><TD>{}</TD></TR>",
                escaper::encode_minimal(
                    utf8.get(&fd.name_id())
                        .map(|s| s.as_str())
                        .unwrap_or("(utf8 not found)")
                ),
                escaper::encode_minimal(&format!("{:?}", fd.field_type()))
            )?;
        }
    }

    writeln!(writer, "</TABLE>")?;
    writeln!(writer, "\t>];")?;

    Ok(())
}
