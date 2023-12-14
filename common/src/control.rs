use uuid::Uuid;

use crate::{binary_serialize::BinarySerialize, file_part_id::FilePartId, validity::ValidityCheck};

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlMessage {
    ConfirmPart(ConfirmPart),
    DeleteFile(DeleteFile),
}

impl std::fmt::Display for ControlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlMessage::ConfirmPart(msg) => write!(
                f,
                "ControlMessage::ConfirmPart {{ file_id: {}, part_index: {} }}",
                msg.file_id, msg.part_index,
            ),
            ControlMessage::DeleteFile(msg) => {
                write!(
                    f,
                    "ControlMessage::DeleteFile {{ file_id: {} }}",
                    msg.file_id,
                )
            }
        }
    }
}

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Confirm that a part of a file was successfully received
pub struct ConfirmPart {
    pub file_id: Uuid,
    pub part_index: FilePartId,
}

impl BinarySerialize for ConfirmPart {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(self.file_id.as_bytes())?;
        writer.write_all(&self.part_index.to_index().to_le_bytes())?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        16 + 4
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let file_id = Uuid::from_bytes(id);

        let mut part_bytes = [0u8; 4];
        reader.read_exact(&mut part_bytes)?;
        let part = u32::from_le_bytes(part_bytes);
        let part = FilePartId::from_index(part);

        Ok(ConfirmPart {
            file_id,
            part_index: part,
        })
    }
}

impl ValidityCheck for ConfirmPart {
    fn is_valid(&self) -> bool {
        self.part_index.is_valid()
    }
}

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Delete a file from the ready folder, in case some error happened
pub struct DeleteFile {
    pub file_id: Uuid,
}

impl BinarySerialize for DeleteFile {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(self.file_id.as_bytes())?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        16
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let file_id = Uuid::from_bytes(id);

        Ok(DeleteFile { file_id })
    }
}

impl ValidityCheck for DeleteFile {
    fn is_valid(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_confirm_part_serialization() {
        let msg = ConfirmPart {
            file_id: Uuid::new_v4(),
            part_index: FilePartId::Part(42),
        };

        let mut buf = Vec::new();
        msg.serialize_to_stream(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let deserialized_msg = ConfirmPart::deserialize_from_stream(&mut cursor).unwrap();

        assert_eq!(msg, deserialized_msg);
    }

    #[test]
    fn test_delete_file_serialization() {
        let msg = DeleteFile {
            file_id: Uuid::new_v4(),
        };

        let mut buf = Vec::new();
        msg.serialize_to_stream(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let deserialized_msg = DeleteFile::deserialize_from_stream(&mut cursor).unwrap();

        assert_eq!(msg, deserialized_msg);
    }
}
