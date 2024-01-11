use uuid::Uuid;

use crate::{
    binary_serialize::BinarySerialize, file_part_id::FilePartIdRangeInclusive,
    validity::ValidityCheck,
};

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlMessage {
    ConfirmPart(ConfirmPart),
    DeleteFile(DeleteFile),
    SetFilePriority(SetFilePriority),
}

impl std::fmt::Display for ControlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlMessage::ConfirmPart(msg) => write!(
                f,
                "ControlMessage::ConfirmPart {{ file_id: {}, part_range: {} }}",
                msg.file_id, msg.part_range,
            ),
            ControlMessage::DeleteFile(msg) => {
                write!(
                    f,
                    "ControlMessage::DeleteFile {{ file_id: {} }}",
                    msg.file_id,
                )
            }
            ControlMessage::SetFilePriority(msg) => write!(
                f,
                "ControlMessage::SetFilePriority {{ file_id: {}, priority: {} }}",
                msg.file_id, msg.priority,
            ),
        }
    }
}

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Confirm that a part of a file was successfully received
pub struct ConfirmPart {
    pub file_id: Uuid,
    pub part_range: FilePartIdRangeInclusive,
}

impl BinarySerialize for ConfirmPart {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(self.file_id.as_bytes())?;
        self.part_range.serialize_to_stream(writer)?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        16 + self.part_range.length_when_serialized()
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let file_id = Uuid::from_bytes(id);

        let part_range = FilePartIdRangeInclusive::deserialize_from_stream(reader)?;

        Ok(ConfirmPart {
            file_id,
            part_range,
        })
    }
}

impl ValidityCheck for ConfirmPart {
    fn is_valid(&self) -> bool {
        self.part_range.is_valid()
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

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
/// Set a file's priority (for every part in the file)
pub struct SetFilePriority {
    pub file_id: Uuid,
    pub priority: i16,
}

impl BinarySerialize for SetFilePriority {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(self.file_id.as_bytes())?;
        writer.write_all(&self.priority.to_le_bytes())?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        16 + 2
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let file_id = Uuid::from_bytes(id);

        let mut priority_bytes = [0u8; 2];
        reader.read_exact(&mut priority_bytes)?;
        let priority = i16::from_le_bytes(priority_bytes);

        Ok(SetFilePriority { file_id, priority })
    }
}

impl ValidityCheck for SetFilePriority {
    fn is_valid(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::file_part_id::FilePartId;

    use super::*;

    #[test]
    fn test_confirm_part_serialization() {
        let msg = ConfirmPart {
            file_id: Uuid::new_v4(),
            part_range: FilePartIdRangeInclusive {
                from: FilePartId::from_index(0),
                to: FilePartId::from_index(10),
            },
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
