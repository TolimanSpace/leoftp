use uuid::Uuid;

use crate::{binary_serialize::BinarySerialize, header::FilePartId, validity::ValidityCheck};

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlMessage {
    // Confirm that a part of a file was successfully received
    ConfirmPart {
        file_id: Uuid,
        part_index: FilePartId,
    },

    // Delete a file from the ready folder, in case some error happened
    DeleteFile {
        file_id: Uuid,
    },

    // The sending loop pauses until control messages come, so if we just want it to unpause without
    // any other messages, then we can send this
    Continue,
}

impl BinarySerialize for ControlMessage {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        match self {
            ControlMessage::ConfirmPart {
                file_id,
                part_index,
            } => {
                writer.write_all(&[0])?;
                writer.write_all(file_id.as_bytes())?;
                writer.write_all(&part_index.to_index().to_le_bytes())?;
            }
            ControlMessage::DeleteFile { file_id } => {
                writer.write_all(&[1])?;
                writer.write_all(file_id.as_bytes())?;
            }
            ControlMessage::Continue => {
                writer.write_all(&[2])?;
            }
        }

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        1 + match self {
            ControlMessage::ConfirmPart { .. } => 16 + 4,
            ControlMessage::DeleteFile { .. } => 16,
            ControlMessage::Continue => 0,
        }
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut msg_type = [0u8; 1];
        reader.read_exact(&mut msg_type)?;

        match msg_type[0] {
            0 => {
                let mut id = [0u8; 16];
                reader.read_exact(&mut id)?;
                let file_id = Uuid::from_bytes(id);

                let mut part_bytes = [0u8; 4];
                reader.read_exact(&mut part_bytes)?;
                let part = u32::from_le_bytes(part_bytes);
                let part = FilePartId::from_index(part);

                Ok(ControlMessage::ConfirmPart {
                    file_id,
                    part_index: part,
                })
            }
            1 => {
                let mut id = [0u8; 16];
                reader.read_exact(&mut id)?;
                let file_id = Uuid::from_bytes(id);

                Ok(ControlMessage::DeleteFile { file_id })
            }
            2 => Ok(ControlMessage::Continue),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid message type",
            )),
        }
    }
}

impl ValidityCheck for ControlMessage {
    fn is_valid(&self) -> bool {
        match self {
            ControlMessage::ConfirmPart { part_index, .. } => part_index.is_valid(),
            ControlMessage::DeleteFile { .. } => true,
            ControlMessage::Continue => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_control_messages_serialization() {
        let messages = vec![
            ControlMessage::ConfirmPart {
                file_id: Uuid::new_v4(),
                part_index: FilePartId::Part(42),
            },
            ControlMessage::DeleteFile {
                file_id: Uuid::new_v4(),
            },
            ControlMessage::Continue,
        ];

        let mut buf = Vec::new();
        for msg in &messages {
            msg.serialize_to_stream(&mut buf).unwrap();
        }

        let mut cursor = Cursor::new(buf);
        let mut deserialized_messages = Vec::new();
        while cursor.position() < cursor.get_ref().len() as u64 {
            let msg = ControlMessage::deserialize_from_stream(&mut cursor).unwrap();
            deserialized_messages.push(msg);
        }

        assert_eq!(messages, deserialized_messages);
    }
}
