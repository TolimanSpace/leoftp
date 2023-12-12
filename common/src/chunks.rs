use uuid::Uuid;

use crate::{binary_serialize::BinarySerialize, header::FilePartId, validity::ValidityCheck};

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq)]
pub struct Chunk {
    pub file_id: Uuid,
    pub part: FilePartId,
    pub data: Vec<u8>,
}

impl BinarySerialize for Chunk {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        let id = self.file_id.as_bytes();
        writer.write_all(id)?;

        let part = self.part.to_index();
        writer.write_all(&part.to_le_bytes())?;

        let len = self.data.len() as u32;
        writer.write_all(&len.to_le_bytes())?;

        writer.write_all(&self.data)?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        16 // UUID
        + 4 // Part index
        + 4 // Data length
        + self.data.len() as u32 // Data
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let file_id = Uuid::from_bytes(id);

        let part = {
            let mut part_bytes = [0u8; 4];
            reader.read_exact(&mut part_bytes)?;
            let part = u32::from_le_bytes(part_bytes);
            FilePartId::from_index(part)
        };

        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes);

        // 1 MiB
        if len > 1048576 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Chunk length {} exceeds 1 MiB", len),
            ));
        }

        let mut data = vec![0u8; len as usize];
        reader.read_exact(&mut data)?;

        Ok(Self {
            file_id,
            part,
            data,
        })
    }
}

impl ValidityCheck for Chunk {
    fn is_valid(&self) -> bool {
        self.data.len() <= 1048576 && self.part.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_chunk_serialization() {
        let file_id = Uuid::new_v4();
        let part = FilePartId::Part(1);
        let data = vec![0, 1, 2, 3, 4, 5];

        let chunk = Chunk {
            file_id,
            part,
            data: data.clone(),
        };

        let mut buffer = Cursor::new(Vec::new());
        chunk.serialize_to_stream(&mut buffer).unwrap();

        buffer.seek(SeekFrom::Start(0)).unwrap();
        let deserialized_chunk = Chunk::deserialize_from_stream(&mut buffer).unwrap();

        assert_eq!(chunk.file_id, deserialized_chunk.file_id);
        assert_eq!(chunk.part, deserialized_chunk.part);
        assert_eq!(chunk.data, deserialized_chunk.data);
    }
}
