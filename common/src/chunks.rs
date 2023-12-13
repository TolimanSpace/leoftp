use std::io;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{binary_serialize::BinarySerialize, header::FilePartId, validity::ValidityCheck};

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq)]
pub enum Chunk {
    Data(DataChunk),
    Header(HeaderChunk),
}

impl Chunk {
    pub fn file_id(&self) -> Uuid {
        match self {
            Chunk::Data(data_chunk) => data_chunk.file_id,
            Chunk::Header(header_chunk) => header_chunk.id,
        }
    }

    pub fn part_index(&self) -> FilePartId {
        match self {
            Chunk::Data(data_chunk) => FilePartId::Part(data_chunk.part),
            Chunk::Header(_) => FilePartId::Header,
        }
    }
}

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq)]
pub struct DataChunk {
    pub file_id: Uuid,
    pub part: u32,
    pub data: Vec<u8>,
}

impl BinarySerialize for DataChunk {
    fn serialize_to_stream(&self, writer: &mut impl io::Write) -> io::Result<()> {
        let id = self.file_id.as_bytes();
        writer.write_all(id)?;

        let part = self.part;
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

    fn deserialize_from_stream(reader: &mut impl io::Read) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let file_id = Uuid::from_bytes(id);

        let part = {
            let mut part_bytes = [0u8; 4];
            reader.read_exact(&mut part_bytes)?;
            u32::from_le_bytes(part_bytes)
        };

        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes);

        // 1 MiB
        if len > 1048576 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("DataChunk length {} exceeds 1 MiB", len),
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

impl ValidityCheck for DataChunk {
    fn is_valid(&self) -> bool {
        self.data.len() <= 1048576 && FilePartId::Part(self.part).is_valid()
    }
}

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeaderChunk {
    pub id: Uuid,
    pub name: String,
    pub date: i64,
    pub part_count: u32,
    pub size: u64,
}

impl BinarySerialize for HeaderChunk {
    fn serialize_to_stream(&self, writer: &mut impl io::Write) -> io::Result<()> {
        writer.write_all(self.id.as_bytes())?;

        let name_bytes = self.name.as_bytes();
        let name_len = name_bytes.len() as u16;
        writer.write_all(&name_len.to_le_bytes())?;

        writer.write_all(name_bytes)?;

        writer.write_all(&self.date.to_le_bytes())?;
        writer.write_all(&self.part_count.to_le_bytes())?;
        writer.write_all(&self.size.to_le_bytes())?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        16 // id
        + 2 // name_len
        + self.name.len() as u32 // name
        + 8 // date
        + 4 // part_count
        + 8 // size
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> io::Result<Self> {
        let mut id_bytes = [0; 16];
        reader.read_exact(&mut id_bytes)?;
        let id = Uuid::from_bytes(id_bytes);

        let mut name_len_bytes = [0; 2];
        reader.read_exact(&mut name_len_bytes)?;
        let name_len = u16::from_le_bytes(name_len_bytes);

        let mut name_bytes = vec![0; name_len as usize];
        reader.read_exact(&mut name_bytes)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut date_bytes = [0; 8];
        reader.read_exact(&mut date_bytes)?;
        let date = i64::from_le_bytes(date_bytes);

        let mut part_count_bytes = [0; 4];
        reader.read_exact(&mut part_count_bytes)?;
        let part_count = u32::from_le_bytes(part_count_bytes);

        let mut size_bytes = [0; 8];
        reader.read_exact(&mut size_bytes)?;
        let size = u64::from_le_bytes(size_bytes);

        Ok(Self {
            id,
            name,
            date,
            part_count,
            size,
        })
    }
}

impl ValidityCheck for HeaderChunk {
    fn is_valid(&self) -> bool {
        self.name.len() <= 65535
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_chunk_serialization() {
        let chunk = DataChunk {
            file_id: Uuid::new_v4(),
            part: 1,
            data: vec![0, 1, 2, 3, 4, 5],
        };

        let mut buffer = Cursor::new(Vec::new());
        chunk.serialize_to_stream(&mut buffer).unwrap();

        buffer.seek(SeekFrom::Start(0)).unwrap();
        let deserialized_chunk = DataChunk::deserialize_from_stream(&mut buffer).unwrap();

        assert_eq!(chunk, deserialized_chunk);
    }

    #[test]
    fn test_header_serialization() {
        let header = HeaderChunk {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            date: 123456789,
            part_count: 42,
            size: 123456789,
        };

        let mut buffer = Cursor::new(Vec::new());
        header.serialize_to_stream(&mut buffer).unwrap();

        buffer.seek(SeekFrom::Start(0)).unwrap();
        let deserialized_header = HeaderChunk::deserialize_from_stream(&mut buffer).unwrap();

        assert_eq!(header, deserialized_header);
    }
}
