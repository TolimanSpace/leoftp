use serde::{Deserialize, Serialize};
use std::io;
use uuid::Uuid;

use crate::binary_serialize::BinarySerialize;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum FilePartId {
    Header,
    Part(u32),
}

#[cfg(feature = "fuzzing")]
impl<'a> arbitrary::Arbitrary<'a> for FilePartId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FilePartId::from_index(u.arbitrary::<u32>()?))
    }
}

impl FilePartId {
    pub fn from_string(s: &str) -> Option<Self> {
        if s == "header" {
            Some(FilePartId::Header)
        } else {
            s.parse::<u32>().ok().map(FilePartId::Part)
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            FilePartId::Header => "header".to_string(),
            FilePartId::Part(i) => i.to_string(),
        }
    }

    pub fn to_index(&self) -> u32 {
        match self {
            FilePartId::Header => u32::MAX,
            FilePartId::Part(i) => *i,
        }
    }

    pub fn from_index(i: u32) -> Self {
        if i == u32::MAX {
            FilePartId::Header
        } else {
            FilePartId::Part(i)
        }
    }
}

impl Serialize for FilePartId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.to_index())
    }
}

impl<'de> Deserialize<'de> for FilePartId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let index = u32::deserialize(deserializer)?;
        Ok(FilePartId::from_index(index))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hash(pub u64);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
pub struct FileHeaderData {
    pub id: Uuid,
    pub name: String,
    pub date: i64, // Number of nanoseconds since the unix epoch
    pub part_count: u32,
    pub size: u64,
}

impl FileHeaderData {
    pub fn serialize_to_json_stream(&self, stream: impl std::io::Write) -> io::Result<()> {
        serde_json::to_writer(stream, self)?;
        Ok(())
    }

    pub fn deserialize_from_json_stream(stream: impl std::io::Read) -> io::Result<Self> {
        let data = serde_json::from_reader(stream)?;
        Ok(data)
    }
}

impl BinarySerialize for FileHeaderData {
    fn serialize_to_stream<W: std::io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let id = self.id.as_bytes();
        writer.write_all(id)?;

        let name_len = self.name.len() as u32;
        writer.write_all(&name_len.to_le_bytes())?;

        let name = self.name.as_bytes();
        writer.write_all(name)?;

        let date = self.date.to_le_bytes();
        writer.write_all(&date)?;

        let part_count = self.part_count.to_le_bytes();
        writer.write_all(&part_count)?;

        let size = self.size.to_le_bytes();
        writer.write_all(&size)?;

        Ok(())
    }

    fn deserialize_from_stream<R: std::io::Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut id = [0u8; 16];
        reader.read_exact(&mut id)?;
        let id = Uuid::from_bytes(id);

        let mut name_len_bytes = [0u8; 4];
        reader.read_exact(&mut name_len_bytes)?;
        let name_len = u32::from_le_bytes(name_len_bytes);

        if name_len > 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Header name length is too long",
            ));
        }

        let mut name = vec![0u8; name_len as usize];
        reader.read_exact(&mut name)?;
        let name = String::from_utf8(name).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Failed to parse header name as utf8",
            )
        })?;

        let mut date_bytes = [0u8; 8];
        reader.read_exact(&mut date_bytes)?;
        let date = i64::from_le_bytes(date_bytes);

        let mut part_count_bytes = [0u8; 4];
        reader.read_exact(&mut part_count_bytes)?;
        let part_count = u32::from_le_bytes(part_count_bytes);

        let mut size_bytes = [0u8; 8];
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek, SeekFrom};

    // Test the BinarySerialize serialization and deserialization

    #[test]
    fn test_binary_serialize() {
        let header = FileHeaderData {
            id: Uuid::new_v4(),
            name: "test.txt".to_string(),
            date: 1629867600,
            part_count: 1,
            size: 1024,
        };

        let mut buffer = Cursor::new(Vec::new());
        header.serialize_to_stream(&mut buffer).unwrap();
        buffer.seek(SeekFrom::Start(0)).unwrap();

        let deserialized_header = FileHeaderData::deserialize_from_stream(&mut buffer).unwrap();

        assert_eq!(header.id, deserialized_header.id);
        assert_eq!(header.name, deserialized_header.name);
        assert_eq!(header.date, deserialized_header.date);
        assert_eq!(header.part_count, deserialized_header.part_count);
        assert_eq!(header.size, deserialized_header.size);
    }
}
