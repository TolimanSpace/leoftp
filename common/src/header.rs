use std::io;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub enum FilePartId {
    Header,
    Part(u64),
}

impl FilePartId {
    pub fn from_string(s: &str) -> Option<Self> {
        if s == "header" {
            Some(FilePartId::Header)
        } else {
            s.parse::<u64>().ok().map(|i| FilePartId::Part(i))
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            FilePartId::Header => "header".to_string(),
            FilePartId::Part(i) => i.to_string(),
        }
    }

    pub fn to_index(&self) -> u32 {
        match self {
            FilePartId::Header => u32::MAX,
            FilePartId::Part(i) => *i as u32,
        }
    }

    pub fn from_index(i: u32) -> Self {
        if i == u32::MAX {
            FilePartId::Header
        } else {
            FilePartId::Part(i as u64)
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileHeaderData {
    pub id: Uuid,
    pub name: String,
    pub date: String,
    pub part_count: u64,
    pub size: u64,
}

impl FileHeaderData {
    pub fn serialize_to_stream(&self, stream: &mut impl std::io::Write) -> bincode::Result<()> {
        bincode::serialize_into(stream, self)?;
        Ok(())
    }

    pub fn deserialize_from_stream(stream: &mut impl std::io::Read) -> bincode::Result<Self> {
        let data = bincode::deserialize_from(stream)?;
        Ok(data)
    }

    pub fn serialize_to_json_stream(&self, stream: &mut impl std::io::Write) -> io::Result<()> {
        serde_json::to_writer(stream, self)?;
        Ok(())
    }

    pub fn deserialize_from_json_stream(stream: &mut impl std::io::Read) -> io::Result<Self> {
        let data = serde_json::from_reader(stream)?;
        Ok(data)
    }
}
