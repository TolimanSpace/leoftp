use serde::{Deserialize, Serialize};
use std::io;
use uuid::Uuid;

use crate::{binary_serialize::BinarySerialize, validity::ValidityCheck};

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

impl ValidityCheck for FilePartId {
    fn is_valid(&self) -> bool {
        match self {
            FilePartId::Header => true,
            FilePartId::Part(i) => *i != u32::MAX,
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
