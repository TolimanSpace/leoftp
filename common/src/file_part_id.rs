use serde::{Deserialize, Serialize};

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
            FilePartId::Header => 0,
            FilePartId::Part(i) => *i + 1,
        }
    }

    pub fn from_index(i: u32) -> Self {
        if i == 0 {
            FilePartId::Header
        } else {
            FilePartId::Part(i - 1)
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
        serializer.serialize_i32(self.to_index() as i32)
    }
}

impl<'de> Deserialize<'de> for FilePartId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let index = i32::deserialize(deserializer)?;
        Ok(FilePartId::from_index(index as u32))
    }
}

impl std::fmt::Display for FilePartId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilePartId::Header => write!(f, "header"),
            FilePartId::Part(i) => write!(f, "{}", i),
        }
    }
}

impl PartialOrd for FilePartId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FilePartId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_index().cmp(&other.to_index())
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
pub struct FilePartIdRangeInclusive {
    pub from: FilePartId,
    pub to: FilePartId,
}

impl FilePartIdRangeInclusive {
    pub fn new(from: FilePartId, to: FilePartId) -> Self {
        assert!(from <= to);

        Self { from, to }
    }

    pub fn new_single(id: FilePartId) -> Self {
        Self { from: id, to: id }
    }

    pub fn contains(&self, id: FilePartId) -> bool {
        self.from <= id && id <= self.to
    }

    /// Iterate over all parts in the inclusive range. Header is the -1th part.
    pub fn iter_parts(&self) -> impl Iterator<Item = FilePartId> {
        let from_index = self.from.to_index();
        let to_index = self.to.to_index();

        (from_index..=to_index).map(FilePartId::from_index)
    }
}

impl BinarySerialize for FilePartIdRangeInclusive {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        let from_int = self.from.to_index();
        let to_int = self.to.to_index();

        writer.write_all(&from_int.to_be_bytes())?;
        writer.write_all(&to_int.to_be_bytes())?;

        Ok(())
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self> {
        let mut from_bytes = [0u8; 4];
        reader.read_exact(&mut from_bytes)?;
        let from_int = u32::from_be_bytes(from_bytes);
        let from = FilePartId::from_index(from_int);

        let mut to_bytes = [0u8; 4];
        reader.read_exact(&mut to_bytes)?;
        let to_int = u32::from_be_bytes(to_bytes);
        let to = FilePartId::from_index(to_int);

        if from > to {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid FilePartIdRangeInclusive: from {} > to {}",
                    from, to
                ),
            ));
        }

        Ok(Self { from, to })
    }

    fn length_when_serialized(&self) -> u32 {
        8
    }
}

impl ValidityCheck for FilePartIdRangeInclusive {
    fn is_valid(&self) -> bool {
        self.from.is_valid() && self.to.is_valid() && self.from <= self.to
    }
}

impl std::fmt::Display for FilePartIdRangeInclusive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {}]", self.from, self.to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_part_id_range_inclusive_iter_parts() {
        let range = FilePartIdRangeInclusive::new(FilePartId::Header, FilePartId::Part(3));
        let parts: Vec<_> = range.iter_parts().collect();
        assert_eq!(
            parts,
            vec![
                FilePartId::Header,
                FilePartId::Part(0),
                FilePartId::Part(1),
                FilePartId::Part(2),
                FilePartId::Part(3)
            ]
        );

        let range = FilePartIdRangeInclusive::new(FilePartId::Part(3), FilePartId::Part(3));
        let parts: Vec<_> = range.iter_parts().collect();
        assert_eq!(parts, vec![FilePartId::Part(3)]);

        let range = FilePartIdRangeInclusive::new(FilePartId::Part(3), FilePartId::Part(5));
        let parts: Vec<_> = range.iter_parts().collect();
        assert_eq!(
            parts,
            vec![
                FilePartId::Part(3),
                FilePartId::Part(4),
                FilePartId::Part(5)
            ]
        );

        let range = FilePartIdRangeInclusive::new_single(FilePartId::Part(3));
        let parts: Vec<_> = range.iter_parts().collect();
        assert_eq!(parts, vec![FilePartId::Part(3)]);

        let range = FilePartIdRangeInclusive::new_single(FilePartId::Header);
        let parts: Vec<_> = range.iter_parts().collect();
        assert_eq!(parts, vec![FilePartId::Header]);
    }
}
