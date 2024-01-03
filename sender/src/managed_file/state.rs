use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use common::file_part_id::FilePartId;

use super::write_file_atomic;

/// Represents a managed file's state, as a managed file object. This struct can only exist
/// when
#[derive(Debug, PartialEq, Eq)]
pub struct ManagedFileState {
    inner_file: PathBuf,
    remaining_parts: Vec<ManagedFileStatePart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedFileStatePart {
    pub part: FilePartId,
    pub priority: i16,
}

impl ManagedFileState {
    pub fn new_from_part_count(
        part_count: u32,
        result_path: impl Into<PathBuf>,
    ) -> anyhow::Result<Self> {
        let mut parts = Vec::with_capacity(part_count as usize + 1);

        parts.push(ManagedFileStatePart {
            part: FilePartId::Header,
            priority: 0,
        });

        for i in 0..part_count {
            parts.push(ManagedFileStatePart {
                part: FilePartId::Part(i),
                priority: 0,
            });
        }

        let path = result_path.into();
        write_file_atomic(&path, |stream| {
            Ok(serialize_parts_to_stream(&parts, stream)?)
        })?;

        Ok(Self {
            inner_file: path,
            remaining_parts: parts,
        })
    }

    pub fn load_from_file(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        let parts = deserialize_parts_from_file(&mut file)?;

        Ok(Self {
            inner_file: path,
            remaining_parts: parts,
        })
    }

    fn modify_single_part(
        &mut self,
        index: usize,
        data: ManagedFileStatePart,
    ) -> anyhow::Result<()> {
        self.remaining_parts[index] = data.clone();

        let mut file = OpenOptions::new().write(true).open(&self.inner_file)?;
        file.seek(SeekFrom::Start((index * 6) as u64))?;
        serialize_part_to_stream(&data, &mut file)?;

        Ok(())
    }

    pub fn modify_part_priority(
        &mut self,
        part: FilePartId,
        modify: impl FnOnce(&mut i16),
    ) -> anyhow::Result<()> {
        let index = self.remaining_parts.iter().position(|p| p.part == part);
        if let Some(index) = index {
            let mut current = self.remaining_parts[index].priority;
            modify(&mut current);
            self.modify_single_part(
                index,
                ManagedFileStatePart {
                    part,
                    priority: current,
                },
            )?;
        } else {
            tracing::info!(
                "Attempted to modify priority of non-existent part: {:?}",
                part
            );
        }

        Ok(())
    }

    pub fn modify_all_part_priorities(
        &mut self,
        part: FilePartId,
        mut modify: impl FnMut(&mut i16),
    ) -> anyhow::Result<()> {
        for part in self.remaining_parts.iter_mut().filter(|p| p.part == part) {
            modify(&mut part.priority);
        }

        write_file_atomic(&self.inner_file, |stream| {
            Ok(serialize_parts_to_stream(&self.remaining_parts, stream)?)
        })?;

        Ok(())
    }

    pub fn filter_remaining_parts(
        &mut self,
        predicate: impl Fn(&ManagedFileStatePart) -> bool,
    ) -> anyhow::Result<()> {
        self.remaining_parts.retain(predicate);

        write_file_atomic(&self.inner_file, |stream| {
            Ok(serialize_parts_to_stream(&self.remaining_parts, stream)?)
        })?;

        Ok(())
    }

    pub fn remaining_parts(&self) -> &[ManagedFileStatePart] {
        &self.remaining_parts
    }

    pub fn remaining_non_header_parts_len(&self) -> usize {
        let contains_header = self
            .remaining_parts
            .iter()
            .any(|p| p.part == FilePartId::Header);

        if contains_header {
            self.remaining_parts.len() - 1
        } else {
            self.remaining_parts.len()
        }
    }
}

fn serialize_part_to_stream(
    part: &ManagedFileStatePart,
    writer: &mut impl std::io::Write,
) -> anyhow::Result<()> {
    let part_int = part.part.to_index();
    writer.write_all(&part_int.to_le_bytes())?;

    writer.write_all(&part.priority.to_le_bytes())?;

    Ok(())
}

fn serialize_parts_to_stream(
    parts: &[ManagedFileStatePart],
    writer: &mut impl std::io::Write,
) -> std::io::Result<()> {
    for part in parts {
        serialize_part_to_stream(part, writer);
    }

    Ok(())
}

fn deserialize_parts_from_file(reader: &mut File) -> std::io::Result<Vec<ManagedFileStatePart>> {
    let file_len = reader.metadata()?.len();

    if file_len % 6 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "State parts file length is not a multiple of 6",
        ));
    }

    let part_count = file_len / 6;
    let mut parts = Vec::with_capacity(part_count as usize);

    for _ in 0..part_count {
        let mut part_bytes = [0u8; 4];
        reader.read_exact(&mut part_bytes)?;
        let part_int = u32::from_le_bytes(part_bytes);
        let part = FilePartId::from_index(part_int);

        let mut priority_bytes = [0u8; 2];
        reader.read_exact(&mut priority_bytes)?;
        let priority = i16::from_le_bytes(priority_bytes);

        parts.push(ManagedFileStatePart { part, priority });
    }

    Ok(parts)
}

#[cfg(test)]
mod tests {
    use crate::tempdir::TempDirProvider;

    use super::*;

    fn test_changes(
        part_count: u32,
        execute: impl FnOnce(&mut ManagedFileState) -> anyhow::Result<()>,
    ) {
        let folder = TempDirProvider::new_test().create().unwrap();
        let path = folder.path().join("state.bin");

        let mut state = ManagedFileState::new_from_part_count(part_count, path.clone()).unwrap();

        execute(&mut state).unwrap();

        // Backup the state
        let new_state_path = folder.path().join("state.bin.new");
        std::fs::copy(path.clone(), new_state_path.clone()).unwrap();

        // Load the new state
        let new_state = ManagedFileState::load_from_file(new_state_path.clone()).unwrap();

        // Check that the new state is the same as the old one
        assert_eq!(state.remaining_parts(), new_state.remaining_parts());
    }

    #[test]
    fn test_serialize_deserialize() {
        test_changes(100, |_| Ok(()));
    }

    #[test]
    fn test_update_priority() {
        test_changes(100, |state| {
            // 51 because the first part is a header
            assert_eq!(state.remaining_parts()[51].priority, 0);
            state.modify_part_priority(FilePartId::Part(50), |priority| {
                *priority = 100;
            })?;
            assert_eq!(state.remaining_parts()[51].priority, 100);

            Ok(())
        });
    }

    #[test]
    fn test_filter() {
        test_changes(100, |state| {
            assert_eq!(state.remaining_parts().len(), 101);
            state.filter_remaining_parts(|part| part.part != FilePartId::Part(50))?;
            assert_eq!(state.remaining_parts().len(), 100);

            Ok(())
        });
    }

    #[test]
    fn test_update_all_priorities() {
        test_changes(100, |state| {
            assert_eq!(state.remaining_parts()[51].priority, 0);
            state.modify_all_part_priorities(FilePartId::Part(50), |priority| {
                *priority = 100;
            })?;
            assert_eq!(state.remaining_parts()[51].priority, 100);

            Ok(())
        });
    }
}
