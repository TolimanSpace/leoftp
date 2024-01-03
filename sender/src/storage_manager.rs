use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Context;
use common::{
    control::ControlMessage,
    file_part_id::{FilePartId, FilePartIdRangeInclusive},
};
use uuid::Uuid;

use super::managed_file::{generate_file_header_from_path, ManagedFile};

pub struct StorageManager {
    path: PathBuf,
    files: HashMap<Uuid, ManagedFile>,
    new_file_chunk_size: u32,
}

impl StorageManager {
    pub fn new(path: PathBuf, new_file_chunk_size: u32) -> anyhow::Result<Self> {
        let folder_files = std::fs::read_dir(&path).context("Failed to read storage folder")?;

        let mut files = HashMap::new();
        for file in folder_files {
            let file = match file {
                Ok(file) => file,
                Err(err) => {
                    tracing::warn!("Failed to read storage folder entry: {}", err);
                    continue;
                }
            };
            let path = file.path();

            if !path.is_dir() {
                continue;
            }

            let file = ManagedFile::try_read_from_path(&path)?;
            let Some(file) = file else {
                // File was likely ended, but not cleaned up.
                continue;
            };
            files.insert(file.header().id, file);
        }

        Ok(Self {
            path,
            files,
            new_file_chunk_size,
        })
    }

    pub fn get_file(&self, file_id: Uuid) -> Option<&ManagedFile> {
        self.files.get(&file_id)
    }

    pub fn get_file_mut(&mut self, file_id: Uuid) -> Option<&mut ManagedFile> {
        self.files.get_mut(&file_id)
    }

    pub fn process_control(&mut self, control: ControlMessage) -> anyhow::Result<()> {
        match control {
            ControlMessage::ConfirmPart(confirm) => {
                let file_id = confirm.file_id;
                let part_index = confirm.part_index;

                let file = self.files.get_mut(&file_id);

                let Some(file) = file else {
                    tracing::info!("Received confirmation for non-existent file: {}", file_id);
                    return Ok(());
                };

                file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(part_index))?;

                Ok(())
            }
            ControlMessage::DeleteFile(delete) => {
                let file_id = delete.file_id;

                let file = self.files.remove(&file_id);

                let Some(file) = file else {
                    tracing::info!("Received delete request for non-existent file: {}", file_id);
                    return Ok(());
                };

                file.delete()?;

                Ok(())
            }
        }
    }

    pub fn iter_files(&self) -> impl Iterator<Item = &ManagedFile> {
        self.files.values()
    }

    pub fn iter_remaining_storage_file_parts(&self) -> impl '_ + Iterator<Item = StorageFilePart> {
        let nested_iter = self.files.values().map(|file| {
            file.remaining_parts().iter().map(|part| StorageFilePart {
                file_id: file.header().id,
                part_id: part.part,
                priority: part.priority,
            })
        });

        nested_iter.flatten()
    }

    pub fn delete_parts_until_size_reached(&mut self, size: u64) -> anyhow::Result<()> {
        let mut total_size = 0;
        for file in self.files.values() {
            total_size += file.calc_remaining_data_size();
        }

        let mut items = self.iter_remaining_storage_file_parts().collect::<Vec<_>>();
        items.sort_unstable_by(|a, b| b.cmp(a)); // Sort from highest priority to lowest, we will pop items from the end

        while total_size < size {
            let item = items.pop();
            let item = match item {
                Some(item) => item,
                None => break,
            };

            let file = self.files.get_mut(&item.file_id);
            let file = match file {
                Some(file) => file,
                None => continue,
            };

            let file_size = file.calc_remaining_data_size();

            // Acknowledge the part to remove it from storage
            file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(item.part_id))?;

            // Update the total size with the difference
            let new_file_size = file.calc_remaining_data_size();
            total_size = total_size - file_size + new_file_size;
        }

        Ok(())
    }

    pub fn add_file_from_path(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let header = generate_file_header_from_path(path.as_ref(), self.new_file_chunk_size)?;
        let destination_path = self.path.join(header.id.to_string());
        let file = ManagedFile::create_new_from_header(destination_path, path, header)?;

        self.files.insert(file.header().id, file);

        Ok(())
    }

    /// Confirms that a file has been sent, decreasing its priority. Not to be confused
    /// with acknowleding files, which deletes their parts.
    pub fn confirm_file_sent(
        &mut self,
        file_id: uuid::Uuid,
        part_id: FilePartId,
    ) -> anyhow::Result<()> {
        let file = self.get_file_mut(file_id);

        let Some(file) = file else {
            tracing::info!(
                "Received file send confirmation for non-existent file: {}",
                file_id
            );
            return Ok(());
        };

        file.decrease_part_priority(part_id)?;

        Ok(())
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageFilePart {
    pub file_id: Uuid,

    pub part_id: FilePartId,
    pub priority: i16,
}

impl std::cmp::Ord for StorageFilePart {
    #[allow(clippy::comparison_chain)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a = self;
        let b = other;

        // Sort by priority first. Higher priority gets higher precedence.
        if a.priority > b.priority {
            return std::cmp::Ordering::Greater;
        } else if a.priority < b.priority {
            return std::cmp::Ordering::Less;
        }

        // Sort by header parts next. Headers always get highest priority.
        let a_header = a.part_id == FilePartId::Header;
        let b_header = b.part_id == FilePartId::Header;
        if a_header && !b_header {
            return std::cmp::Ordering::Greater;
        } else if !a_header && b_header {
            return std::cmp::Ordering::Less;
        }

        // Then, sort by part number. Higher part numbers get higher precedence.
        let a_part = match a.part_id {
            FilePartId::Part(part) => part,
            FilePartId::Header => 0,
        };
        let b_part = match b.part_id {
            FilePartId::Part(part) => part,
            FilePartId::Header => 0,
        };
        a_part.cmp(&b_part)
    }
}

impl std::cmp::PartialOrd for StorageFilePart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
