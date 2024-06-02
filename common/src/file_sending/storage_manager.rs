use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::{
    control::ControlMessage,
    file_part_id::{FilePartId, FilePartIdRangeInclusive},
};
use anyhow::Context;
use uuid::Uuid;

use super::managed_sending_file::{generate_file_header_from_path, ManagedSendingFile};

#[derive(Debug, Clone)]
pub struct StorageManagerConfig {
    /// The chunk size to use for new files
    pub new_file_chunk_size: u32,

    /// Maximum size of the storage folder in bytes
    pub max_folder_size: Option<u64>,

    /// Trigger a split if >=n chunks worth of disk space can be saved by splitting
    pub split_file_if_n_chunks_saved: Option<u32>,
}

pub struct StorageManager {
    path: PathBuf,
    files: HashMap<Uuid, ManagedSendingFile>,
    config: StorageManagerConfig,
}

impl StorageManager {
    pub fn new(path: PathBuf, config: StorageManagerConfig) -> anyhow::Result<Self> {
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

            let file = ManagedSendingFile::try_read_from_path(&path)?;
            let Some(file) = file else {
                // File was likely ended, but not cleaned up.
                continue;
            };
            files.insert(file.header().id, file);
        }

        Ok(Self {
            path,
            files,
            config,
        })
    }

    pub fn get_file(&self, file_id: Uuid) -> Option<&ManagedSendingFile> {
        self.files.get(&file_id)
    }

    pub fn get_file_mut(&mut self, file_id: Uuid) -> Option<&mut ManagedSendingFile> {
        self.files.get_mut(&file_id)
    }

    pub fn process_control(&mut self, control: ControlMessage) -> anyhow::Result<()> {
        match control {
            ControlMessage::ConfirmPart(confirm) => {
                let file_id = confirm.file_id;
                let part_range = confirm.part_range;

                let file = self.files.get_mut(&file_id);

                let Some(file) = file else {
                    tracing::info!("Received confirmation for non-existent file: {}", file_id);
                    return Ok(());
                };

                file.acknowledge_file_parts(part_range)?;
                if file.is_finished() {
                    self.delete_file_by_id(file_id)?;
                }

                Ok(())
            }
            ControlMessage::DeleteFile(delete) => {
                let file_id = delete.file_id;
                self.delete_file_by_id(file_id)?;
                Ok(())
            }
            ControlMessage::SetFilePriority(set_priority) => {
                let file_id = set_priority.file_id;
                let priority = set_priority.priority;

                let file = self.files.get_mut(&file_id);

                let Some(file) = file else {
                    tracing::info!("Received priority set for non-existent file: {}", file_id);
                    return Ok(());
                };

                file.set_all_parts_priorities(priority)?;

                Ok(())
            }
        }
    }

    pub fn iter_files(&self) -> impl Iterator<Item = &ManagedSendingFile> {
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

    fn delete_file_by_id(&mut self, file_id: Uuid) -> anyhow::Result<()> {
        let file = self.files.remove(&file_id);

        let Some(file) = file else {
            tracing::info!("Received delete request for non-existent file: {}", file_id);
            return Ok(());
        };

        file.delete()?;

        Ok(())
    }

    pub fn delete_parts_until_max_size_reached(&mut self, size: u64) -> anyhow::Result<()> {
        let mut total_size = 0;
        for file in self.files.values() {
            total_size += file.calc_remaining_data_size();
        }

        let mut items = self.iter_remaining_storage_file_parts().collect::<Vec<_>>();
        items.sort_unstable_by(|a, b| cmp_file_storage_part_for_deletion(b, a)); // Sort from highest priority to lowest, we will pop items from the end

        let mut deleted_parts_count = 0;

        while total_size > size {
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
            deleted_parts_count += 1;

            if file.is_finished() {
                self.delete_file_by_id(item.file_id)?;

                // Update the total size with the difference
                let new_file_size = 0;
                total_size = total_size - file_size + new_file_size;
                continue;
            }

            if let Some(split_if_n) = self.config.split_file_if_n_chunks_saved {
                if file.chunks_saved_by_splitting() >= split_if_n {
                    file.trigger_file_split()?;
                }
            }

            // Update the total size with the difference
            let new_file_size = file.calc_remaining_data_size();
            total_size = total_size - file_size + new_file_size;
        }

        tracing::warn!(
            "Deleted {} parts to reach maximum folder size of {}",
            deleted_parts_count,
            size
        );

        Ok(())
    }

    pub fn add_file_from_path(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let header =
            generate_file_header_from_path(path.as_ref(), self.config.new_file_chunk_size)?;
        let destination_path = self.path.join(header.id.to_string());

        if let Some(max_folder_size) = self.config.max_folder_size {
            if max_folder_size < header.size {
                anyhow::bail!(
                    "File size {} exceeds maximum folder size {}",
                    header.size,
                    max_folder_size
                );
            }

            let remaining_size = max_folder_size - header.size;

            self.delete_parts_until_max_size_reached(remaining_size)?;
        }

        let file = ManagedSendingFile::create_new_from_header(destination_path, path, header)?;

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

#[allow(clippy::comparison_chain)]
pub fn cmp_file_storage_part_normal(
    a: &StorageFilePart,
    b: &StorageFilePart,
) -> std::cmp::Ordering {
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

#[allow(clippy::comparison_chain)]
/// Same as the normal cmp_file_storage_part, but with the priority for part index reversed.
pub fn cmp_file_storage_part_for_deletion(
    a: &StorageFilePart,
    b: &StorageFilePart,
) -> std::cmp::Ordering {
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

    // Then, sort by part number. Lower part numbers get higher precedence. Headers are still highest.
    let a_part = match a.part_id {
        FilePartId::Part(part) => part,
        FilePartId::Header => u32::MAX,
    };
    let b_part = match b.part_id {
        FilePartId::Part(part) => part,
        FilePartId::Header => u32::MAX,
    };
    b_part.cmp(&a_part)
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use crate::tempdir::{TempDir, TempDirProvider};

    struct DummyFile {
        _folder: TempDir,
        path: PathBuf,
    }

    fn make_dummy_file(size: u64) -> anyhow::Result<DummyFile> {
        let folder = TempDirProvider::new_for_test().create()?;
        let path = folder.path().join("data.bin");

        let file = File::create(&path)?;
        file.set_len(size)?;

        Ok(DummyFile {
            _folder: folder,
            path,
        })
    }

    fn get_remaining_data_part_count(storage_manager: &StorageManager) -> usize {
        storage_manager
            .iter_remaining_storage_file_parts()
            .filter(|part| part.part_id != FilePartId::Header)
            .count()
    }

    #[test]
    fn test_shrinking_storage_data() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_for_test().create()?;

        let mut storage_manager = StorageManager::new(
            folder.path().clone(),
            StorageManagerConfig {
                split_file_if_n_chunks_saved: Some(5),
                max_folder_size: Some(15),
                new_file_chunk_size: 1,
            },
        )?;

        let file = make_dummy_file(10)?;
        storage_manager.add_file_from_path(&file.path)?;

        assert_eq!(get_remaining_data_part_count(&storage_manager), 10);

        let file = make_dummy_file(10)?;
        storage_manager.add_file_from_path(&file.path)?;

        assert_eq!(
            get_remaining_data_part_count(&storage_manager),
            15 // Would be 20, but we expect it to shrink to 15 as that's the max
        );

        Ok(())
    }

    #[test]
    fn test_shrinking_data_lowest_priority() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_for_test().create()?;

        let mut storage_manager = StorageManager::new(
            folder.path().clone(),
            StorageManagerConfig {
                split_file_if_n_chunks_saved: None,
                max_folder_size: Some(15),
                new_file_chunk_size: 1,
            },
        )?;

        let file = make_dummy_file(5)?;
        storage_manager.add_file_from_path(&file.path)?;

        let file_id = storage_manager.iter_files().next().unwrap().header().id;

        // Decrease priority of all file parts in this file
        let parts = storage_manager
            .iter_remaining_storage_file_parts()
            .collect::<Vec<_>>();
        for part in parts {
            if part.file_id == file_id {
                storage_manager
                    .confirm_file_sent(file_id, part.part_id)
                    .unwrap();
            }
        }

        // Add 2 more files, expecting the first file to still be present with 6 total parts
        let file = make_dummy_file(5)?;
        storage_manager.add_file_from_path(&file.path)?;

        let file = make_dummy_file(5)?;
        storage_manager.add_file_from_path(&file.path)?;

        let old_file = storage_manager
            .iter_files()
            .find(|file| file.header().id == file_id)
            .unwrap();

        assert_eq!(old_file.remaining_parts().len(), 6);
        assert_eq!(get_remaining_data_part_count(&storage_manager), 15);

        // Add another file, expect the first file should only have the header left
        let file = make_dummy_file(5)?;
        storage_manager.add_file_from_path(&file.path)?;

        assert_eq!(get_remaining_data_part_count(&storage_manager), 15);

        let old_file = storage_manager
            .iter_files()
            .find(|file| file.header().id == file_id)
            .unwrap();

        assert_eq!(old_file.remaining_parts().len(), 1);
        assert_eq!(old_file.remaining_parts()[0].part, FilePartId::Header);

        Ok(())
    }
}
