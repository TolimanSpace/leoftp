use std::{collections::HashMap, path::PathBuf};

use common::{
    control::ControlMessage,
    file_part_id::{FilePartId, FilePartIdRangeInclusive},
};
use uuid::Uuid;

use super::managed_file::ManagedFile;

pub struct StorageManager {
    path: PathBuf,
    files: HashMap<Uuid, ManagedFile>,
    total_storage: u64,
}

impl StorageManager {
    // pub fn new(path: PathBuf) -> Self {
    //     Self { files }
    // }

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
}
