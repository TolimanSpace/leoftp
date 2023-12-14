use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    thread::JoinHandle,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use common::{
    chunks::{Chunk, HeaderChunk},
    control::ControlMessage,
    file_part_id::FilePartId,
};
use crossbeam_channel::{Receiver, Sender};
use uuid::Uuid;

use crate::ready_file::{is_file_fully_confirmed, mark_part_as_sent};

use self::sender_loop::spawn_chunk_sender;

mod sender_loop;

pub struct ReadyFolderThreads {
    join_handle: JoinHandle<()>,
}

impl ReadyFolderThreads {
    pub fn spawn(
        path: PathBuf,
        new_file_rcv: Receiver<PathBuf>,
        chunks_snd: Sender<Chunk>,
        confirmation_rcv: Receiver<ControlMessage>,
        file_part_size: u32,
    ) -> Self {
        let handler = ReadyFolderHandler {
            path,
            file_part_size,
        };

        let join_handle = spawn_chunk_sender(handler, new_file_rcv, chunks_snd, confirmation_rcv);

        ReadyFolderThreads { join_handle }
    }

    pub fn join(self) {
        self.join_handle.join().unwrap()
    }
}

pub struct ReadyFolderHandler {
    path: PathBuf,
    file_part_size: u32,
}

impl ReadyFolderHandler {
    fn get_folder_path(&self, file_id: Uuid) -> PathBuf {
        self.path.join(file_id.to_string())
    }

    fn process_confirmation(&self, file_id: Uuid, part_index: FilePartId) -> anyhow::Result<()> {
        let folder_path = self.get_folder_path(file_id);

        if !folder_path.exists() {
            // File already deleted, ignore
            return Ok(());
        }

        mark_part_as_sent(&folder_path, part_index)?;
        if is_file_fully_confirmed(&folder_path)? {
            // Delete the whole folder, as it's no longer needed
            std::fs::remove_dir_all(&folder_path).context("Failed to remove ready folder")?;
        }

        Ok(())
    }

    fn delete_folder(&self, file_id: Uuid) -> anyhow::Result<()> {
        let folder_path = self.get_folder_path(file_id);

        std::fs::remove_dir_all(folder_path).context("Failed to remove ready folder")?;

        Ok(())
    }

    fn get_all_folders(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut folders = Vec::new();

        for entry in std::fs::read_dir(&self.path).context("Failed to read ready folder")? {
            let entry = entry.context("Failed to read ready folder entry")?;
            let path = entry.path();
            if path.is_dir() {
                folders.push(path);
            }
        }

        Ok(folders)
    }

    pub fn generate_file_header(&self, path: &Path) -> io::Result<HeaderChunk> {
        let file = File::open(path)?;

        let file_size = file.metadata()?.len();

        let file_created_date = file.metadata()?.created()?;
        // Convert SystemTime to DateTime<Utc>
        let datetime: DateTime<Utc> = file_created_date.into();
        // Format the datetime to number of nanoseconds since the unix epoch
        let date = datetime.timestamp_nanos_opt().unwrap_or_default();

        let part_count = if file_size % self.file_part_size as u64 == 0 {
            file_size / self.file_part_size as u64
        } else {
            file_size / self.file_part_size as u64 + 1
        };

        let file_header = HeaderChunk {
            id: uuid::Uuid::new_v4(),
            name: path.file_name().unwrap().to_str().unwrap().to_string(),
            date,
            size: file_size,
            part_count: part_count as u32,
            file_part_size: self.file_part_size,
        };

        Ok(file_header)
    }
}
