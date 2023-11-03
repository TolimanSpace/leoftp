use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use common::{
    chunks::Chunk,
    header::{FileHeaderData, FilePartId},
};
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use uuid::Uuid;

use crate::{
    ready_file::{
        is_file_fully_confirmed, mark_part_as_sent, parse_ready_folder, write_ready_file_folder,
    },
    FILE_PART_SIZE,
};

pub struct Confirmation {
    file_id: Uuid,
    part_index: FilePartId,
}

pub struct ReadyFolderThreads {
    // TODO: Join handles
}

impl ReadyFolderThreads {
    pub fn spawn(
        path: PathBuf,
        new_file_rcv: Receiver<PathBuf>,
        chunks_snd: Sender<Chunk>,
        confirmation_rcv: Receiver<Confirmation>,
    ) -> Self {
        let handler = ReadyFolderHandler { path };

        spawn_chunk_sender(handler, new_file_rcv, chunks_snd, confirmation_rcv);

        ReadyFolderThreads {}
    }
}

fn spawn_chunk_sender(
    handler: ReadyFolderHandler,
    new_file_rcv: Receiver<PathBuf>,
    chunks_snd: Sender<Chunk>,
    confirmation_rcv: Receiver<Confirmation>,
) {
    std::thread::spawn(move || loop {
        // TODO: Include more context in these error logs

        // Then, process new recieved files
        loop {
            let new_file = match new_file_rcv.try_recv() {
                Ok(new_file) => new_file,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return, // The queue has been removed
            };

            let header = generate_file_header(&new_file);
            let header = match header {
                Ok(header) => header,
                Err(err) => {
                    println!(
                        "Failed to generate file header.\nFile: {:?}\nErr: {:?}",
                        new_file, err
                    );
                    continue;
                }
            };

            let path = handler.get_folder_path(header.id);

            let write_result = write_ready_file_folder(path, new_file, header);
            if let Err(err) = write_result {
                println!("Failed to write ready file folder: {:?}", err);
                continue;
            }
        }

        // Find all folders
        let folders = match handler.get_all_folders() {
            Ok(folders) => folders,
            Err(err) => {
                println!("Failed to get ready folders: {:?}", err);

                // Wait 1 second to not spam the error
                std::thread::sleep(std::time::Duration::from_secs(1));

                continue;
            }
        };

        // Then, for each folder, send all chunks
        for folder in folders {
            let ready_file = parse_ready_folder(folder);
            let ready_file = match ready_file {
                Ok(ready_file) => ready_file,
                Err(err) => {
                    println!("Failed to parse ready folder: {:?}", err);
                    continue;
                }
            };

            for part in &ready_file.unsent_parts {
                let chunk = ready_file.get_unsent_part_as_chunk(*part);

                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        println!("Failed to get unsent part as chunk: {:?}", err);
                        continue;
                    }
                };

                let snd_result = chunks_snd.send(chunk);
                if snd_result.is_err() {
                    return; // The queue has been removed
                }
            }
        }

        // Process confirmations. Block the loop until at least one confirmation is recieved.
        let mut first_confirmation = true;
        loop {
            let confirmation = if first_confirmation {
                first_confirmation = false;
                match confirmation_rcv.recv() {
                    Ok(confirmation) => confirmation,
                    Err(_) => return, // The queue has been removed
                }
            } else {
                match confirmation_rcv.try_recv() {
                    Ok(confirmation) => confirmation,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return, // The queue has been removed
                }
            };

            if let Err(err) = handler.process_confirmation(confirmation) {
                println!("Failed to process confirmation: {:?}", err);
            }
        }
    });
}

pub struct ReadyFolderHandler {
    path: PathBuf,
}

impl ReadyFolderHandler {
    fn get_folder_path(&self, file_id: Uuid) -> PathBuf {
        self.path.join(file_id.to_string())
    }

    fn process_confirmation(&self, confirmation: Confirmation) -> anyhow::Result<()> {
        let folder_path = self.get_folder_path(confirmation.file_id);

        mark_part_as_sent(&folder_path, confirmation.part_index)?;
        if is_file_fully_confirmed(&folder_path)? {
            // Delete the whole folder, as it's no longer needed
            std::fs::remove_dir_all(&folder_path).context("Failed to remove ready folder")?;
        }

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
}

pub fn generate_file_header(path: &Path) -> io::Result<FileHeaderData> {
    let file = File::open(path)?;

    let file_size = file.metadata()?.len();

    let file_created_date = file.metadata()?.created()?;
    // Convert SystemTime to DateTime<Utc>
    let datetime: DateTime<Utc> = file_created_date.into();
    // Format the datetime to ISO 8601
    let iso_string = datetime.to_rfc3339();

    let part_count = if file_size % FILE_PART_SIZE == 0 {
        file_size / FILE_PART_SIZE
    } else {
        file_size / FILE_PART_SIZE + 1
    };

    let file_header = FileHeaderData {
        id: uuid::Uuid::new_v4(),
        name: path.file_name().unwrap().to_str().unwrap().to_string(),
        date: iso_string,
        size: file_size,
        part_count,
    };

    Ok(file_header)
}
