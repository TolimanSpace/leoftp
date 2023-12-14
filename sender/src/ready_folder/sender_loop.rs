#![allow(clippy::comparison_chain)]
use std::path::PathBuf;

use common::{chunks::Chunk, control::ControlMessage, header::FilePartId};
use crossbeam_channel::{Receiver, Sender, TryRecvError};

use crate::ready_file::{parse_ready_folder, write_ready_file_folder, ReadyFile};

use super::ReadyFolderHandler;

#[derive(Debug)]
struct LoopKilled;

struct PacketLocation<'a> {
    file: &'a ReadyFile,
    part_id: FilePartId,
}

fn sort_packet_locations_by_importance(packet_locations: &mut [PacketLocation]) {
    // Sort priority:
    // Any header parts are higher priority than data parts `.part_id == FilePartId::Header`
    // Sort by dates, so older files always get higher precedence `.file.header.date`
    // Sort by part id `FilePartId::Part(part) = _.part_id`
    packet_locations.sort_by(|a, b| {
        let a_header = a.part_id == FilePartId::Header;
        let b_header = b.part_id == FilePartId::Header;

        if a_header && !b_header {
            return std::cmp::Ordering::Less;
        } else if !a_header && b_header {
            return std::cmp::Ordering::Greater;
        }

        let a_date = a.file.header.date;
        let b_date = b.file.header.date;

        if a_date < b_date {
            return std::cmp::Ordering::Less;
        } else if a_date > b_date {
            return std::cmp::Ordering::Greater;
        }

        let a_part = match a.part_id {
            FilePartId::Part(part) => part,
            FilePartId::Header => 0,
        };
        let b_part = match b.part_id {
            FilePartId::Part(part) => part,
            FilePartId::Header => 0,
        };

        a_part.cmp(&b_part)
    });
}

struct SenderLoopHandler {
    folder: ReadyFolderHandler,
    new_file_rcv: Receiver<PathBuf>,
    chunks_snd: Sender<Chunk>,
    confirmation_rcv: Receiver<ControlMessage>,
}

impl SenderLoopHandler {
    fn new(
        folder: ReadyFolderHandler,
        new_file_rcv: Receiver<PathBuf>,
        chunks_snd: Sender<Chunk>,
        confirmation_rcv: Receiver<ControlMessage>,
    ) -> Self {
        Self {
            folder,
            new_file_rcv,
            chunks_snd,
            confirmation_rcv,
        }
    }

    /// Try to read from the control events queue, processing all available events.
    fn process_control_events(&self) -> Result<(), LoopKilled> {
        loop {
            let confirmation = match self.confirmation_rcv.try_recv() {
                Ok(confirmation) => confirmation,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err(LoopKilled), // The queue has been removed
            };

            match confirmation {
                ControlMessage::ConfirmPart(confirm) => {
                    let process_result = self
                        .folder
                        .process_confirmation(confirm.file_id, confirm.part_index);
                    if let Err(err) = process_result {
                        tracing::debug!("Failed to process confirmation: {:?}", err);
                    }
                }
                ControlMessage::DeleteFile(delete) => {
                    let delete_result = self.folder.delete_folder(delete.file_id);
                    if let Err(err) = delete_result {
                        tracing::debug!("Failed to delete folder: {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }

    /// Process new files if there are any, return a vec of all their processed paths
    fn process_new_files(&self) -> Result<Vec<PathBuf>, LoopKilled> {
        let mut written_paths = Vec::new();

        // First, process all received new files, if any are available.
        loop {
            let new_file = match self.new_file_rcv.try_recv() {
                Ok(new_file) => new_file,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err(LoopKilled), // The queue has been removed
            };

            let header = self.folder.generate_file_header(&new_file);
            let header = match header {
                Ok(header) => header,
                Err(err) => {
                    tracing::debug!(
                        "Failed to generate file header.\nFile: {:?}\nErr: {:?}",
                        new_file,
                        err
                    );
                    continue;
                }
            };

            let path = self.folder.get_folder_path(header.id);

            let write_result = write_ready_file_folder(&path, &new_file, header);
            if let Err(err) = write_result {
                tracing::debug!("Failed to write ready file folder: {:?}", err);
                continue;
            }

            written_paths.push(path);
        }

        Ok(written_paths)
    }

    /// Attempt to receive any newly added files. If there are newly added files,
    /// block the caller until all the chunks of the files have been sent.
    /// Also, control events are still processed between each chunk.
    fn process_and_send_new_files(&self) -> Result<(), LoopKilled> {
        let written_paths = self.process_new_files()?;

        // If there are paths, attempt to send all the chunks through, blocking the parent caller until it's done.
        if !written_paths.is_empty() {
            let mut ready_files = Vec::new();

            for path in written_paths {
                let ready_file = match parse_ready_folder(path) {
                    Ok(ready_file) => ready_file,
                    Err(err) => {
                        tracing::error!("Failed to parse ready folder for new folder: {:?}", err);
                        continue;
                    }
                };

                ready_files.push(ready_file);
            }

            let mut packet_locations = Vec::new();

            for ready_file in &ready_files {
                for part in &ready_file.unsent_parts {
                    packet_locations.push(PacketLocation {
                        file: ready_file,
                        part_id: *part,
                    });
                }
            }

            sort_packet_locations_by_importance(&mut packet_locations);

            for packet_location in packet_locations {
                if !packet_location.file.still_exists_on_disk() {
                    tracing::error!(
                        "New file {} disappeared from disk before being fully sent",
                        packet_location.file.header.id
                    );
                }

                let chunk = match packet_location
                    .file
                    .get_unsent_part_as_chunk(packet_location.part_id)
                {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        tracing::error!("Failed to get unsent part as chunk: {:?}", err);
                        continue;
                    }
                };

                let snd_result = self.chunks_snd.send(chunk);
                if snd_result.is_err() {
                    return Err(LoopKilled); // The queue has been removed
                }

                // Process control events between chunk sends
                self.process_control_events()?;
            }
        }

        Ok(())
    }

    /// For any old chunks on the disk, repeatedly try to send them in order of importance.
    /// In between sending each chunk, attempt to process new files and process control events.
    fn run_process_loop(self) -> Result<(), LoopKilled> {
        loop {
            let old_folders = match self.folder.get_all_folders() {
                Ok(folders) => folders,
                Err(err) => {
                    tracing::error!("Failed to get ready folders: {:?}", err);

                    // Wait 1 second to not spam the error
                    std::thread::sleep(std::time::Duration::from_secs(1));

                    continue;
                }
            };

            let mut old_folders_parsed = Vec::new();

            for folder in old_folders {
                let ready_file = match parse_ready_folder(folder) {
                    Ok(ready_file) => ready_file,
                    Err(err) => {
                        tracing::error!("Failed to parse ready folder for old folder: {:?}", err);
                        continue;
                    }
                };

                old_folders_parsed.push(ready_file);
            }

            let mut packet_locations = Vec::new();

            for ready_file in &old_folders_parsed {
                for part in &ready_file.unsent_parts {
                    packet_locations.push(PacketLocation {
                        file: ready_file,
                        part_id: *part,
                    });
                }
            }

            // If there are no packets to send, process new files and control events
            if packet_locations.is_empty() {
                // Process control events between chunk sends
                self.process_control_events()?;

                // Process new files between chunk sends
                self.process_and_send_new_files()?;

                // Wait 100ms to not abuse the cpu
                std::thread::sleep(std::time::Duration::from_millis(100));
            } else {
                sort_packet_locations_by_importance(&mut packet_locations);

                for packet_location in packet_locations {
                    if !packet_location.file.still_exists_on_disk() {
                        continue;
                    }

                    let chunk = match packet_location
                        .file
                        .get_unsent_part_as_chunk(packet_location.part_id)
                    {
                        Ok(chunk) => chunk,
                        Err(err) => {
                            tracing::error!("Failed to get unsent part as chunk: {:?}", err);
                            continue;
                        }
                    };

                    let snd_result = self.chunks_snd.send(chunk);
                    if snd_result.is_err() {
                        return Err(LoopKilled); // The queue has been removed
                    }

                    // Process control events between chunk sends
                    self.process_control_events()?;

                    // Process new files between chunk sends
                    self.process_and_send_new_files()?;
                }
            }
        }
    }
}

pub fn spawn_chunk_sender(
    handler: ReadyFolderHandler,
    new_file_rcv: Receiver<PathBuf>,
    chunks_snd: Sender<Chunk>,
    confirmation_rcv: Receiver<ControlMessage>,
) {
    std::thread::spawn(move || {
        let handler = SenderLoopHandler::new(handler, new_file_rcv, chunks_snd, confirmation_rcv);
        handler.run_process_loop().ok();
    });
}
