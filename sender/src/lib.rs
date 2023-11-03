use std::{io, path::PathBuf};

use common::chunks::Chunk;
use crossbeam_channel::{Receiver, Sender};
use input_folder::InputFolderThreads;
use ready_folder::{Confirmation, ReadyFolderThreads};
mod input_folder;
mod ready_file;
mod ready_folder;

// Procedure:
// 1. Service creates a new file in the input folder. The file is valid as long as it has no other file handles using it.
// 2. Sender moves the file to the pending folder. While in the pending folder, it determines any relevant metadata, such as block hashes.
// 3. Sender creates a folder in the ready folder with the same name as the file. The file gets moved in, along with a json file that represents the file's metadata.
// 4. File parts are sent to the radio, including the file header/metadata
// 5. Whenever we get a confirmation that a file part has been successfuly sent, we remove it from the ready folder.

const FILE_PART_SIZE: u64 = 1024 * 64; // 64kb

/// The file server. It automatically has a queue of chunks to send, and a queue of confirmations to process.
pub struct FileServer {
    _input_folder: InputFolderThreads,
    _ready_folder: ReadyFolderThreads,

    chunks_rcv: Receiver<Chunk>,
    confirmations_snd: Sender<Confirmation>,
}

impl FileServer {
    pub fn spawn(input_folder: PathBuf, workdir: PathBuf) -> io::Result<Self> {
        let (new_file_snd, new_file_rcv) = crossbeam_channel::unbounded();
        let (confirmations_snd, confirmations_rcv) = crossbeam_channel::unbounded();

        // We need to make sure chunks is bounded, so it doesn't overflow memory
        let (chunks_snd, chunks_rcv) = crossbeam_channel::bounded(10);

        let pending_folder = workdir.join("pending");
        let ready_folder = workdir.join("ready");

        // Ensure that the folders exist
        std::fs::create_dir_all(&input_folder)?;
        std::fs::create_dir_all(&pending_folder)?;
        std::fs::create_dir_all(&ready_folder)?;

        Ok(FileServer {
            _input_folder: InputFolderThreads::spawn(input_folder, pending_folder, new_file_snd)?,
            _ready_folder: ReadyFolderThreads::spawn(
                ready_folder,
                new_file_rcv,
                chunks_snd,
                confirmations_rcv,
            ),

            chunks_rcv,
            confirmations_snd,
        })
    }

    pub fn get_chunk(&self) -> Chunk {
        self.chunks_rcv.recv().unwrap()
    }

    pub fn send_confirmation(&self, confirmation: Confirmation) {
        self.confirmations_snd.send(confirmation).unwrap();
    }
}
