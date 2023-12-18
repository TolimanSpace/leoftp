use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use common::{
    chunks::{Chunk, DataChunk},
    control::ControlMessage,
};
use crossbeam_channel::{Receiver, RecvError, SendError, Sender};
use input_folder::InputFolderThreads;
use ready_folder::ReadyFolderThreads;
mod input_folder;
mod ready_file;
mod ready_folder;
mod new;

// Procedure:
// 1. Service creates a new file in the input folder. The file is valid as long as it has no other file handles using it.
// 2. Sender moves the file to the pending folder. While in the pending folder, it determines any relevant metadata.
// 3. Sender creates a folder in the ready folder with the same name as the file. The file gets moved in, along with a json file that represents the file's metadata.
// 4. File parts are sent to the radio, including the file header/metadata
// 5. Whenever we get a confirmation that a file part has been successfuly sent, we remove it from the ready folder.

const DEFAULT_FILE_PART_SIZE: u32 = 1024 * 64; // 64kb

struct FileServerInner {
    input_folder: InputFolderThreads,
    ready_folder: ReadyFolderThreads,
}

/// The file server. It automatically has a queue of chunks to send, and a queue of confirmations to process.
#[derive(Clone)]
pub struct FileServer {
    inner: Arc<Mutex<Option<FileServerInner>>>,
    chunks_rcv: Receiver<Chunk>,
    control_snd: Sender<ControlMessage>,
}

impl FileServer {
    pub fn spawn(input_folder: PathBuf, workdir: PathBuf) -> io::Result<Self> {
        Self::spawn_with_part_size(input_folder, workdir, DEFAULT_FILE_PART_SIZE)
    }

    pub fn spawn_with_part_size(
        input_folder: PathBuf,
        workdir: PathBuf,
        file_part_size: u32,
    ) -> io::Result<Self> {
        assert!(file_part_size > 0);
        assert!(file_part_size < DataChunk::MAX_CHUNK_LENGTH as u32);

        let (new_file_snd, new_file_rcv) = crossbeam_channel::unbounded();
        let (control_snd, control_rcv) = crossbeam_channel::unbounded();

        // We need to make sure chunks is bounded, so it doesn't overflow memory
        let (chunks_snd, chunks_rcv) = crossbeam_channel::bounded(10);

        let pending_folder = workdir.join("pending");
        let ready_folder = workdir.join("ready");

        // Ensure that the folders exist
        std::fs::create_dir_all(&input_folder)?;
        std::fs::create_dir_all(&pending_folder)?;
        std::fs::create_dir_all(&ready_folder)?;

        Ok(FileServer {
            inner: Arc::new(Mutex::new(Some(FileServerInner {
                input_folder: InputFolderThreads::spawn(
                    input_folder,
                    pending_folder,
                    new_file_snd,
                )?,
                ready_folder: ReadyFolderThreads::spawn(
                    ready_folder,
                    new_file_rcv,
                    chunks_snd,
                    control_rcv,
                    file_part_size,
                ),
            }))),

            chunks_rcv,
            control_snd,
        })
    }

    pub fn get_chunk(&self) -> Result<Chunk, RecvError> {
        self.chunks_rcv.recv()
    }

    pub fn send_control_msg(
        &self,
        control: ControlMessage,
    ) -> Result<(), SendError<ControlMessage>> {
        self.control_snd.send(control)
    }

    pub fn join(self) {
        let inner = self.inner.lock().unwrap().take().unwrap();

        // Then join the threads
        inner.input_folder.join();
        inner.ready_folder.join();
    }
}
