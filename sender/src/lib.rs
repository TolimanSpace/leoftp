use std::{io::Read, path::PathBuf, sync::Mutex, thread::JoinHandle};

use anyhow::Context;
use common::{
    chunks::Chunk,
    transport_packet::{parse_transport_packet_stream, TransportPacket, TransportPacketData},
};
use crossbeam_channel::{Receiver, Sender};

use self::background_runner::{run_downlink_server_bg_runner, DownlinkServerMessage};

mod background_runner;
mod downlink_session;
mod managed_file;
mod storage_manager;
mod tempdir;

pub use storage_manager::StorageManagerConfig;

pub struct DownlinkServer {
    background_runner_messages: Sender<DownlinkServerMessage>,
    join_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl DownlinkServer {
    pub fn spawn(
        input_folder: PathBuf,
        workdir: PathBuf,
        storage_config: StorageManagerConfig,
    ) -> anyhow::Result<Self> {
        let (message_snd, message_rcv) = crossbeam_channel::unbounded();

        let pending_folder = workdir.join("pending");
        let ready_folder = workdir.join("ready");

        // Create the folders
        std::fs::create_dir_all(&input_folder)?;
        std::fs::create_dir_all(&pending_folder)?;
        std::fs::create_dir_all(&ready_folder)?;

        let poller_join_handle =
            spawn_file_poller(input_folder, pending_folder, message_snd.clone())
                .context("Failed to spawn file poller")?;

        let server_join_handle =
            run_downlink_server_bg_runner(ready_folder, message_rcv, storage_config)?;

        Ok(Self {
            background_runner_messages: message_snd,
            join_handles: Mutex::new(vec![server_join_handle, poller_join_handle]),
        })
    }

    pub fn add_control_message_reader(&self, reader: impl 'static + Read + Send) {
        let handle = spawn_control_reader(reader, self.background_runner_messages.clone());
        self.join_handles.lock().unwrap().push(handle);
    }

    pub fn begin_downlink_session(&self) -> anyhow::Result<DownlinkReader> {
        let (chunk_snd, chunk_rcv) = crossbeam_channel::bounded(3);

        let message = DownlinkServerMessage::BeginDownlinkSession(chunk_snd);
        self.background_runner_messages.send(message).context("Failed to notify background runner that a downlink started. The background runner is probably dead.")?;

        Ok(DownlinkReader {
            background_runner_messages: self.background_runner_messages.clone(),
            chunks: chunk_rcv,
        })
    }

    pub fn join(self) {
        self.background_runner_messages
            .send(DownlinkServerMessage::StopAndQuit)
            .ok();

        for handle in self.join_handles.lock().unwrap().drain(..) {
            handle.join().ok();
        }
    }
}

impl Drop for DownlinkServer {
    fn drop(&mut self) {
        self.background_runner_messages
            .send(DownlinkServerMessage::StopAndQuit)
            .ok();

        // Simply kill the server, don't join because dropping shouldn't block
    }
}

/// The downlink reader. Provides a stream of chunks. The session ends
/// when the reader is dropped.
pub struct DownlinkReader {
    background_runner_messages: Sender<DownlinkServerMessage>,
    chunks: Receiver<Option<Chunk>>,
}

impl DownlinkReader {
    pub fn next_transport_packet(&self) -> anyhow::Result<Option<TransportPacket>> {
        let next_chunk = self
            .chunks
            .recv()
            .context("Failed to receive next chunk. The background server is probably dead.")?;

        let Some(next_chunk) = next_chunk else {
            return Ok(None);
        };

        let ack = DownlinkServerMessage::ConfirmChunkSent {
            file_id: next_chunk.file_id(),
            part_id: next_chunk.part_index(),
        };
        self.background_runner_messages
            .send(ack)
            .context("Failed to send ack. The background server is probably dead.")?;

        Ok(Some(TransportPacket::new(TransportPacketData::from_chunk(
            next_chunk,
        ))))
    }
}

impl Drop for DownlinkReader {
    fn drop(&mut self) {
        // Notify the background runner that the session is over
        self.background_runner_messages
            .send(DownlinkServerMessage::EndDownlinkSession)
            .ok();

        // Wait until the receiver disconnects
        while self.chunks.recv().is_ok() {}
    }
}

fn spawn_control_reader(
    control_reader: impl 'static + Read + Send,
    control_snd: Sender<DownlinkServerMessage>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let control_parser = parse_transport_packet_stream(control_reader);

        for packet in control_parser {
            let packet = match packet {
                Ok(packet) => packet,
                Err(err) => {
                    tracing::error!("Error parsing control packet: {}", err);
                    continue;
                }
            };

            let control = packet.as_control_message();
            let Some(control) = control else {
                tracing::error!("Received non-control packet in control stream");
                continue;
            };

            let send_result = control_snd.send(DownlinkServerMessage::Control(control));
            if send_result.is_err() {
                // If the receiver is gone, we can just stop reading
                tracing::info!("Control message receiver disconnected");
                return;
            }
        }
    })
}

pub fn spawn_file_poller(
    input_folder: PathBuf,
    pending_folder: PathBuf,
    new_file_snd: Sender<DownlinkServerMessage>,
) -> anyhow::Result<JoinHandle<()>> {
    // Add all the files that are already in the pending folder to the queue
    for entry in std::fs::read_dir(&pending_folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            new_file_snd
                .send(DownlinkServerMessage::AddFile(path))
                .context("Failed to add file to queue when spawning poller")?;
        }
    }

    // Spawn a thread that polls the new folder, and moves each file there into the pending folder and
    // notifies the pending queue
    let join = std::thread::spawn(move || loop {
        // Find all the new foles in the new folder
        let mut new_files = Vec::new();
        for entry in std::fs::read_dir(&input_folder).unwrap() {
            // Ensure that the file no longer has open file handles

            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                new_files.push(path);
            }
        }

        for path in new_files {
            let pending_path = pending_folder.join(path.file_name().unwrap());

            let rename_result = std::fs::rename(&path, &pending_path);
            if rename_result.is_err() {
                tracing::error!("Failed to move file to pending folder: {:?}", rename_result);
                continue;
            }

            let snd_result = new_file_snd.send(DownlinkServerMessage::AddFile(pending_path));
            if snd_result.is_err() {
                return; // The queue has been removed
            }
        }

        // Sleep to not spam the disk too much
        std::thread::sleep(std::time::Duration::from_millis(100));
    });

    Ok(join)
}
