use std::{
    path::PathBuf,
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::Context;
use common::{chunks::Chunk, control::ControlMessage, file_part_id::FilePartId};
use crossbeam_channel::{Receiver, SendTimeoutError, Sender};
use uuid::Uuid;

use crate::storage_manager::StorageManagerConfig;

use super::{downlink_session::DownlinkSession, storage_manager::StorageManager};

pub enum DownlinkServerMessage {
    /// Process a control message received from outside
    Control(ControlMessage),
    /// Mark a file part as sent. Not to be confused with acknowledging a file part,
    /// which deletes it. This just decreases the part's priority.
    ConfirmChunkSent { file_id: Uuid, part_id: FilePartId },
    /// Inform the server that a new file can be added by the following path.
    AddFile(PathBuf),
    /// Begin a new downlink session, sending chunks to the new chunk queue. The session
    /// will end when the receiver is dropped.
    BeginDownlinkSession(Sender<Option<Chunk>>),
    /// Stop the downlink background runner and quit. This should be called when the
    /// service is dropped.
    StopAndQuit,
}

/// The background runner is in a downlink session. It sends chunks from the downlink
/// session, processes control messages, but can't add new files. Instead, new file paths
/// get collected to be processed when the session ends.
struct BackgroundRunnerDownlinkSessionState {
    downlink_session: DownlinkSession,
    sender: Sender<Option<Chunk>>,
    pending_new_files: Vec<PathBuf>,
}

/// The background runner is waiting for a downlink session to start. It can't send
/// chunks, but it can process control messages and add new files.
struct BackgroundRunnerWaitingState {
    storage: StorageManager,
}

pub fn run_downlink_server_bg_runner(
    files_dir: PathBuf,
    message_rcv: Receiver<DownlinkServerMessage>,
    storage_config: StorageManagerConfig,
) -> anyhow::Result<JoinHandle<()>> {
    let storage = StorageManager::new(files_dir.clone(), storage_config)
        .context("Failed to load storage when initializing downlink background runner")?;

    let mut prev_waiting_state = BackgroundRunnerWaitingState { storage };

    // Run a thread that bounces between the two states until killed.
    Ok(thread::spawn(move || loop {
        let downlink_state = prev_waiting_state.run_until_switched(message_rcv.clone());
        let Some(downlink_state) = downlink_state else {
            // Killed
            return;
        };

        let waiting_state = downlink_state.run_until_switched(message_rcv.clone());
        let Some(waiting_state) = waiting_state else {
            // Killed
            return;
        };

        prev_waiting_state = waiting_state;
    }))
}

impl BackgroundRunnerDownlinkSessionState {
    fn run_until_switched(
        mut self,
        message_rcv: Receiver<DownlinkServerMessage>,
    ) -> Option<BackgroundRunnerWaitingState> {
        // Penidng chunk allows us to both attempt sending into a blocking channel while also
        // processing control events. This helps avoid deadlocks.
        let mut pending_chunk = None;
        loop {
            if pending_chunk.is_none() {
                // Try to process the next chunk
                let next_chunk = self.downlink_session.next_chunk();
                let next_chunk_value = match next_chunk {
                    Ok(None) => {
                        // This is likely because there's no files to send.
                        // Sleep for 100ms, to not spam the CPU.
                        std::thread::sleep(std::time::Duration::from_millis(100));

                        Some(None)
                    }
                    Ok(Some(chunk)) => Some(Some(chunk)),
                    Err(err) => {
                        // An unexpected error occurred when trying to get the next chunk.
                        // Sleep for 1000ms, to not spam the CPU or the logs.
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                        tracing::error!("Error getting next chunk from downlink: {}", err);

                        None
                    }
                };

                if let Some(value) = next_chunk_value {
                    pending_chunk = Some(value);
                }
            }

            if let Some(chunk) = pending_chunk {
                // Try to send the chunk. Give it 100ms of timeout, to avoid deadlocks. After 100ms,
                // we need to process the control events and loop back.
                let send_result = self.sender.send_timeout(chunk, Duration::from_millis(100));
                if let Err(err) = send_result {
                    match err {
                        SendTimeoutError::Timeout(chunk) => {
                            // Timeout. Set the pending chunk back to the chunk we tried to send.
                            pending_chunk = Some(chunk);
                        }
                        SendTimeoutError::Disconnected(_) => {
                            // The receiver is gone. End the downlink session.
                            tracing::info!("Chunk receiver disconnected. Ending downlink session.");
                            return Some(self.transition_to_waiting_state());
                        }
                    }
                } else {
                    // We sent the chunk, so we can clear it.
                    pending_chunk = None;
                }
            }

            // Then, try to process the next message(s).
            while let Ok(message) = message_rcv.try_recv() {
                match message {
                    DownlinkServerMessage::Control(control) => {
                        let process_result = self.downlink_session.process_control(control.clone());
                        if let Err(err) = process_result {
                            tracing::error!(
                                "Error processing control message.\nMessage: {:?}\nError: {}",
                                control,
                                err
                            );
                        }
                    }
                    DownlinkServerMessage::ConfirmChunkSent { file_id, part_id } => {
                        let process_result =
                            self.downlink_session.confirm_file_sent(file_id, part_id);
                        if let Err(err) = process_result {
                            tracing::error!("Error marking file part as sent. File ID: {}\nPart ID: {}\nError: {}", file_id, part_id, err);
                        }
                    }
                    DownlinkServerMessage::AddFile(path) => {
                        // We can't add new files during a downlink session, as that may require re-shuffing a lot of data.
                        // Instead, we queue them up and process them when the session ends.
                        self.pending_new_files.push(path);
                    }
                    DownlinkServerMessage::BeginDownlinkSession(_) => {
                        // We can't start a new downlink session while we're already in one.
                        tracing::error!("Received BeginDownlinkSession message while already in a downlink session");
                    }
                    DownlinkServerMessage::StopAndQuit => {
                        // We're done here
                        return None;
                    }
                }
            }
        }
    }

    fn transition_to_waiting_state(self) -> BackgroundRunnerWaitingState {
        let mut current_storage_manager = self.downlink_session.into_storage_manager();

        for new_file_path in self.pending_new_files {
            let result = current_storage_manager.add_file_from_path(&new_file_path);
            if let Err(err) = result {
                tracing::error!(
                    "Error adding new file. Path: {:?}\nError: {}",
                    new_file_path,
                    err
                );
            }
        }

        BackgroundRunnerWaitingState {
            storage: current_storage_manager,
        }
    }
}

impl BackgroundRunnerWaitingState {
    fn run_until_switched(
        mut self,
        message_rcv: Receiver<DownlinkServerMessage>,
    ) -> Option<BackgroundRunnerDownlinkSessionState> {
        loop {
            // Then, try to process the next message(s).
            while let Ok(message) = message_rcv.try_recv() {
                match message {
                    DownlinkServerMessage::Control(control) => {
                        let process_result = self.storage.process_control(control.clone());
                        if let Err(err) = process_result {
                            tracing::error!(
                                "Error processing control message.\nMessage: {:?}\nError: {}",
                                control,
                                err
                            );
                        }
                    }
                    DownlinkServerMessage::ConfirmChunkSent { file_id, part_id } => {
                        let result = self.storage.confirm_file_sent(file_id, part_id);
                        if let Err(err) = result {
                            tracing::error!("Error marking file part as sent. File ID: {}\nPart ID: {}\nError: {}", file_id, part_id, err);
                        }
                    }
                    DownlinkServerMessage::AddFile(path) => {
                        let add_result = self.storage.add_file_from_path(&path);
                        if let Err(err) = add_result {
                            tracing::error!(
                                "Error adding file from path. Path: {:?}\nError: {}",
                                path,
                                err
                            );
                        }
                    }
                    DownlinkServerMessage::BeginDownlinkSession(sender) => {
                        // Transition to a downlinking session
                        return Some(self.transition_to_downlink_session(sender));
                    }
                    DownlinkServerMessage::StopAndQuit => {
                        // We're done here
                        return None;
                    }
                }
            }
        }
    }

    fn transition_to_downlink_session(
        self,
        sender: Sender<Option<Chunk>>,
    ) -> BackgroundRunnerDownlinkSessionState {
        let downlink_session = DownlinkSession::new(self.storage);
        BackgroundRunnerDownlinkSessionState {
            downlink_session,
            sender,
            pending_new_files: Vec::new(),
        }
    }
}
