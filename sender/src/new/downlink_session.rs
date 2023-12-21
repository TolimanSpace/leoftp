use std::collections::BinaryHeap;

use common::{chunks::Chunk, control::ControlMessage, file_part_id::FilePartId};
use uuid::Uuid;

use super::storage_manager::StorageManager;

/// A single "downlink session". Sorts all chunks by priority and sends them all.
/// When all chunks run out, the session ends. If more chunks still need to be sent,
/// a new session should be created. The downlink session can only reduce data used
/// by the service, it can't add new files. Adding new files is handled by the
/// StorageManager outside of downlink sessions.
pub struct DownlinkSession {
    storage: StorageManager,
    parts_queue: BinaryHeap<DownlinkSessionItem>,
}

impl DownlinkSession {
    pub fn new(storage: StorageManager) -> Self {
        let mut parts_queue = BinaryHeap::new();

        for file in storage.iter_files() {
            for part in file.remaining_parts() {
                parts_queue.push(DownlinkSessionItem {
                    file_id: file.header().id,
                    part_id: part.part,
                    priority: part.priority,
                });
            }
        }

        Self {
            storage,
            parts_queue,
        }
    }

    pub fn process_control(&mut self, control: ControlMessage) -> anyhow::Result<()> {
        self.storage.process_control(control)
    }

    pub fn next_chunk(&mut self) -> anyhow::Result<Option<Chunk>> {
        loop {
            let item = self.parts_queue.pop();
            let Some(item) = item else {
                return Ok(None);
            };

            let file = &mut self.storage.get_file(item.file_id);
            let Some(file) = file else {
                // File likely deleted, due to acknowledgements
                continue;
            };

            let chunk = file.get_file_part(item.part_id);
            let chunk = match chunk {
                Ok(None) => continue,
                Ok(Some(chunk)) => chunk,
                Err(err) => {
                    tracing::warn!("Failed to get file part: {}", err);
                    continue;
                }
            };

            return Ok(Some(chunk));
        }
    }

    /// Confirms that a file has been sent, decreasing its priority. Not to be confused
    /// with acknowleding files, which deletes their parts.
    pub fn confirm_file_sent(&mut self, file_id: uuid::Uuid) -> anyhow::Result<()> {
        let file = self.storage.get_file_mut(file_id);

        let Some(file) = file else {
            tracing::info!(
                "Received sent confirmation for non-existent file: {}",
                file_id
            );
            return Ok(());
        };

        file.decrease_part_priority(FilePartId::Header)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DownlinkSessionItem {
    file_id: Uuid,

    part_id: FilePartId,
    priority: i16,
}

impl std::cmp::Ord for DownlinkSessionItem {
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

impl std::cmp::PartialOrd for DownlinkSessionItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
