use common::{
    chunks::Chunk,
    control::ControlMessage,
    file_part_id::FilePartId,
    file_sending::storage_manager::{
        cmp_file_storage_part_normal, SendingStorageManager, StorageFilePart,
    },
};

/// A single "downlink session". Sorts all chunks by priority and sends them all.
/// Once all chunks are sent, it loops from the start. The downlink session can only
/// reduce data used by the service, it can't add new files. Adding new files is
/// handled by the StorageManager outside of downlink sessions.
pub struct DownlinkSession {
    storage: SendingStorageManager,
    parts_queue: Vec<StorageFilePart>,
    parts_queue_index: usize,
}

impl DownlinkSession {
    pub fn new(storage: SendingStorageManager) -> Self {
        let mut parts_queue = storage
            .iter_remaining_storage_file_parts()
            .collect::<Vec<_>>();

        // Sort from highest priority to lowest, we will index from the start and increment up.
        parts_queue.sort_unstable_by(|a, b| cmp_file_storage_part_normal(b, a));

        Self {
            storage,
            parts_queue,
            parts_queue_index: 0,
        }
    }

    pub fn process_control(&mut self, control: ControlMessage) -> anyhow::Result<()> {
        self.storage.process_control(control)
    }

    fn next_item(&mut self) -> Option<StorageFilePart> {
        if self.parts_queue.is_empty() {
            return None;
        }

        let item = self.parts_queue[self.parts_queue_index].clone();
        self.parts_queue_index += 1;
        if self.parts_queue_index >= self.parts_queue.len() {
            self.parts_queue_index = 0;
        }

        Some(item)
    }

    pub fn next_chunk(&mut self) -> anyhow::Result<Option<Chunk>> {
        let start_index = self.parts_queue_index;
        loop {
            let item = self.next_item();
            let Some(item) = item else {
                return Ok(None);
            };

            if self.parts_queue_index == start_index {
                // We looped around, no more items
                return Ok(None);
            }

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
    pub fn confirm_file_sent(
        &mut self,
        file_id: uuid::Uuid,
        part_id: FilePartId,
    ) -> anyhow::Result<()> {
        self.storage.confirm_file_sent(file_id, part_id)
    }

    pub fn into_storage_manager(self) -> SendingStorageManager {
        self.storage
    }
}
