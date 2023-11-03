use uuid::Uuid;

use crate::header::FilePartId;

pub enum ControlMessage {
    // Confirm that a part of a file was successfully received
    ConfirmPart {
        file_id: Uuid,
        part_index: FilePartId,
    },

    // Delete a file from the ready folder, in case some error happened
    DeleteFile {
        file_id: Uuid,
    },

    // The sending loop pauses until control messages come, so if we just want it to unpause without
    // any other messages, then we can send this
    Continue,
}
