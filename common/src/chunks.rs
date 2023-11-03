use uuid::Uuid;

use crate::header::FilePartId;

pub struct Chunk {
    pub file_id: Uuid,
    pub part: FilePartId,
    pub data: Vec<u8>,
}
