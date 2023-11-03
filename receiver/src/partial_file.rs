use common::{chunks::Chunk, header::FilePartId};
use uuid::Uuid;

pub struct PartialFile {
    pub file_id: Uuid,
    pub parts: Vec<FilePartId>,
    pub chunks: Vec<Chunk>,
}
