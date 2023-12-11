use std::io;

pub trait BinarySerialize {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> io::Result<()>;
    fn length_when_serialized(&self) -> u32;
    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> io::Result<Self>
    where
        Self: Sized;
}
