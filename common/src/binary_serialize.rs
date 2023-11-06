use std::io;

pub trait BinarySerialize {
    fn serialize_to_stream<W: std::io::Write>(&self, writer: &mut W) -> io::Result<()>;
    fn deserialize_from_stream<R: std::io::Read>(reader: &mut R) -> io::Result<Self>
    where
        Self: Sized;
}
