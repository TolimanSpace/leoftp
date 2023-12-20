use std::io;

use common::binary_serialize::BinarySerialize;
use num_derive::{FromPrimitive, ToPrimitive};

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive, ToPrimitive)]
pub enum ManagedFileMode {
    Contiguous = 0,
    Split = 1,
}

impl BinarySerialize for ManagedFileMode {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> io::Result<()> {
        let value = num_traits::ToPrimitive::to_u8(self);
        let Some(value) = value else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid managed file mode",
            ));
        };

        writer.write_all(&value.to_le_bytes())?;
        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        1
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mut bytes = [0u8; 1];
        reader.read_exact(&mut bytes)?;
        let value = u8::from_le_bytes(bytes);

        let value = num_traits::FromPrimitive::from_u8(value);
        let Some(value) = value else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid managed file mode",
            ));
        };

        Ok(value)
    }
}
