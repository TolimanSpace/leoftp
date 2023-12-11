use crate::{binary_serialize::BinarySerialize, validity::ValidityCheck};

use self::{
    hashing::{HashedReader, HashedWriter},
    scrambling::{ScramblingWriter, UnscramblingReader},
    substream::SubstreamReader,
};

mod checkpoint_stream;
mod hashing;
mod scrambling;
mod substream;
mod tolerant_parser;

pub const CONST_PACKET_SIGNATURE: &[u8] = b"LFTP";

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct TransportPacket<T: BinarySerialize> {
    data: TransportPacketInner<T>,
}

impl<T: BinarySerialize> TransportPacket<T> {
    const MAX_DATA_LEN: usize = TransportPacketInner::<T>::MAX_DATA_LEN;

    pub fn new(data: T) -> Self {
        Self {
            data: TransportPacketInner::new(data),
        }
    }

    pub fn data(self) -> T {
        self.data.data
    }
}

impl<T: BinarySerialize> BinarySerialize for TransportPacket<T> {
    fn serialize_to_stream(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(CONST_PACKET_SIGNATURE)?;
        self.data.serialize_to_stream(writer)?;
        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        CONST_PACKET_SIGNATURE.len() as u32 // Constant signature
         + self.data.length_when_serialized() // Data
    }

    fn deserialize_from_stream(reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut signature = [0u8; 4];
        reader.read_exact(&mut signature)?;

        if signature != CONST_PACKET_SIGNATURE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid packet signature",
            ));
        }

        let data = TransportPacketInner::deserialize_from_stream(reader)?;

        Ok(Self { data })
    }
}

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct TransportPacketInner<T: BinarySerialize> {
    data: T,
}

impl<T: BinarySerialize> TransportPacketInner<T> {
    const MAX_DATA_LEN: usize = 8388608; // 8 MiB

    pub fn new(data: T) -> Self {
        Self { data }
    }

    pub fn data(self) -> T {
        self.data
    }
}

impl<T: BinarySerialize> BinarySerialize for TransportPacketInner<T> {
    fn serialize_to_stream(&self, mut writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&self.data.length_when_serialized().to_le_bytes())?;

        let mut hash_writer = HashedWriter::new(ScramblingWriter::new(&mut writer));
        self.data.serialize_to_stream(&mut hash_writer)?;
        let hash = hash_writer.result();

        writer.write_all(&hash.to_le_bytes())?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        4 // Data length
         + self.data.length_when_serialized() // Data
    }

    fn deserialize_from_stream(mut reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes);

        if len > Self::MAX_DATA_LEN as u32 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Transport packet length {} exceeds {}",
                    len,
                    Self::MAX_DATA_LEN as u32
                ),
            ));
        }

        let mut data_reader = SubstreamReader::new(&mut reader, len as usize);
        let mut data_reader_hashed = HashedReader::new(UnscramblingReader::new(&mut data_reader));
        let data = T::deserialize_from_stream(&mut data_reader_hashed)?;
        let calculated_hash = data_reader_hashed.result();

        if !data_reader.reached_end() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Transport packet data length mismatch",
            ));
        }

        let mut hash_bytes = [0u8; 8];
        reader.read_exact(&mut hash_bytes)?;
        let hash = u64::from_le_bytes(hash_bytes);

        if hash != calculated_hash {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid packet hash",
            ));
        }

        Ok(Self { data })
    }
}

impl<T: BinarySerialize> ValidityCheck for TransportPacketInner<T> {
    fn is_valid(&self) -> bool {
        self.data.length_when_serialized() <= Self::MAX_DATA_LEN as u32
    }
}

impl<T: BinarySerialize> ValidityCheck for TransportPacket<T> {
    fn is_valid(&self) -> bool {
        self.data.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use crate::{chunks::Chunk, header::FilePartId};

    use super::*;

    #[test]
    fn test_transport_packet() {
        let packet = Chunk {
            // data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            data: vec![],
            file_id: Default::default(),
            part: FilePartId::Part(12),
        };

        let packet = TransportPacket::new(packet);

        let mut serialized = Vec::new();
        packet.serialize_to_stream(&mut serialized).unwrap();
        dbg!(&serialized);

        let mut deserialized = std::io::Cursor::new(serialized);
        let deserialized =
            TransportPacket::<Chunk>::deserialize_from_stream(&mut deserialized).unwrap();

        assert_eq!(deserialized.data(), packet.data());
    }
}
