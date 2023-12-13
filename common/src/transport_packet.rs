use crate::{binary_serialize::BinarySerialize, chunks::Chunk, validity::ValidityCheck};

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
pub use tolerant_parser::parse_transport_packet_stream;

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, PartialEq, Debug)]
pub enum TransportPacketData {
    HeaderChunk(crate::chunks::HeaderChunk),            // 0
    DataChunk(crate::chunks::DataChunk),                // 1
    AcknowledgementPacket(crate::control::ConfirmPart), // 128
    DeleteFile(crate::control::DeleteFile),             // 129
}

pub const CONST_PACKET_SIGNATURE: &[u8] = b"LFTP";

#[cfg_attr(feature = "fuzzing", derive(arbitrary::Arbitrary))]
#[derive(Clone, PartialEq, Debug)]
/// The full transport packet, including the signature
pub struct TransportPacket {
    data: TransportPacketInner,
}

impl TransportPacket {
    const MAX_DATA_LEN: usize = TransportPacketInner::MAX_DATA_LEN;

    pub fn new(data: TransportPacketData) -> Self {
        Self {
            data: TransportPacketInner::new(data),
        }
    }

    pub fn data(self) -> TransportPacketData {
        self.data.data
    }

    pub fn data_as_chunk(self) -> Option<Chunk> {
        match self.data.data {
            TransportPacketData::HeaderChunk(header_chunk) => Some(Chunk::Header(header_chunk)),
            TransportPacketData::DataChunk(data_chunk) => Some(Chunk::Data(data_chunk)),
            _ => None,
        }
    }

    pub fn data_as_control_message(self) -> Option<crate::control::ControlMessage> {
        match self.data.data {
            TransportPacketData::AcknowledgementPacket(ack) => {
                Some(crate::control::ControlMessage::ConfirmPart(ack))
            }
            TransportPacketData::DeleteFile(delete_file) => {
                Some(crate::control::ControlMessage::DeleteFile(delete_file))
            }
            _ => None,
        }
    }
}

impl BinarySerialize for TransportPacket {
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
#[derive(Clone, PartialEq, Debug)]
/// The inner transport packet, without the signature
pub struct TransportPacketInner {
    data: TransportPacketData,
}

impl TransportPacketInner {
    const MAX_DATA_LEN: usize = 8388608; // 8 MiB

    pub fn new(data: TransportPacketData) -> Self {
        Self { data }
    }

    pub fn data(self) -> TransportPacketData {
        self.data
    }

    fn inner_data_len(&self) -> u32 {
        match self.data {
            TransportPacketData::HeaderChunk(ref header_chunk) => {
                header_chunk.length_when_serialized()
            }
            TransportPacketData::DataChunk(ref data_chunk) => data_chunk.length_when_serialized(),
            TransportPacketData::AcknowledgementPacket(ref ack) => ack.length_when_serialized(),
            TransportPacketData::DeleteFile(ref delete_file) => {
                delete_file.length_when_serialized()
            }
        }
    }
}

impl BinarySerialize for TransportPacketInner {
    fn serialize_to_stream(&self, mut writer: &mut impl std::io::Write) -> std::io::Result<()> {
        let hash = match self.data {
            TransportPacketData::HeaderChunk(ref header_chunk) => {
                writer.write_all(&[0])?;
                writer.write_all(&header_chunk.length_when_serialized().to_le_bytes())?;
                let mut writer = HashedWriter::new(ScramblingWriter::new(&mut writer));
                header_chunk.serialize_to_stream(&mut writer)?;
                writer.result()
            }
            TransportPacketData::DataChunk(ref data_chunk) => {
                writer.write_all(&[1])?;
                writer.write_all(&data_chunk.length_when_serialized().to_le_bytes())?;
                let mut writer = HashedWriter::new(ScramblingWriter::new(&mut writer));
                data_chunk.serialize_to_stream(&mut writer)?;
                writer.result()
            }
            TransportPacketData::AcknowledgementPacket(ref ack) => {
                writer.write_all(&[128])?;
                writer.write_all(&ack.length_when_serialized().to_le_bytes())?;
                let mut writer = HashedWriter::new(ScramblingWriter::new(&mut writer));
                ack.serialize_to_stream(&mut writer)?;
                writer.result()
            }
            TransportPacketData::DeleteFile(ref delete_file) => {
                writer.write_all(&[129])?;
                writer.write_all(&delete_file.length_when_serialized().to_le_bytes())?;
                let mut writer = HashedWriter::new(ScramblingWriter::new(&mut writer));
                delete_file.serialize_to_stream(&mut writer)?;
                writer.result()
            }
        };

        writer.write_all(&hash.to_le_bytes())?;

        Ok(())
    }

    fn length_when_serialized(&self) -> u32 {
        1 // Type
        + 4// Length
        + self.inner_data_len()
    }

    fn deserialize_from_stream(mut reader: &mut impl std::io::Read) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let mut type_buf = [0u8; 1];
        reader.read_exact(&mut type_buf)?;
        let type_ = type_buf[0];

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

        let mut inner_reader = SubstreamReader::new(&mut reader, len as usize);
        let mut inner_reader = HashedReader::new(UnscramblingReader::new(&mut inner_reader));
        let inner_data = parse_data_from_type(type_, &mut inner_reader)?;
        let calculated_hash = inner_reader.result();

        let mut hash_buf = [0u8; 8];
        reader.read_exact(&mut hash_buf)?;
        let hash = u64::from_le_bytes(hash_buf);

        if hash != calculated_hash {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid packet hash",
            ));
        }

        Ok(Self { data: inner_data })
    }
}

impl ValidityCheck for TransportPacketInner {
    fn is_valid(&self) -> bool {
        self.data.is_valid() && self.inner_data_len() <= Self::MAX_DATA_LEN as u32
    }
}

impl ValidityCheck for TransportPacket {
    fn is_valid(&self) -> bool {
        self.data.is_valid()
    }
}

impl ValidityCheck for TransportPacketData {
    fn is_valid(&self) -> bool {
        match self {
            TransportPacketData::HeaderChunk(header_chunk) => header_chunk.is_valid(),
            TransportPacketData::DataChunk(data_chunk) => data_chunk.is_valid(),
            TransportPacketData::AcknowledgementPacket(ack) => ack.is_valid(),
            TransportPacketData::DeleteFile(delete_file) => delete_file.is_valid(),
        }
    }
}

fn parse_data_from_type(
    type_: u8,
    stream: &mut impl std::io::Read,
) -> std::io::Result<TransportPacketData> {
    let data = match type_ {
        0 => TransportPacketData::HeaderChunk(crate::chunks::HeaderChunk::deserialize_from_stream(
            stream,
        )?),
        1 => TransportPacketData::DataChunk(crate::chunks::DataChunk::deserialize_from_stream(
            stream,
        )?),
        128 => TransportPacketData::AcknowledgementPacket(
            crate::control::ConfirmPart::deserialize_from_stream(stream)?,
        ),
        129 => TransportPacketData::DeleteFile(
            crate::control::DeleteFile::deserialize_from_stream(stream)?,
        ),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid transport packet type {}", type_),
            ))
        }
    };

    Ok(data)
}

#[cfg(test)]
mod tests {
    use crate::chunks::DataChunk;

    use super::*;

    #[test]
    fn test_transport_packet() {
        let packet = TransportPacketData::DataChunk(DataChunk {
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            file_id: Default::default(),
            part: 12,
        });

        let packet = TransportPacket::new(packet);

        let mut serialized = Vec::new();
        packet.serialize_to_stream(&mut serialized).unwrap();
        dbg!(&serialized);

        let mut deserialized = std::io::Cursor::new(serialized);
        let deserialized = TransportPacket::deserialize_from_stream(&mut deserialized).unwrap();

        assert_eq!(deserialized.data(), packet.data());
    }
}
