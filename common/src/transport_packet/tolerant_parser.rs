use std::hash::Hasher;
use std::io::{self, Read};

use crate::binary_serialize::BinarySerialize;
use crate::transport_packet::scrambling::UnscramblingReader;
use crate::transport_packet::substream::SubstreamReader;

use super::{checkpoint_stream::StreamWithCheckpoints, TransportPacket};

pub fn debug_stream(stream: &mut StreamWithCheckpoints<impl Read>, len: usize) {
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).unwrap();
    stream.rollback_n(len);
    let chars = buf.iter().map(|&b| b as char).collect::<Vec<char>>();
    println!("Stream: {:?}", chars);
}

pub fn parse_transport_packet_stream<T: BinarySerialize>(
    stream: impl Read,
) -> impl Iterator<Item = io::Result<T>> {
    let mut stream = StreamWithCheckpoints::new(stream);

    let mut errored = false;

    std::iter::from_fn(move || {
        loop {
            if errored {
                return None;
            }

            let result = parse_until_signature(&mut stream);
            match result {
                Ok(SearchSignatureStatus::Found) => {}
                Ok(SearchSignatureStatus::Eof) => return None,
                Err(e) => {
                    errored = true;
                    return Some(Err(e));
                }
            }

            stream.checkpoint();

            let next_packet = parse_next_packet::<T>(&mut stream);
            match next_packet {
                Ok(Ok(packet)) => {
                    stream.checkpoint();
                    return Some(Ok(packet));
                }
                Err(e) => {
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
                    ) {
                        // Unexpected EOF, or corrupt data, could be a corrupt packet length. Rollback and continue.
                        stream.rollback();
                        continue;
                    } else {
                        // Unexpected error in the stream
                        errored = true;
                        return Some(Err(e));
                    }
                }
                Ok(Err(_e)) => {
                    // Error parsing data, rollback and continue
                    stream.rollback();
                    continue;
                }
            }
        }
    })
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SearchSignatureStatus {
    Found,
    Eof,
}

fn parse_until_signature(
    stream: &mut StreamWithCheckpoints<impl Read>,
) -> io::Result<SearchSignatureStatus> {
    let mut buf = [0u8; 256];

    loop {
        stream.checkpoint();
        let read = stream.read(&mut buf).unwrap();

        if read < 4 {
            return Ok(SearchSignatureStatus::Eof);
        }

        if let Some(index) = index_of_signature(&buf[..read]) {
            let backwards = read - index;
            stream.rollback_n(backwards);

            return Ok(SearchSignatureStatus::Found);
        }

        stream.rollback_n(3); // Rollback by 3 so that we can find the signature if it's split across two reads
    }
}

fn index_of_signature(buf: &[u8]) -> Option<usize> {
    if buf.len() < 6 {
        return None;
    }

    let mut i = 0;
    while i < buf.len() - 3 {
        if buf[i] == b'L' && buf[i + 1] == b'F' && buf[i + 2] == b'T' && buf[i + 3] == b'P' {
            return Some(i);
        }

        i += 1;
    }

    None
}

/// Returns a dual error type, where the outer error is a packet parsing error,
/// and the inner error is a data deserialization error.
fn parse_next_packet<T: BinarySerialize>(
    mut stream: &mut StreamWithCheckpoints<impl Read>,
) -> io::Result<io::Result<T>> {
    // Parse signature
    let mut signature_buf = [0u8; 4];
    stream.read_exact(&mut signature_buf)?;
    debug_assert_eq!(&signature_buf, b"LFTP");

    stream.checkpoint();

    // Parse length
    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf)?;
    let length = u32::from_le_bytes(length_buf);

    if length as usize > TransportPacket::<T>::MAX_DATA_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Transport packet length {} exceeds maximum {}",
                length,
                TransportPacket::<T>::MAX_DATA_LEN
            ),
        ));
    }

    stream.checkpoint();

    let mut unscrambled_stream = UnscramblingReader::new(&mut stream);

    // Read ahead, ensure that the hash is valid
    let mut hasher = twox_hash::XxHash64::with_seed(0);

    let mut buffer = [0u8; 1024];
    let mut parsed = 0;
    while parsed < length as usize {
        let to_read = std::cmp::min(buffer.len(), length as usize - parsed);
        unscrambled_stream.read_exact(&mut buffer[..to_read])?;
        parsed += to_read;

        hasher.write(&buffer[..to_read]);
    }

    let mut hash_buf = [0u8; 8];
    stream.read_exact(&mut hash_buf)?;
    let hash = u64::from_le_bytes(hash_buf);

    let calculated_hash = hasher.finish();
    if hash != calculated_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid packet hash",
        ));
    }

    stream.rollback();

    let mut substream = SubstreamReader::new(UnscramblingReader::new(&mut stream), length as usize);
    let parsed = T::deserialize_from_stream(&mut substream);
    let reached_end = substream.reached_end();

    // Skip hash value
    stream.skip_and_checkpoint(8);

    let parsed = match parsed {
        Ok(parsed) => Ok(parsed),
        Err(e) => return Ok(Err(e)),
    };

    if !reached_end {
        return Ok(Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Packet length does not match actual length",
        )));
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_serialize::BinarySerialize;
    use crate::chunks::Chunk;
    use crate::header::FilePartId;
    use crate::transport_packet::TransportPacket;

    fn make_dummy_packets_list(count: usize) -> Vec<TransportPacket<Chunk>> {
        let mut packets = Vec::new();

        for i in 0..count {
            let mut data = Vec::new();
            for _ in 0..i {
                data.push(i as u8);
            }

            let packet = TransportPacket::new(Chunk {
                file_id: Default::default(),
                part: FilePartId::Part(i as u32),
                data,
            });

            packets.push(packet);
        }

        packets
    }

    #[test]
    fn test_stream_uncorrupt() {
        let packets = make_dummy_packets_list(10);

        let mut stream = Vec::new();
        for packet in &packets {
            packet.serialize_to_stream(&mut stream).unwrap();
        }

        let mut stream = std::io::Cursor::new(stream);
        let mut parsed_packets = parse_transport_packet_stream::<Chunk>(&mut stream);

        for packet in packets {
            println!("Read packet, {}", packet.data.data.data.len());
            let parsed_packet = parsed_packets.next().unwrap().unwrap();
            assert_eq!(parsed_packet, packet.data());
        }
    }

    #[test]
    fn test_stream_corrupt_padded() {
        let packets = make_dummy_packets_list(10);

        let mut stream = Vec::new();
        for packet in &packets {
            packet.serialize_to_stream(&mut stream).unwrap();

            // append some corruption
            stream.extend_from_slice(&[1, 2, 3, 4]);
        }

        let mut stream = std::io::Cursor::new(stream);
        let mut parsed_packets = parse_transport_packet_stream::<Chunk>(&mut stream);

        for packet in packets {
            let parsed_packet = parsed_packets.next().unwrap().unwrap();
            assert_eq!(parsed_packet, packet.data());
        }
    }

    #[test]
    fn test_stream_corrupt_packets() {
        let packets = make_dummy_packets_list(10);

        let mut stream = Vec::new();
        for (i, packet) in packets.iter().enumerate() {
            let mut vec = Vec::new();
            packet.serialize_to_stream(&mut vec).unwrap();

            if i % 2 == 0 {
                // Corrupt one byte
                let arbitrary_byte = (((vec.len() - i) * 1000) % 256) % vec.len();
                vec[arbitrary_byte] += 1;
            }

            stream.extend_from_slice(&vec);
        }

        let mut stream = std::io::Cursor::new(stream);
        let mut parsed_packets = parse_transport_packet_stream::<Chunk>(&mut stream);

        for (i, packet) in packets.into_iter().enumerate() {
            if i % 2 == 0 {
                // Corrupt packet, skip
                continue;
            }

            let parsed_packet = parsed_packets.next().unwrap().unwrap();
            assert_eq!(parsed_packet, packet.data());
        }
    }
}
