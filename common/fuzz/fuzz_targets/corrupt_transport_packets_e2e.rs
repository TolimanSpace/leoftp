#![no_main]

use std::io::{Cursor, Seek, SeekFrom};

use common::{
    binary_serialize::BinarySerialize,
    chunks::Chunk,
    header::FileHeaderData,
    transport_packet::{parse_transport_packet_stream, TransportPacket},
    validity::ValidityCheck,
};
use libfuzzer_sys::{arbitrary::Arbitrary, fuzz_target};

#[derive(Clone, Copy, Debug, Arbitrary)]
struct ByteAdd {
    index: usize,
    value: u8,
}

#[derive(Clone, Copy, Debug, Arbitrary)]
struct ByteInsert {
    index: usize,
    value: u8,
}

#[derive(Clone, Copy, Debug, Arbitrary)]
struct ByteRemove {
    index: usize,
}

#[derive(Clone, Copy, Debug, Arbitrary)]
struct ByteRemoveRange {
    len: usize,
}

#[derive(Clone, Debug, Arbitrary)]
enum CorruptionKind {
    NoCorruption1,
    NoCorruption2,
    NoCorruption3,
    NoCorruption4,
    NoCorruption5,
    NoCorruption6,
    NoCorruption7,
    NoCorruption8,
    ByteAdd(ByteAdd),
    MultiByteAdd(Vec<ByteAdd>),
    ByteInsert(ByteInsert),
    MultiByteInsert(Vec<ByteInsert>),
    ByteRemove(ByteRemove),
    ByteRemoveStart(ByteRemoveRange),
    ByteRemoveEnd(ByteRemoveRange),
}

fn corrupt_buffer(buf: &mut Vec<u8>, corruption: &CorruptionKind) {
    if buf.is_empty() {
        return;
    }

    match corruption {
        CorruptionKind::NoCorruption1
        | CorruptionKind::NoCorruption2
        | CorruptionKind::NoCorruption3
        | CorruptionKind::NoCorruption4
        | CorruptionKind::NoCorruption5
        | CorruptionKind::NoCorruption6
        | CorruptionKind::NoCorruption7
        | CorruptionKind::NoCorruption8 => {}
        CorruptionKind::ByteAdd(ByteAdd { index, value }) => {
            let index = *index % buf.len();
            let value = (*value).max(1); // shouldn't be adding 0

            buf[index] = buf[index].wrapping_add(value);
        }
        CorruptionKind::MultiByteAdd(adds) => {
            for ByteAdd { index, value } in adds {
                let index = *index % buf.len();
                let value = (*value).max(1); // shouldn't be adding 0

                buf[index] = buf[index].wrapping_add(value);
            }
        }
        CorruptionKind::ByteInsert(ByteInsert { index, value }) => {
            if buf.len() < 3 {
                return;
            }
            let index = (*index % (buf.len() - 2)) + 1; // Don't insert at start or end
            buf.insert(index, *value);
        }
        CorruptionKind::MultiByteInsert(inserts) => {
            if buf.len() < 3 {
                return;
            }
            for ByteInsert { index, value } in inserts {
                let index = (index % (buf.len() - 2)) + 1; // Don't insert at start or end
                buf.insert(index, *value);
            }
        }
        CorruptionKind::ByteRemove(ByteRemove { index }) => {
            let index = *index % buf.len();
            buf.remove(index);
        }
        CorruptionKind::ByteRemoveStart(ByteRemoveRange { len }) => {
            let len = *len % (buf.len() / 2);
            buf.drain(0..len);
        }
        CorruptionKind::ByteRemoveEnd(ByteRemoveRange { len }) => {
            let len = *len % (buf.len() / 2);
            let buf_len = buf.len();
            buf.drain(buf_len - len..buf_len);
        }
    }
}

#[derive(Clone, Debug, Arbitrary)]
struct ChunkWithCorruption {
    chunk: Chunk,
    corruption: CorruptionKind,
}

struct ChunkSerialized {
    chunk: Chunk,
    bytes_corrupt: Vec<u8>,
    bytes_uncorrupt: Vec<u8>,
}

// So the way this works is:
// 1. Generate a bunch of chunks, alongside the optional corruption to apply to them
// 2. Serialize them into a stream, some chunks with their added corruptions
// (A chunk is considred corrupt if the corrupt array differs from the initial array)
// 3. Deserialize the stream
// 4. Compare the array of deserialize chunks with the array of chunks that were not corrupted

fuzz_target!(|data: Vec<ChunkWithCorruption>| {
    // Filter invalid chunks
    let data: Vec<ChunkWithCorruption> = data.into_iter().filter(|c| c.chunk.is_valid()).collect();

    // Serialize
    let serialized = data
        .iter()
        .cloned()
        .map(|ChunkWithCorruption { chunk, corruption }| {
            let chunk_cloned = chunk.clone();

            // As a transport packet
            let transport_packet = TransportPacket::new(chunk);

            // Serialize
            let mut bytes = Vec::new();
            transport_packet.serialize_to_stream(&mut bytes).unwrap();

            // Corrupt
            let mut bytes_corrupt = bytes.clone();
            corrupt_buffer(&mut bytes_corrupt, &corruption);

            ChunkSerialized {
                chunk: chunk_cloned,
                bytes_corrupt,
                bytes_uncorrupt: bytes,
            }
        })
        .collect::<Vec<_>>();

    // Serialize the corrupt chunks into a stream
    let mut final_stream = Vec::<u8>::new();
    let mut chunk_start_positions = Vec::<usize>::new();
    for item in &serialized {
        chunk_start_positions.push(final_stream.len());
        final_stream.extend(&item.bytes_corrupt);
    }

    // Scan over the resulting stream, finding what's expected to be uncorrupt and what isn't, to assert for the result
    let mut expected_result_chunks = Vec::<Chunk>::new();
    let mut prev_corrupt = false;
    let mut prev_eats_into_next = false;
    for (item, pos) in serialized.iter().zip(chunk_start_positions) {
        if prev_eats_into_next {
            prev_eats_into_next = false;
            continue;
        }

        let uncorrupt_buf = &item.bytes_uncorrupt;

        // If the forwards bytes match, uncorrupt
        let uncorrupt_forwards = if pos + uncorrupt_buf.len() <= final_stream.len() {
            let result_buf = &final_stream[pos..pos + uncorrupt_buf.len()];
            uncorrupt_buf == result_buf
        } else {
            false
        };

        // If the backwards bytes match, AND the previous is corrupt (aka it will have random data to feed in), then uncorrupt
        let uncorrupt_backwards = if pos >= uncorrupt_buf.len() && prev_corrupt {
            let result_buf = &final_stream[pos - uncorrupt_buf.len()..pos];
            uncorrupt_buf == result_buf
        } else {
            false
        };

        let uncorrtupt = uncorrupt_forwards || uncorrupt_backwards;

        if uncorrtupt {
            expected_result_chunks.push(item.chunk.clone());
        }

        if uncorrupt_forwards && item.bytes_corrupt.len() < uncorrupt_buf.len() {
            prev_eats_into_next = true;
        }

        prev_corrupt = !uncorrtupt;
    }

    // Deserialize
    let mut cursor = Cursor::new(final_stream);
    let mut result_chunks = Vec::<Chunk>::new();
    for chunk in parse_transport_packet_stream(&mut cursor) {
        result_chunks.push(chunk.unwrap());
    }

    // It's difficult to make perfect, but sometimes real data can be created from multiple corrupt chunk boundaries overlapping,
    // so we just check for expected chunks only.
    for expected_chunk in &expected_result_chunks {
        if !result_chunks.contains(expected_chunk) {
            println!("Input: {:#?}", data);

            println!("Expected: {:#?}", expected_result_chunks);
            println!("Result: {:#?}", result_chunks);
            panic!("Chunks differ");
        }
    }

    let chunks = data
        .iter()
        .map(|ChunkWithCorruption { chunk, .. }| chunk)
        .collect::<Vec<_>>();

    // Then, check that every chunk in the result is part of the original input
    for result_chunk in &result_chunks {
        if !chunks.contains(&result_chunk) {
            println!("Input: {:#?}", data);

            println!("Expected: {:#?}", expected_result_chunks);
            println!("Result: {:#?}", result_chunks);
            panic!("Chunks differ");
        }
    }
});
