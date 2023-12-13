#![no_main]

use common::binary_serialize::BinarySerialize;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cursor = std::io::Cursor::new(data);
    let _ = common::chunks::DataChunk::deserialize_from_stream(&mut cursor);
    let _ = common::chunks::HeaderChunk::deserialize_from_stream(&mut cursor);
});
