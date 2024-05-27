#![no_main]

use common::transport_packet::parse_transport_packet_stream;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cursor = std::io::Cursor::new(data);
    let mut result_chunks = Vec::new();
    for chunk in parse_transport_packet_stream(&mut cursor) {
        result_chunks.push(chunk.unwrap());
    }
});
