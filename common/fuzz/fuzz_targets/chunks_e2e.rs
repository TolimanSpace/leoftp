#![no_main]

use std::io::{Cursor, Seek, SeekFrom};

use common::{binary_serialize::BinarySerialize, chunks::Chunk, validity::ValidityCheck};
use libfuzzer_sys::fuzz_target;
use std::fmt::Debug;

fuzz_target!(|data: Chunk| {
    match data {
        Chunk::Data(data_chunk) => check_message(data_chunk),
        Chunk::Header(header_chunk) => check_message(header_chunk),
    }
});

fn check_message<T: BinarySerialize + ValidityCheck + PartialEq + Debug>(data: T) {
    if !data.is_valid() {
        return;
    }

    let mut stream = Cursor::new(Vec::new());

    data.serialize_to_stream(&mut stream).unwrap();

    assert_eq!(data.length_when_serialized(), stream.position() as u32);

    stream.seek(SeekFrom::Start(0)).unwrap();
    let deserialized = T::deserialize_from_stream(&mut stream).unwrap();

    assert_eq!(data, deserialized);
}
