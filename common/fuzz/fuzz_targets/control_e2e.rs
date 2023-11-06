#![no_main]

use std::io::{Cursor, Seek, SeekFrom};

use common::{binary_serialize::BinarySerialize, control::ControlMessage};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: ControlMessage| {
    let mut stream = Cursor::new(Vec::new());

    data.serialize_to_stream(&mut stream).unwrap();
    stream.seek(SeekFrom::Start(0)).unwrap();
    let deserialized = ControlMessage::deserialize_from_stream(&mut stream).unwrap();

    assert_eq!(data, deserialized);
});
