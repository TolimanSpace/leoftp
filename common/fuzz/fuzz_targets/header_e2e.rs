#![no_main]

use std::io::{Cursor, Seek, SeekFrom};

use common::{binary_serialize::BinarySerialize, header::FileHeaderData, validity::ValidityCheck};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: FileHeaderData| {
    if !data.is_valid() {
        return;
    }

    let mut stream = Cursor::new(Vec::new());

    data.serialize_to_stream(&mut stream).unwrap();

    assert_eq!(data.length_when_serialized(), stream.position() as u32);

    stream.seek(SeekFrom::Start(0)).unwrap();
    let deserialized = FileHeaderData::deserialize_from_stream(&mut stream).unwrap();

    assert_eq!(data, deserialized);
});
