#![no_main]

use common::binary_serialize::BinarySerialize;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cursor = std::io::Cursor::new(data);
    let _ = common::control::ConfirmPart::deserialize_from_stream(&mut cursor);
    let _ = common::control::DeleteFile::deserialize_from_stream(&mut cursor);
    let _ = common::control::SetFilePriority::deserialize_from_stream(&mut cursor);
});
