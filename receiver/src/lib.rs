use std::path::PathBuf;

mod partial_file;

// Received file folder structure
// [file uuid]/
//     header.json        - The header of the file, if received (can be absent)
//     [part index].bin   - The received parts of the file, added as they are received

// A file is finished when all the parts are present

pub struct Reciever {
    workdir_folder: PathBuf,
    result_folder: PathBuf,
}

impl Reciever {}
