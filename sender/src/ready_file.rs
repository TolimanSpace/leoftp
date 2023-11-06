use std::{
    fs::{self, File},
    io::{self, Read, Seek},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use common::{
    binary_serialize::BinarySerialize,
    chunks::Chunk,
    header::{FileHeaderData, FilePartId},
};

use crate::FILE_PART_SIZE;

#[derive(Clone, Debug)]
/// A file that's ready for sending
pub struct ReadyFile {
    pub folder_path: PathBuf,
    pub header: FileHeaderData,
    pub unsent_parts: Vec<FilePartId>,
}

// Ready folder structure:
// ready/
//    file            - Raw binary file blob
//    header          - Raw binary file header to be sent as the first part
//    header.json     - The header data, but in json format, for easier inspection
//    unsent-parts/   - Folder containing indexes of unsent parts. They are deleted whenever they're confirmed to be recieved.
//       header
//       0
//       1
//       2
//       ...

// Ready folder considerations:
// - If any of the files are missing, then the folder is invalid and should be deleted (`file` is always copied in last, so if it's missing, then the folder is incomplete)

pub fn parse_ready_folder(path: PathBuf) -> anyhow::Result<ReadyFile> {
    // First, check that all the files/folders are there
    let file_path = path.join("file");
    if !file_path.exists() {
        bail!("File is missing");
    }

    let header_path = path.join("header");
    if !header_path.exists() {
        bail!("Header is missing");
    }

    let unsent_parts_path = path.join("unsent-parts");
    if !unsent_parts_path.exists() {
        bail!("Unsent parts folder is missing");
    }

    // Then, parse the metadata
    let header = FileHeaderData::deserialize_from_stream(&mut File::open(header_path)?)?;

    // Then, parse the unsent parts
    let mut unsent_parts = Vec::new();
    for entry in fs::read_dir(unsent_parts_path)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_str().unwrap();
        if let Some(part) = FilePartId::from_string(file_name) {
            unsent_parts.push(part);
        }
    }

    Ok(ReadyFile {
        folder_path: path,
        header,
        unsent_parts,
    })
}

pub fn write_ready_file_folder(
    destination_path: PathBuf,
    source_file: PathBuf,
    header: FileHeaderData,
) -> anyhow::Result<()> {
    // Make sure the destination folder exists
    fs::create_dir_all(&destination_path).context("Failed to create destination folder")?;

    // Write header
    let header_path = destination_path.join("header");
    let mut header_file =
        File::create(&header_path).context("Failed to create header file in destination folder")?;
    header.serialize_to_stream(&mut header_file)?;

    // Write header json
    let header_json_path = destination_path.join("header.json");
    let mut header_json_file = File::create(&header_json_path)
        .context("Failed to create header json file in destination folder")?;
    header.serialize_to_json_stream(&mut header_json_file)?;

    // Write unsent parts
    let unsent_parts_path = destination_path.join("unsent-parts");
    fs::create_dir_all(&unsent_parts_path)
        .context("Failed to create unsent parts folder in destination folder")?;

    // Write the header part
    let header_part = FilePartId::Header;
    let header_part_path = unsent_parts_path.join(header_part.as_string());
    File::create(&header_part_path)
        .context("Failed to create unsent part file in destination folder")?;

    // Write the body parts
    for body_part in 0..header.part_count {
        let part = FilePartId::Part(body_part);
        let part_path = unsent_parts_path.join(part.as_string());
        File::create(&part_path)
            .context("Failed to create unsent part file in destination folder")?;
    }

    // Move file
    let destination_file_path = destination_path.join("file");
    fs::rename(source_file, destination_file_path)
        .context("Failed to move file to destination folder")?;

    Ok(())
}

impl ReadyFile {
    pub fn get_unsent_part_as_chunk(&self, part: FilePartId) -> anyhow::Result<Chunk> {
        let data = match part {
            FilePartId::Header => {
                let mut file = File::open(self.folder_path.join("header"))?;
                let mut data = Vec::new();
                file.read_to_end(&mut data)?;

                data
            }
            FilePartId::Part(i) => {
                let mut file = File::open(self.folder_path.join("file"))?;

                let mut data = vec![0u8; FILE_PART_SIZE as usize];
                file.seek(io::SeekFrom::Start((i * FILE_PART_SIZE) as u64))?;
                let byte_count = file.read(&mut data)?;
                data.truncate(byte_count);

                data
            }
        };

        Ok(Chunk {
            file_id: self.header.id,
            part,
            data,
        })
    }
}

pub fn mark_part_as_sent(folder: &Path, part: FilePartId) -> anyhow::Result<()> {
    let unsent_parts_path = folder.join("unsent-parts");
    let part_path = unsent_parts_path.join(part.as_string());
    if !part_path.exists() {
        return Ok(()); // Already removed
    }
    fs::remove_file(part_path).context("Failed to remove part file")?;

    Ok(())
}

pub fn is_file_fully_confirmed(folder: &Path) -> anyhow::Result<bool> {
    let unsent_parts_path = folder.join("unsent-parts");

    for entry in fs::read_dir(unsent_parts_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            return Ok(false);
        }
    }

    Ok(true)
}
