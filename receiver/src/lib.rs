use std::{
    fs::File,
    io::{self, Cursor, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use common::{
    chunks::Chunk,
    control::ControlMessage,
    header::{FileHeaderData, FilePartId},
};

// Received file folder structure
// [file uuid]/
//     header.json        - The header of the file, if received (can be absent)
//     [part index].bin   - The received parts of the file, added as they are received

// A file is finished when all the parts are present

pub struct Reciever {
    workdir_folder: PathBuf,
    result_folder: PathBuf,

    control_msg_queue: Vec<ControlMessage>,
}

impl Reciever {
    pub fn receive_chunk(&mut self, chunk: Chunk) -> anyhow::Result<()> {
        // Ensure the folder for this file exists
        let file_folder = self.workdir_folder.join(chunk.file_id.to_string());
        std::fs::create_dir_all(&file_folder)?;

        match chunk.part {
            FilePartId::Header => {
                let header = FileHeaderData::deserialize_from_stream(Cursor::new(chunk.data))?;

                // Write the header json
                let header_json_path = file_folder.join("header.json");
                let mut header_json_file = File::create(&header_json_path)
                    .context("Failed to create header json file in destination folder")?;

                header.serialize_to_json_stream(&mut header_json_file)?;
            }
            FilePartId::Part(part_index) => {
                let filename = format!("{}.bin", part_index);

                let part_path = file_folder.join(filename);
                let mut part_file = File::create(&part_path)
                    .context("Failed to create part file in destination folder")?;

                part_file.write_all(&chunk.data)?;
            }
        }

        self.control_msg_queue.push(ControlMessage::ConfirmPart {
            file_id: chunk.file_id,
            part_index: chunk.part,
        });

        Ok(())
    }

    fn get_all_file_folders(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut folders = Vec::new();

        for entry in std::fs::read_dir(&self.workdir_folder).context("Failed to read workdir")? {
            let entry = entry.context("Failed to read workdir entry")?;
            let path = entry.path();
            if path.is_dir() {
                folders.push(path);
            }
        }

        Ok(folders)
    }

    pub fn output_finished_files(&self) -> anyhow::Result<()> {
        // List all unfinished file folders
        let file_folders = self.get_all_file_folders()?;

        for file_folder in file_folders {
            if is_file_finished(&file_folder)? {
                write_finished_file_to_output_folder(&file_folder, &self.result_folder)?;
            }
        }

        Ok(())
    }
}

fn is_file_finished(file_folder: &Path) -> anyhow::Result<bool> {
    let header_json_path = file_folder.join("header.json");
    if !header_json_path.exists() {
        return Ok(false);
    }

    let header = FileHeaderData::deserialize_from_stream(File::open(header_json_path)?)?;

    for part_index in 0..header.part_count {
        let filename = format!("{}.bin", part_index);
        let part_path = file_folder.join(filename);

        if !part_path.exists() {
            return Ok(false);
        }
    }

    Ok(true)
}

fn write_finished_file_to_output_folder(
    file_folder: &Path,
    output_folder: &Path,
) -> anyhow::Result<()> {
    let header_json_path = file_folder.join("header.json");
    let header = FileHeaderData::deserialize_from_stream(File::open(header_json_path)?)?;

    let filename = header.name;

    // Find a valid filename for the output folder, adding `(n)` to the end if necessary
    let mut output_path = output_folder.join(&filename);
    let mut i = 0;
    while output_path.exists() {
        i += 1;
        output_path = output_folder.join(format!("{} ({})", &filename, i));
    }

    let mut output = File::create(output_path)?;
    for part_index in 0..header.part_count {
        let filename = format!("{}.bin", part_index);
        let part_path = file_folder.join(filename);

        let mut part_file = File::open(part_path)?;

        io::copy(&mut part_file, &mut output)?;
    }

    Ok(())
}
