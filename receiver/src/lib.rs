use std::{
    collections::HashMap,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use common::{
    chunks::{Chunk, HeaderChunk},
    control::{ConfirmPart, ControlMessage},
    file_part_id::FilePartId,
};
use uuid::Uuid;

// Received file folder structure
// [file uuid]/
//     header.json        - The header of the file, if received (can be absent)
//     [part index].bin   - The received parts of the file, added as they are received

// A file is finished when all the parts are present

pub struct Reciever {
    workdir_folder: PathBuf,
    result_folder: PathBuf,

    confirmed_parts: HashMap<Uuid, Vec<FilePartId>>,
}

impl Reciever {
    pub fn new(workdir_folder: PathBuf, result_folder: PathBuf) -> anyhow::Result<Self> {
        // Ensure that both folders exist
        std::fs::create_dir_all(&workdir_folder)?;
        std::fs::create_dir_all(&result_folder)?;

        Ok(Self {
            workdir_folder,
            result_folder,

            confirmed_parts: HashMap::new(),
        })
    }

    fn get_data_folder_path_for_id(&self, file_id: Uuid) -> PathBuf {
        self.workdir_folder.join(file_id.to_string())
    }

    pub fn receive_chunk(&mut self, chunk: Chunk) -> anyhow::Result<()> {
        let file_id = chunk.file_id();
        let part_index = chunk.part_index();
        let path = self.get_data_folder_path_for_id(file_id);

        if is_file_finished(&path)? {
            // Already finished, confirm and ignore
            self.confirmed_parts
                .entry(file_id)
                .or_default()
                .push(part_index);

            return Ok(());
        }

        // Ensure the folder for this file exists
        let file_data_folder = get_data_folder_path(&path);
        std::fs::create_dir_all(&file_data_folder)?;

        match chunk {
            Chunk::Header(header_chunk) => {
                // Write the header json
                let header_json_tmp_path = file_data_folder.join("header.json.tmp");
                let mut header_json_file = File::create(&header_json_tmp_path)
                    .context("Failed to create header json file in destination folder")?;

                serde_json::to_writer(&mut header_json_file, &header_chunk)
                    .context("Failed to write header json file")?;

                // Copy the header json to the final location
                let header_json_path = file_data_folder.join("header.json");
                std::fs::rename(header_json_tmp_path, header_json_path)?;
            }
            Chunk::Data(data_chunk) => {
                let filename = format!("{}.bin", data_chunk.part);

                let part_path = file_data_folder.join(filename);
                let mut part_file = File::create(part_path)
                    .context("Failed to create part file in destination folder")?;

                part_file.write_all(&data_chunk.data)?;
            }
        }

        self.confirmed_parts
            .entry(file_id)
            .or_default()
            .push(part_index);

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
                continue;
            }

            if is_file_data_finished(&file_folder)? {
                write_finished_file_to_output_folder(&file_folder, &self.result_folder)?;

                // Mark as finished
                mark_folder_as_finished(&file_folder)?;
            }
        }

        Ok(())
    }

    pub fn iter_control_messages(&mut self) -> impl Iterator<Item = ControlMessage> + '_ {
        #![allow(clippy::unnecessary_unwrap)]

        self.confirmed_parts
            .drain()
            .flat_map(|(file_id, mut parts)| {
                // Sort the parts
                parts.sort_unstable();

                // Capture inclusive ranges of all the parts
                let mut ranges = Vec::new();
                let mut start = None;
                let mut end = None;

                for part in parts {
                    if start.is_none() {
                        start = Some(part);
                        end = Some(part);
                    } else if end.unwrap().to_index() == part.to_index() - 1 {
                        end = Some(part);
                    } else {
                        ranges.push((start.unwrap(), end.unwrap()));
                        start = Some(part);
                        end = Some(part);
                    }
                }

                if start.is_some() {
                    ranges.push((start.unwrap(), end.unwrap()));
                }

                ranges.into_iter().map(move |(from, to)| {
                    ControlMessage::ConfirmPart(ConfirmPart {
                        file_id,
                        part_range: common::file_part_id::FilePartIdRangeInclusive::new(from, to),
                    })
                })
            })
    }
}

fn get_data_folder_path(path: &Path) -> PathBuf {
    path.join("data")
}

fn get_marker_file_path(path: &Path) -> PathBuf {
    path.join("finished")
}

fn is_file_finished(path: &Path) -> anyhow::Result<bool> {
    let marker_file = get_marker_file_path(path);
    Ok(marker_file.try_exists()?)
}

fn mark_folder_as_finished(path: &Path) -> anyhow::Result<()> {
    // Delete the data
    let data_folder = get_data_folder_path(path);
    std::fs::remove_dir_all(data_folder)?;

    // Create the marker file
    let marker_file = get_marker_file_path(path);
    File::create(marker_file)?;
    Ok(())
}

fn is_file_data_finished(file_folder: &Path) -> anyhow::Result<bool> {
    let data_folder = get_data_folder_path(file_folder);

    let header_json_path = data_folder.join("header.json");
    if !header_json_path.exists() {
        return Ok(false);
    }

    let header: HeaderChunk = serde_json::from_reader(File::open(header_json_path)?)?;

    for part_index in 0..header.part_count {
        let filename = format!("{}.bin", part_index);
        let part_path = data_folder.join(filename);

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
    let data_folder = get_data_folder_path(file_folder);
    let header_json_path = data_folder.join("header.json");
    let header: HeaderChunk = serde_json::from_reader(File::open(header_json_path)?)?;

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
        let part_path = data_folder.join(filename);

        let mut part_file = File::open(part_path)?;

        io::copy(&mut part_file, &mut output)?;
    }

    Ok(())
}
