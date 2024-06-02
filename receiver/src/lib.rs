use std::{
    collections::HashMap,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use common::{
    chunks::{Chunk, DataChunk, HeaderChunk},
    control::{ConfirmPart, ControlMessage},
    file_part_id::FilePartId,
};
use uuid::Uuid;

// Received file folder structure
// [file uuid]/
//     header.json        - The header of the file, if received (can be absent)
//     [part index].bin   - The received parts of the file, added as they are received
//     finished           - A file is finished when this file exists. This is for tracking files that were historically completed, but the confirmation was lost.

// A file is finished when all the parts are present

pub struct Reciever {
    workdir_folder: PathBuf,
    result_folder: PathBuf,

    confirmed_parts: HashMap<Uuid, Vec<FilePartId>>,
    finished_files: Vec<PathBuf>,
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
            finished_files: Vec::new(),
        })
    }

    fn get_data_folder_path_for_id(&self, file_id: Uuid) -> PathBuf {
        self.workdir_folder.join(file_id.to_string())
    }

    fn add_confirmation_for_file_part(&mut self, file_id: Uuid, part_index: FilePartId) {
        self.confirmed_parts
            .entry(file_id)
            .or_default()
            .push(part_index);
    }

    pub fn receive_chunk(&mut self, chunk: Chunk) -> anyhow::Result<()> {
        let file_id = chunk.file_id();
        let part_index = chunk.part_index();
        let path = self.get_data_folder_path_for_id(file_id);

        let managed_file = ManagedReceivingFile::open_or_create(path)?;

        if managed_file.is_finished()? {
            // Already finished, make a confirmation and ignore
            self.add_confirmation_for_file_part(file_id, part_index);
            return Ok(());
        }

        match chunk {
            Chunk::Header(header_chunk) => {
                managed_file.receive_header_chunk(header_chunk)?;
            }
            Chunk::Data(data_chunk) => {
                managed_file.receive_data_chunk(data_chunk)?;
            }
        }

        self.add_confirmation_for_file_part(file_id, part_index);

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

    pub fn output_finished_files(&mut self) -> anyhow::Result<()> {
        // List all unfinished file folders
        let file_folders = self.get_all_file_folders()?;

        for file_folder in file_folders {
            let managed_file = ManagedReceivingFile::open_or_create(file_folder)?;

            if managed_file.is_finished()? {
                continue;
            }

            if managed_file.is_file_data_finished()? {
                let path =
                    managed_file.write_finished_file_to_output_folder(&self.result_folder)?;

                self.finished_files.push(path);
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

    pub fn iter_finished_files(&mut self) -> impl Iterator<Item = PathBuf> + '_ {
        self.finished_files.drain(..)
    }
}

pub struct ManagedReceivingFile {
    path: PathBuf,
}

impl ManagedReceivingFile {
    pub fn open_or_create(path: PathBuf) -> anyhow::Result<Self> {
        let file = Self { path };
        std::fs::create_dir_all(&file.path)?;
        std::fs::create_dir_all(&file.get_data_folder_path())?;

        Ok(file)
    }

    pub fn is_finished(&self) -> anyhow::Result<bool> {
        let marker_file = self.get_marker_file_path();
        Ok(marker_file.try_exists()?)
    }

    pub fn is_file_data_finished(&self) -> anyhow::Result<bool> {
        let header_json_path = self.get_header_json_path();
        if !header_json_path.exists() {
            return Ok(false);
        }

        let header: HeaderChunk = self.get_header()?;
        for part_index in 0..header.part_count {
            let part_path = self.get_bin_path(part_index);

            if !part_path.exists() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn receive_header_chunk(&self, chunk: HeaderChunk) -> anyhow::Result<()> {
        let header_json_tmp_path = self.get_header_json_path().with_extension(".json.tmp");
        let mut header_json_file = File::create(&header_json_tmp_path)
            .context("Failed to create header json file in destination folder")?;

        serde_json::to_writer(&mut header_json_file, &chunk)
            .context("Failed to write header json file")?;

        // Copy the header json to the final location
        let header_json_path = self.get_header_json_path();
        std::fs::rename(header_json_tmp_path, header_json_path)?;

        Ok(())
    }

    pub fn receive_data_chunk(&self, chunk: DataChunk) -> anyhow::Result<()> {
        let part_path = self.get_bin_path(chunk.part);
        let mut part_file =
            File::create(part_path).context("Failed to create part file in destination folder")?;

        part_file.write_all(&chunk.data)?;

        Ok(())
    }

    pub fn write_finished_file_to_output_folder(
        self,
        output_folder: &Path,
    ) -> anyhow::Result<PathBuf> {
        let header: HeaderChunk = self.get_header()?;

        let filename = header.name;

        // Find a valid filename for the output folder, adding `(n)` to the end if necessary
        let mut output_path = output_folder.join(&filename);
        let mut i = 0;
        while output_path.exists() {
            i += 1;
            output_path = output_folder.join(format!("{} ({})", &filename, i));
        }

        let mut output = File::create(&output_path)?;
        for part_index in 0..header.part_count {
            let part_path = self.get_bin_path(part_index);
            let mut part_file = File::open(part_path)?;

            io::copy(&mut part_file, &mut output)?;
        }

        // Create the marker file
        let marker_file = self.get_marker_file_path();
        File::create(marker_file)?;

        // Delete the data
        let data_folder = self.get_data_folder_path();
        std::fs::remove_dir_all(data_folder)?;

        Ok(output_path)
    }

    fn get_data_folder_path(&self) -> PathBuf {
        self.path.join("data")
    }

    fn get_header_json_path(&self) -> PathBuf {
        self.path.join("header.json")
    }

    fn get_marker_file_path(&self) -> PathBuf {
        self.path.join("finished")
    }

    fn get_bin_path(&self, part_index: u32) -> PathBuf {
        let data_folder = self.get_data_folder_path();
        let filename = format!("{}.bin", part_index);
        data_folder.join(filename)
    }

    fn get_header(&self) -> anyhow::Result<HeaderChunk> {
        let header_json_path = self.get_header_json_path();
        let header: HeaderChunk = serde_json::from_reader(File::open(header_json_path)?)?;

        Ok(header)
    }
}
