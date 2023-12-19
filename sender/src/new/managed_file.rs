use std::{
    fmt::Write,
    fs::File,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
};

use common::{
    binary_serialize::BinarySerialize, chunks::HeaderChunk, file_part_id::FilePartId,
    substream::SubstreamReader,
};
use serde::{Deserialize, Serialize};

/// Managed file structure:
///
/// ```plaintext
/// [folder root]/  - The root folder, usually the file id
/// ├── header.json - The human-readable header file. This is never read.
/// ├── state.json  - The human-readable state file. This is never read.
/// ├
/// ├── header.bin  - The machine-readable header file.
/// ├── state.bin   - The state file. It marks which parts are acknowledged.
/// ├
/// ├── data.bin    - The raw file binary data. This is present in contiguous mode.
/// └── data/       - The data folder. This is present in split mode.
///     ├── 0.bin   - Each part's data is stored in a separate file.
///     ├── 1.bin
///     └── 2.bin
/// ```
///
/// There are 2 modes: contiguous and split. In contiguous mode, the data is stored in a single
/// file. In split mode, the data is stored in multiple files. Split mode can save storage space for
/// files that have been mostly acknolwedged, with only a few parts remaining. But, they require
/// more disk reads/writes to create.
///
/// ## Creation process
///
/// When a managed file is being created, the following steps are made:
/// 1. Create the folder
/// 2. Create the header.json and state.json files
/// 3. Create the the header.bin file and the state.bin file
/// 4. Move in the data.bin file
///
/// The state should be "contiguous".
///
/// When reading header.bin, if it's "contugous" and data.bin is missing, then the file
/// is invalid, as the creation process must've been interrupted.
///
/// If the state is "contiguous" and the data folder exists, then the splitting process
/// must've been interrupted. In this case, the splitting process is run again.
///
/// ### Sending process
///
/// When a part of a contuguous managed file is sent, the state files should be updated.
///
/// ### Acknowledging process
///
/// When a part of a contuguous managed file part is acknowledged, the state files should be updated.
/// Nothing else is affected. If zero parts are left, then the managed file can safely be deleted.
///
/// ## Splitting process
///
/// When a managed file is being split, the following steps are made:
/// 1. Create the data folder
/// 2. Split the data.bin into the data folder based on remaining parts.
/// 3. Modify the state to be "split"
/// 4. Delete the data.bin file
///
/// If the state is "split" and the data folder is missing, then the state is invalid and
/// the file can be deleted.
///
/// If the state is "split" and the data.bin file exists, then it can be deleted, as the
/// state changes to "split" only after the file has been fully split.
///
/// If the state is "split" and there's files in the data folder that aren't in the state,
/// then they can be safely deleted as they're not needed anymore.
///
/// ### Sending process
///
/// When a part of a split managed file is sent, the state files should be updated.
///
/// ### Acknowledging process
///
/// When a part of a split managed file part is acknowledged, the state files should be updated first,
/// then the data file should be deleted after. If zero parts are left, then the managed file can
/// safely be deleted.
pub struct ManagedFile {
    pub folder_path: PathBuf,
    pub header: HeaderChunk,
    pub state: ManagedFileState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedFileState {
    pub mode: ManagedFileStateMode,
    pub remaining_parts: Vec<ManagedFileStatePart>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ManagedFileStateMode {
    Contiguous,
    Split,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedFileStatePart {
    pub part: FilePartId,
    pub priority: i16,
}

impl ManagedFile {
    /// Please reference the doc comment on [`ManagedFile`] for more information.
    pub fn create_new_from_header(
        path: impl AsRef<Path>,
        header: HeaderChunk,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let state = ManagedFileState::new_from_header(&header);

        // 1. Create the folder
        std::fs::create_dir_all(path)?;

        // 2. Create the header.json and state.json files
        write_file_atomic(path.join("header.json"), |file| {
            Ok(serde_json::to_writer_pretty(file, &header)?)
        })?;
        write_file_atomic(path.join("state.json"), |file| {
            Ok(serde_json::to_writer_pretty(file, &state)?)
        })?;

        // 3. Create the the header.bin file and the state.bin file
        write_file_atomic(path.join("header.bin"), |file| {
            Ok(header.serialize_to_stream(file)?)
        })?;
        write_file_atomic(path.join("state.bin"), |file| {
            Ok(bincode::serialize_into(file, &state)?)
        })?;

        // 4. Move in the data.bin file
        std::fs::rename(path.join("data.bin"), path.join("data.bin"))?;

        Ok(Self {
            folder_path: path.to_path_buf(),
            header,
            state,
        })
    }

    /// Please reference the doc comment on [`ManagedFile`] for more information.
    pub fn try_read_from_path(path: impl AsRef<Path>) -> anyhow::Result<Option<Self>> {
        // Logging rule: Use `tracing::warn` for recoverable states, and `tracing::error` for
        // unrecoverable states that will result in data loss.

        let path = path.as_ref();

        let cleanup = || {
            let result = std::fs::remove_dir_all(path);

            if let Err(e) = result {
                tracing::error!(
                    "Failed to delete invalid/corrupt managed file folder: {}",
                    e
                );
            }
        };

        // Check if the path exists and is a folder, otherwise return None.
        if !path.is_dir() {
            return Ok(None);
        }

        // Read header.bin, if it's missing then it's invalid
        let header_path = path.join("header.bin");
        if !header_path.is_file() {
            tracing::warn!(
                "Managed file folder was missing header.bin, deleting: {:?}",
                path
            );
            cleanup();
            return Ok(None);
        }

        let mut header_file = File::open(header_path)?;
        let header = HeaderChunk::deserialize_from_stream(&mut header_file)?;

        // Read state.bin, if it's missing then it's invalid
        let state_path = path.join("state.bin");
        if !state_path.is_file() {
            tracing::warn!(
                "Managed file folder was missing state.bin, deleting: {:?}",
                path
            );
            cleanup();
            return Ok(None);
        }

        let mut state_file = File::open(state_path)?;
        let state: ManagedFileState = bincode::deserialize_from(&mut state_file)?;

        let mut should_trigger_splitting = false;

        match state.mode {
            ManagedFileStateMode::Contiguous => {
                // If the state is "contugous" and data.bin is missing, then the file
                // is invalid, as the creation process must've been interrupted.
                let data_path = path.join("data.bin");
                if !data_path.is_file() {
                    tracing::warn!(
                        "Managed file folder (contiguous mode) was missing data.bin, deleting: {:?}",
                        path
                    );
                    cleanup();
                    return Ok(None);
                }

                // If the state is "contiguous" and the data folder exists, then the splitting process
                // must've been interrupted. In this case, the splitting process is run again.
                let data_folder_path = path.join("data");
                if data_folder_path.is_dir() {
                    tracing::warn!(
                        "Managed file folder (contiguous mode) had a data folder which should only exist in split mode. Splitting: {:?}",
                        path
                    );
                    should_trigger_splitting = true;
                }
            }
            ManagedFileStateMode::Split => {
                // If the state is "split" and the data folder is missing, then the state is invalid and
                // the file can be deleted.
                let data_folder_path = path.join("data");
                if !data_folder_path.is_dir() {
                    tracing::error!(
                        "Managed file folder (split mode) was missing data folder, deleting: {:?}",
                        path
                    );
                    cleanup();
                    return Ok(None);
                }

                // If the state is "split" and the data.bin file exists, then it can be deleted, as the
                // state changes to "split" only after the file has been fully split.
                let data_path = path.join("data.bin");
                if data_path.is_file() {
                    tracing::warn!(
                        "Managed file folder (split mode) had a data.bin file which should only exist in contiguous mode. Deleting data.bin in: {:?}",
                        path
                    );
                    std::fs::remove_file(data_path)?;
                }

                // If the state is "split" and there's files in the data folder that aren't in the state,
                // then they can be safely deleted as they're not needed anymore.
                let mut files = std::fs::read_dir(data_folder_path)?
                    .map(|e| e.map(|e| e.path()))
                    .collect::<Result<Vec<_>, _>>()?;

                let mut found_parts = 0;
                for file in files.iter_mut() {
                    // Unwrap is ok because it returns None only if the path ends with `..`
                    let file_name = file.file_name().unwrap().to_string_lossy();

                    // Remove the `.bin` at the end. If .bin isn't on the file, then it can be deleted.
                    if !file_name.ends_with(".bin") {
                        tracing::warn!(
                            "Managed file folder (split mode) had a file in the data folder that didn't end with .bin, deleting: {:?}",
                            file
                        );
                        std::fs::remove_file(file)?;
                        continue;
                    }
                    let file_part_name = &file_name[..file_name.len() - 4];

                    // If the part name is invalid, then it can be deleted.
                    let part = FilePartId::from_string(file_part_name);
                    let Some(part) = part else {
                        tracing::warn!(
                            "Managed file folder (split mode) had a file in the data folder that didn't have a valid part name, deleting: {:?}",
                            file
                        );
                        std::fs::remove_file(file)?;
                        continue;
                    };

                    // If the part is not in the state, then it can be deleted.
                    if !state.remaining_parts.iter().any(|p| p.part == part) {
                        std::fs::remove_file(file)?;
                    }

                    found_parts += 1;
                }

                // If less parts were found than expected, then we error
                if found_parts < state.remaining_parts.len() {
                    tracing::error!(
                        "Found {} parts in the data folder, but the state expects {}",
                        found_parts,
                        state.remaining_parts.len()
                    );
                    cleanup();
                    return Ok(None);
                }
            }
        };

        let mut file = Self {
            folder_path: path.to_path_buf(),
            header,
            state,
        };

        if should_trigger_splitting {
            file.trigger_file_split()?;
        }

        Ok(Some(file))
    }

    pub fn get_file_part_reader(&self, part: FilePartId) -> anyhow::Result<impl Read> {
        // If the part id is a header, then return the header.bin file.
        // If the part id is a body party, then:
        // - For "contiguous" files, seek to the correct offset in data.bin and return a substream.
        // - For "split" files, return the correct file in the data folder.

        match part {
            FilePartId::Header => {
                let file_path = self.folder_path.join("header.bin");
                let file = File::open(file_path)?;
                let file_len = file.metadata()?.len() as usize;
                // Use a substream to avoid returning 2 different types
                let substream = SubstreamReader::new(file, file_len);
                Ok(substream)
            }
            FilePartId::Part(part_id) => {
                if part_id >= self.header.part_count {
                    anyhow::bail!("Part index out of bounds");
                }

                if !self.state.remaining_parts.iter().any(|p| p.part == part) {
                    anyhow::bail!("Part has already been acknolwedged");
                }

                match self.state.mode {
                    ManagedFileStateMode::Contiguous => {
                        let file_path = self.folder_path.join("data.bin");
                        let mut file = File::open(file_path)?;

                        let part_size = self.header.file_part_size as usize;
                        let offset = part_id as usize * part_size;
                        file.seek(io::SeekFrom::Start(offset as u64))?;

                        let file = SubstreamReader::new(file, part_size);
                        Ok(file)
                    }
                    ManagedFileStateMode::Split => {
                        let file_path = self
                            .folder_path
                            .join(format!("data/{}.bin", part.as_string()));
                        let file = File::open(file_path)?;

                        // Use a substream to avoid returning 2 different types
                        let file = SubstreamReader::new(file, self.header.file_part_size as usize);
                        Ok(file)
                    }
                }
            }
        }
    }

    pub fn trigger_file_split(&mut self) -> anyhow::Result<()> {
        if self.state.mode == ManagedFileStateMode::Split {
            anyhow::bail!("File is already split");
        }

        // 1. Create the data folder
        std::fs::create_dir_all(self.folder_path.join("data"))?;

        // 2. Split the data.bin into the data folder based on remaining parts.
        for part in self.state.remaining_parts.iter() {
            let part_id = match part.part {
                FilePartId::Header => continue,
                FilePartId::Part(i) => i,
            };

            let mut part_file =
                File::create(self.folder_path.join(format!("data/{}.bin", part_id)))?;

            let mut reader = self.get_file_part_reader(part.part)?;
            io::copy(&mut reader, &mut part_file)?;
        }

        // 3. Modify the state to be "split"
        self.modify_state(|state| {
            state.mode = ManagedFileStateMode::Split;
        })?;

        // 4. Delete the data.bin file
        std::fs::remove_file(self.folder_path.join("data.bin"))?;

        Ok(())
    }

    fn decrease_part_priority(&mut self, part: FilePartId) -> anyhow::Result<()> {
        self.modify_state(|state| {
            let part = match state.remaining_parts.iter_mut().find(|p| p.part == part) {
                Some(part) => part,
                None => {
                    tracing::warn!(
                        "Tried to decrease priority of part {} but it wasn't found",
                        part
                    );
                    return;
                }
            };

            part.priority -= 1;
        })?;

        Ok(())
    }

    fn modify_state(&mut self, modify: impl FnOnce(&mut ManagedFileState)) -> anyhow::Result<()> {
        modify(&mut self.state);

        write_file_atomic(self.folder_path.join("state.bin"), |file| {
            Ok(bincode::serialize_into(file, &self.state)?)
        })?;
        write_file_atomic(self.folder_path.join("state.json"), |file| {
            Ok(serde_json::to_writer_pretty(file, &self.state)?)
        })?;

        Ok(())
    }
}

fn write_file_atomic(
    path: impl AsRef<Path>,
    write: impl FnOnce(&mut File) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("-tmp");

    let mut file = std::fs::File::create(&tmp_path)?;
    write(&mut file)?;
    file.sync_all()?;
    drop(file);

    std::fs::rename(&tmp_path, path)?;

    Ok(())
}

impl ManagedFileState {
    pub fn new_from_header(header: &HeaderChunk) -> Self {
        let mode = ManagedFileStateMode::Contiguous;
        let part_count = header.part_count;

        let remaining_parts = (0..part_count)
            .map(|i| ManagedFileStatePart {
                part: FilePartId::Part(i),
                priority: 0,
            })
            .collect();

        Self {
            mode,
            remaining_parts,
        }
    }
}
