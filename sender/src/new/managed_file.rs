use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use common::{
    binary_serialize::BinarySerialize,
    chunks::{Chunk, DataChunk, HeaderChunk},
    file_part_id::{FilePartId, FilePartIdRangeInclusive},
    substream::SubstreamReader,
};

use self::{
    mode::ManagedFileMode,
    state::{ManagedFileState, ManagedFileStatePart},
};

mod mode;
mod state;

/// Managed file structure:
///
/// ```plaintext
/// [folder root]/  - The root folder, usually the file id
/// ├── header.json - The human-readable header file. This is never read.
/// ├
/// ├── header.bin  - The machine-readable header file.
/// ├── mode.bin    - The mode of the file, representing either "contiguous" or "split".
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
/// 2. Create the header.json files
/// 3. Create the the header.bin, mode.bin the state.bin file
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
/// If the data.bin file can be shrunk down from the end, then it is shrunk accordingly. If zero parts
/// are left, then the managed file can safely be deleted.
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
#[derive(Debug, PartialEq, Eq)]
pub struct ManagedFile {
    folder_path: PathBuf,
    header: HeaderChunk,
    mode: ManagedFileMode,
    state: ManagedFileState,
}

impl ManagedFile {
    /// Please reference the doc comment on [`ManagedFile`] for more information.
    pub fn create_new_from_header(
        path: impl AsRef<Path>,
        data_file_path: impl AsRef<Path>,
        header: HeaderChunk,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let data_file_path = data_file_path.as_ref();
        let mode = ManagedFileMode::Contiguous;

        // 1. Create the folder
        std::fs::create_dir_all(path)?;

        // 2. Create the header.json file
        write_file_atomic(path.join("header.json"), |file| {
            Ok(serde_json::to_writer_pretty(file, &header)?)
        })?;

        // 3. Create the the header.bin, mode.bin the state.bin file
        write_file_atomic(path.join("header.bin"), |file| {
            Ok(header.serialize_to_stream(file)?)
        })?;
        write_file_atomic(path.join("mode.bin"), |file| {
            Ok(mode.serialize_to_stream(file)?)
        })?;

        let state_path = path.join("state.bin");
        let state = ManagedFileState::new_from_part_count(header.part_count, &state_path)?;

        // 4. Move in the data.bin file
        std::fs::rename(data_file_path, path.join("data.bin"))?;

        Ok(Self {
            folder_path: path.to_path_buf(),
            header,
            mode,
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

        let state: ManagedFileState = ManagedFileState::load_from_file(state_path)?;

        // Read mode.bin, if it's missing then it's invalid
        let mode_path = path.join("mode.bin");
        if !mode_path.is_file() {
            tracing::warn!(
                "Managed file folder was missing mode.bin, deleting: {:?}",
                path
            );
            cleanup();
            return Ok(None);
        }

        let mut mode_file = File::open(mode_path)?;
        let mode: ManagedFileMode = BinarySerialize::deserialize_from_stream(&mut mode_file)?;

        let mut should_trigger_splitting = false;

        match mode {
            ManagedFileMode::Contiguous => {
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
            ManagedFileMode::Split => {
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
                    if !state.remaining_parts().iter().any(|p| p.part == part) {
                        std::fs::remove_file(file)?;
                    }

                    found_parts += 1;
                }

                // If less parts were found than expected, then we error
                if found_parts < state.remaining_non_header_parts_len() {
                    tracing::error!(
                        "Found {} parts in the data folder, but the state expects {}",
                        found_parts,
                        state.remaining_parts().len()
                    );
                    cleanup();
                    return Ok(None);
                }
            }
        };

        let mut file = Self {
            folder_path: path.to_path_buf(),
            header,
            mode,
            state,
        };

        if should_trigger_splitting {
            file.trigger_file_split()?;
        }

        Ok(Some(file))
    }

    pub fn get_file_part(&self, part: FilePartId) -> anyhow::Result<Option<Chunk>> {
        // If the part id is a header, then return the header.bin file.
        // If the part id is a body party, then:
        // - For "contiguous" files, seek to the correct offset in data.bin and return a substream.
        // - For "split" files, return the correct file in the data folder.

        match part {
            FilePartId::Header => {
                let file_path = self.folder_path.join("header.bin");
                let file = File::open(file_path)?;
                let _file_len = file.metadata()?.len() as usize;
                Ok(Some(Chunk::Header(self.header.clone())))
            }
            FilePartId::Part(part_id) => {
                if part_id >= self.header.part_count {
                    anyhow::bail!("Part index out of bounds");
                }

                if !self.state.remaining_parts().iter().any(|p| p.part == part) {
                    return Ok(None);
                }

                match self.mode {
                    ManagedFileMode::Contiguous => {
                        let file_path = self.folder_path.join("data.bin");
                        let mut file = File::open(file_path)?;

                        let part_size = self.header.file_part_size as usize;
                        let offset = part_id as usize * part_size;
                        file.seek(io::SeekFrom::Start(offset as u64))?;

                        let mut file = SubstreamReader::new(file, part_size);
                        let mut data = Vec::new();
                        std::io::copy(&mut file, &mut data)?;

                        let data_chunk = DataChunk {
                            data,
                            file_id: self.header.id,
                            part: part_id,
                        };

                        Ok(Some(Chunk::Data(data_chunk)))
                    }
                    ManagedFileMode::Split => {
                        let file_path = self
                            .folder_path
                            .join(format!("data/{}.bin", part.as_string()));
                        let mut file = File::open(file_path)?;

                        let mut data = Vec::new();
                        std::io::copy(&mut file, &mut data)?;

                        let data_chunk = DataChunk {
                            data,
                            file_id: self.header.id,
                            part: part_id,
                        };

                        Ok(Some(Chunk::Data(data_chunk)))
                    }
                }
            }
        }
    }

    pub fn trigger_file_split(&mut self) -> anyhow::Result<()> {
        if self.mode == ManagedFileMode::Split {
            anyhow::bail!("File is already split");
        }

        // 1. Create the data folder
        std::fs::create_dir_all(self.folder_path.join("data"))?;

        // Prepare data (open the file, get all the non-header remaining parts sorted in reverse)
        let mut file = File::open(self.folder_path.join("data.bin"))?;
        let remaining_parts = &self.state.remaining_parts();
        let mut remaining_part_numbers = remaining_parts
            .iter()
            .filter_map(|p| match p.part {
                FilePartId::Header => None,
                FilePartId::Part(i) => Some(i),
            })
            .collect::<Vec<_>>();
        // Sort reverse. We start from the last part and work our way backwards.
        remaining_part_numbers.sort_by(|a, b| b.cmp(a));

        // 2. Split the data.bin into the data folder based on remaining parts, starting
        // from the last part and working our way backwards, reducing the file size as we go.
        for part_id in remaining_part_numbers {
            let part_size = self.header.file_part_size as usize;
            let offset = part_id as usize * part_size;
            let max = offset + part_size;

            if max > file.metadata()?.len() as usize {
                anyhow::bail!("File size is smaller than expected");
            }

            file.seek(io::SeekFrom::Start(offset as u64))?;
            let mut reader = SubstreamReader::new(&mut file, part_size);

            let path = self.folder_path.join(format!("data/{}.bin", part_id));
            write_file_atomic(path, move |file| {
                io::copy(&mut reader, file)?;
                Ok(())
            })?;
        }

        // 3. Modify the state to be "split"
        self.set_mode(ManagedFileMode::Split)?;

        // 4. Delete the data.bin file
        std::fs::remove_file(self.folder_path.join("data.bin"))?;

        Ok(())
    }

    pub fn decrease_part_priority(&mut self, part: FilePartId) -> anyhow::Result<()> {
        self.state.modify_part_priority(part, |p| {
            *p -= 1;
        })?;

        Ok(())
    }

    pub fn acknowledge_file_parts(
        &mut self,
        part_range: FilePartIdRangeInclusive,
    ) -> anyhow::Result<()> {
        // First, update the state
        self.state
            .filter_remaining_parts(|p| !part_range.contains(p.part))?;

        // Then, delete the data accordingly
        match self.mode {
            ManagedFileMode::Contiguous => {
                // Find the last part number that's unacknowledged
                let mut last_unacknowledged_part = None;
                for part in self.state.remaining_parts().iter() {
                    match part.part {
                        FilePartId::Header => {}
                        FilePartId::Part(i) => {
                            if let Some(last) = last_unacknowledged_part {
                                if i > last {
                                    last_unacknowledged_part = Some(i);
                                }
                            } else {
                                last_unacknowledged_part = Some(i);
                            }
                        }
                    }
                }

                // Open the file
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(self.folder_path.join("data.bin"))?;

                // Determine the resulting file size
                let result_len = if let Some(last) = last_unacknowledged_part {
                    let part_size = self.header.file_part_size as u64;
                    (last + 1) as u64 * part_size
                } else {
                    0
                };

                // Shrink the file to the correct size
                let file_current_len = file.metadata()?.len();
                if file_current_len > result_len {
                    file.set_len(result_len)?;
                }
            }
            ManagedFileMode::Split => {
                for part in part_range.iter_parts() {
                    // Get the file part number if it's a file part. If it's a header, do nothing.
                    let FilePartId::Part(i) = part else {
                        continue;
                    };

                    // Find the .bin file for the acknowledged part
                    let file_path = self.folder_path.join(format!("data/{}.bin", i));

                    // If the file doesn't exist already, do nothing.
                    if !file_path.is_file() {
                        continue;
                    }

                    // Delete the file
                    let result = std::fs::remove_file(file_path);
                    if let Err(e) = result {
                        tracing::error!(
                            "Failed to delete file part {} in managed file folder: {}",
                            i,
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.state.remaining_parts().is_empty()
    }

    pub fn delete(self) -> anyhow::Result<()> {
        std::fs::remove_dir_all(&self.folder_path)?;
        Ok(())
    }

    fn set_mode(&mut self, new_mode: ManagedFileMode) -> anyhow::Result<()> {
        self.mode = new_mode;

        write_file_atomic(self.folder_path.join("mode.bin"), |file| {
            Ok(self.mode.serialize_to_stream(file)?)
        })?;

        Ok(())
    }

    pub fn remaining_parts(&self) -> &[ManagedFileStatePart] {
        self.state.remaining_parts()
    }

    pub fn header(&self) -> &HeaderChunk {
        &self.header
    }
}

fn write_file_atomic(
    path: impl AsRef<Path>,
    write: impl FnOnce(&mut BufWriter<&mut File>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("-tmp");

    let mut file = std::fs::File::create(&tmp_path)?;
    let mut writer = BufWriter::new(&mut file);
    write(&mut writer)?;
    writer.flush()?;
    drop(writer);

    file.sync_all()?;
    drop(file);

    std::fs::rename(&tmp_path, path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{state::ManagedFileState, *};
    use crate::new::tempdir::{TempDir, TempDirProvider};

    struct DummyFile {
        _folder: TempDir,
        path: PathBuf,
    }

    fn make_dummy_file(size: u64) -> anyhow::Result<DummyFile> {
        let folder = TempDirProvider::new_test().create()?;
        let path = folder.path().join("data.bin");

        let file = File::create(&path)?;
        file.set_len(size)?;

        Ok(DummyFile {
            _folder: folder,
            path,
        })
    }

    fn make_test_header(file_size: u64, part_size: u64) -> HeaderChunk {
        HeaderChunk {
            id: uuid::Uuid::new_v4(),
            date: 123456789,
            name: "test".to_string(),
            file_part_size: part_size as u32,
            size: file_size,
            part_count: if file_size % part_size == 0 {
                (file_size / part_size) as u32
            } else {
                (file_size / part_size + 1) as u32
            },
        }
    }

    fn make_test_managed_file(
        path: impl AsRef<Path>,
        file_size: u64,
        part_size: u64,
    ) -> anyhow::Result<ManagedFile> {
        let header = make_test_header(file_size, part_size);
        let file = make_dummy_file(file_size)?;
        ManagedFile::create_new_from_header(path, file.path, header)
    }

    fn assert_file_exists(path: impl AsRef<Path>) {
        assert!(path.as_ref().exists(), "{:?} doesn't exist", path.as_ref());
    }

    fn assert_file_exists_with_size(path: impl AsRef<Path>, size: u64) {
        assert_file_exists(&path);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
    }

    fn assert_file_doesnt_exist(path: impl AsRef<Path>) {
        assert!(!path.as_ref().exists(), "{:?} exists", path.as_ref());
    }

    fn read_state(folder_path: impl AsRef<Path>) -> ManagedFileState {
        let state_path = folder_path.as_ref().join("state.bin");
        ManagedFileState::load_from_file(state_path).unwrap()
    }

    fn read_mode(folder_path: impl AsRef<Path>) -> ManagedFileMode {
        let mode_path = folder_path.as_ref().join("mode.bin");
        let mut file = File::open(mode_path).unwrap();
        BinarySerialize::deserialize_from_stream(&mut file).unwrap()
    }

    fn assert_equal_after_parsing(path: impl AsRef<Path>, file: &ManagedFile) {
        let new_file = ManagedFile::try_read_from_path(path).unwrap().unwrap();
        assert_eq!(&new_file, file);
    }

    #[test]
    fn test_file_data() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_test().create()?;
        let mut file = make_test_managed_file(folder.path(), 100, 10)?;

        assert_file_exists(folder.path().join("header.bin"));
        assert_file_exists(folder.path().join("state.bin"));
        assert_file_exists_with_size(folder.path().join("data.bin"), 100);
        dbg!(&file.folder_path);

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 11); // 10 + header
        let mode = read_mode(folder.path());
        assert_eq!(mode, ManagedFileMode::Contiguous);

        file.trigger_file_split()?;

        assert_file_exists(folder.path().join("header.bin"));
        assert_file_exists(folder.path().join("state.bin"));
        for i in 0..10 {
            assert_file_exists_with_size(folder.path().join(format!("data/{i}.bin")), 10);
        }
        assert_file_doesnt_exist(folder.path().join("data/10.bin"));
        assert_file_doesnt_exist(folder.path().join("data.bin"));

        assert_equal_after_parsing(folder.path(), &file);

        Ok(())
    }

    #[test]
    fn test_acknowledging_contiguous() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_test().create()?;
        let mut file = make_test_managed_file(folder.path(), 100, 10)?;
        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 11);

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(9)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 10);
        assert_file_exists_with_size(folder.path().join("data.bin"), 90);

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(7)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 9);
        assert_file_exists_with_size(folder.path().join("data.bin"), 90); // Still 90, because we haven't acknowledged part 8

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(8)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 8);
        assert_file_exists_with_size(folder.path().join("data.bin"), 70);

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(0)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 7);
        assert_file_exists_with_size(folder.path().join("data.bin"), 70); // Again, there's parts in front

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Header))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 6);
        assert_file_exists_with_size(folder.path().join("data.bin"), 70); // Header shouldn't affect this

        // Expect header to still exist
        assert_file_exists(folder.path().join("header.bin"));

        // Check the remaining parts are still there
        let parts = [
            FilePartId::Part(1),
            FilePartId::Part(2),
            FilePartId::Part(3),
            FilePartId::Part(4),
            FilePartId::Part(5),
            FilePartId::Part(6),
        ];

        let state = read_state(folder.path());
        for part in parts {
            let exists = state.remaining_parts().iter().any(|p| p.part == part);
            if !exists {
                dbg!(state.remaining_parts());
                panic!("Part {:?} was not found in the remaining parts", part);
            }
        }

        assert_equal_after_parsing(folder.path(), &file);

        Ok(())
    }

    #[test]
    fn test_acknowledging_split() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_test().create()?;
        let mut file = make_test_managed_file(folder.path(), 100, 10)?;

        file.trigger_file_split()?;
        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 11);

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(9)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 10);
        assert_file_doesnt_exist(folder.path().join("data/9.bin"));

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(7)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 9);
        assert_file_doesnt_exist(folder.path().join("data/7.bin"));

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(8)))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 8);
        assert_file_doesnt_exist(folder.path().join("data/8.bin"));

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new(
            FilePartId::Part(0),
            FilePartId::Part(2),
        ))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 5);
        assert_file_doesnt_exist(folder.path().join("data/0.bin"));

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Header))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 4);

        // Expect header to still exist
        assert_file_exists(folder.path().join("header.bin"));

        // Check the remaining parts are still there
        let parts = [
            FilePartId::Part(3),
            FilePartId::Part(4),
            FilePartId::Part(5),
            FilePartId::Part(6),
        ];

        let state = read_state(folder.path());

        for part_id in 0..10 {
            let part = FilePartId::Part(part_id);

            if !parts.contains(&part) {
                assert_file_doesnt_exist(folder.path().join(format!("data/{part_id}.bin")));

                assert!(
                    !state.remaining_parts().iter().any(|p| p.part == part),
                    "Part {:?} was found in the remaining parts",
                    part
                );
            } else {
                assert_file_exists_with_size(folder.path().join(format!("data/{part_id}.bin")), 10);

                assert!(
                    state.remaining_parts().iter().any(|p| p.part == part),
                    "Part {:?} was not found in the remaining parts",
                    part
                );
            }
        }

        assert_equal_after_parsing(folder.path(), &file);

        Ok(())
    }

    #[test]
    fn test_splitting_after_acknowleding() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_test().create()?;
        let mut file = make_test_managed_file(folder.path(), 100, 10)?;

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(9)))?;
        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(7)))?;
        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(8)))?;
        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Part(0)))?;
        file.acknowledge_file_parts(FilePartIdRangeInclusive::new_single(FilePartId::Header))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 6);

        file.trigger_file_split()?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 6);

        assert_file_exists(folder.path().join("header.bin"));
        assert_file_exists(folder.path().join("state.bin"));
        for i in 1..6 {
            assert_file_exists_with_size(folder.path().join(format!("data/{i}.bin")), 10);
        }
        for i in [0, 7, 8, 9].iter() {
            assert_file_doesnt_exist(folder.path().join(format!("data/{i}.bin")));
        }
        assert_file_doesnt_exist(folder.path().join("data.bin"));

        assert_equal_after_parsing(folder.path(), &file);

        Ok(())
    }

    #[test]
    fn test_continuous_all_acknowledged() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_test().create()?;
        let mut file = make_test_managed_file(folder.path(), 100, 10)?;

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new(
            FilePartId::Header,
            FilePartId::Part(9),
        ))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 0);

        assert_file_exists(folder.path().join("header.bin"));
        assert_file_exists(folder.path().join("state.bin"));
        assert_file_exists_with_size(folder.path().join("data.bin"), 0);

        assert_equal_after_parsing(folder.path(), &file);

        assert!(file.is_finished());
        file.delete()?;

        assert!(!folder.path().exists());

        Ok(())
    }

    #[test]
    fn test_split_all_acknowledged() -> anyhow::Result<()> {
        let folder = TempDirProvider::new_test().create()?;
        let mut file = make_test_managed_file(folder.path(), 100, 10)?;
        file.trigger_file_split()?;

        file.acknowledge_file_parts(FilePartIdRangeInclusive::new(
            FilePartId::Header,
            FilePartId::Part(9),
        ))?;

        let state = read_state(folder.path());
        assert_eq!(state.remaining_parts().len(), 0);

        dbg!(std::fs::read_dir(folder.path().join("data"))?.collect::<Vec<_>>());

        assert_file_exists(folder.path().join("header.bin"));
        assert_file_exists(folder.path().join("state.bin"));
        // Assert the data folder is empty
        assert!(std::fs::read_dir(folder.path().join("data"))?
            .next()
            .is_none());

        assert_equal_after_parsing(folder.path(), &file);

        assert!(file.is_finished());
        file.delete()?;

        assert!(!folder.path().exists());

        Ok(())
    }
}
