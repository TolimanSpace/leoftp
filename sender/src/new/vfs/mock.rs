use std::{
    collections::BTreeMap,
    io::Cursor,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use super::Filesystem;

#[derive(Debug, Clone)]
enum PathType {
    File(Vec<u8>),
    OpenFile,
    Directory,
}

impl PathType {
    fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }

    fn is_open_file(&self) -> bool {
        matches!(self, Self::OpenFile)
    }

    fn is_directory(&self) -> bool {
        matches!(self, Self::Directory)
    }
}

pub struct MockFilesystem {
    filesystem: Arc<Mutex<FilesystemInner>>,
}

struct FilesystemInner {
    data: BTreeMap<PathBuf, PathType>,
}

impl MockFilesystem {
    pub fn new() -> Self {
        Self {
            filesystem: Arc::new(Mutex::new(FilesystemInner::new())),
        }
    }
}

impl FilesystemInner {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn all_paths_with_prefix(&self, prefix: impl AsRef<std::path::Path>) -> Vec<PathBuf> {
        self.data
            .keys()
            .filter(|path| path.starts_with(prefix.as_ref()))
            .cloned()
            .collect()
    }

    fn create_dir(&mut self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let mut path = path.as_ref();

        loop {
            let existing = self.data.get(path);
            if let Some(existing) = existing {
                if !existing.is_directory() {
                    panic!("Cannot create directory, file already exists");
                }
            }

            self.data.insert(path.to_owned(), PathType::Directory);

            if let Some(parent) = path.parent() {
                path = parent;
            } else {
                break;
            }
        }

        Ok(())
    }

    fn delete_dir(&mut self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let path = path.as_ref();

        let paths = self.all_paths_with_prefix(path);

        for path in paths {
            // Check that it's not an open file
            if let Some(PathType::OpenFile) = self.data.get(&path) {
                panic!("Cannot delete an open file");
            }

            self.data.remove(&path);
        }

        Ok(())
    }

    fn move_path(
        &mut self,
        from: impl AsRef<std::path::Path>,
        to: impl AsRef<std::path::Path>,
    ) -> std::io::Result<()> {
        let from = from.as_ref();
        let to = to.as_ref();

        let paths = self.all_paths_with_prefix(from);

        for path in paths {
            let new_path = to.join(path.strip_prefix(from).unwrap());

            // Check that it's not an open file
            if let Some(PathType::OpenFile) = self.data.get(&path) {
                panic!("Cannot move an open file");
            }

            self.data.insert(new_path, self.data.remove(&path).unwrap());
        }

        Ok(())
    }
}

impl Filesystem for MockFilesystem {
    type File = MockFilesystemFile;

    fn create_file(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let mut filesystem = self.filesystem.lock().unwrap();

        let path = path.as_ref().to_owned();
        if let Some(dir) = path.parent() {
            filesystem.create_dir(dir)?;
        }

        let existing_entry = filesystem.data.get(&path);
        if let Some(existing_entry) = existing_entry {
            panic!("Cannot create file, file/directory already exists");
        }

        let data = filesystem.data.insert(path, PathType::File(Vec::new()));
        Ok(())
    }

    fn open_file(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<Self::File> {
        let mut filesystem = self.filesystem.lock().unwrap();

        let path = path.as_ref().to_owned();
        let existing_entry = filesystem.data.get(&path);
        if let Some(existing_entry) = existing_entry {
            if existing_entry.is_open_file() {
                panic!("Cannot open file, file is already open");
            }
        }

        let data = filesystem.data.insert(path, PathType::OpenFile);
        Ok(MockFilesystemFile {
            path,
            filesystem: self.filesystem.clone(),
            cursor: Cursor::new(Vec::new()),
        })
    }

    fn file_exists(&self, path: impl AsRef<std::path::Path>) -> bool {
        todo!()
    }

    fn delete_file(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        todo!()
    }

    fn create_dir(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        todo!()
    }

    fn delete_dir(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        todo!()
    }

    fn dir_exists(&self, path: impl AsRef<std::path::Path>) -> bool {
        todo!()
    }

    fn move_path(
        &self,
        from: impl AsRef<std::path::Path>,
        to: impl AsRef<std::path::Path>,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn move_file_overwriting(
        &self,
        from: impl AsRef<std::path::Path>,
        to: impl AsRef<std::path::Path>,
    ) -> std::io::Result<()> {
        todo!()
    }
}

struct MockFilesystemFile {
    path: PathBuf,
    filesystem: Arc<Mutex<BTreeMap<PathBuf, Option<Vec<u8>>>>>,
    cursor: Cursor<Vec<u8>>,
}
