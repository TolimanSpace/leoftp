use std::{
    collections::BTreeMap,
    io::Cursor,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use super::Filesystem;

pub struct MockFilesystem {
    filesystem: Arc<Mutex<BTreeMap<PathBuf, Option<Vec<u8>>>>>,
}

impl MockFilesystem {
    pub fn new() -> Self {
        Self {
            filesystem: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Filesystem for MockFilesystem {
    type Dir = MockDir;
    type File<'a> = MockFile<'a>;

    fn root(&self) -> Self::Dir {
        MockDir {
            filesystem: self.filesystem.clone(),
            path: PathBuf::new(),
        }
    }
}

pub struct MockDir {
    filesystem: Arc<Mutex<BTreeMap<PathBuf, Option<Vec<u8>>>>>,
    path: PathBuf,
}

impl super::FilesystemDir for MockDir {
    type File<'a> = MockFile<'a>;

    fn create_child_dir(&self, name: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let mut filesystem = self.filesystem.lock().unwrap();
        let path = self.path.join(name);
        if filesystem.contains_key(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "File already exists",
            ));
        }
        filesystem.insert(path.clone(), None);
        Ok(Self {
            filesystem: self.filesystem.clone(),
            path,
        })
    }

    fn create_child_file(
        &self,
        name: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Self::File<'_>> {
        let mut filesystem = self.filesystem.lock().unwrap();
        let path = self.path.join(name);
        if filesystem.contains_key(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "File already exists",
            ));
        }
        filesystem.insert(path.clone(), Some(Vec::new()));

        Self::File::new(filesystem, path)
    }

    fn delete_file(&self, name: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        if !self.child_file_exists(name.as_ref()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ));
        }

        let mut filesystem = self.filesystem.lock().unwrap();
        let path = self.path.join(name.as_ref());
        if !filesystem.contains_key(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ));
        }
        filesystem.remove(&path);
        Ok(())
    }

    fn delete_self(self) -> std::io::Result<()> {
        let mut filesystem = self.filesystem.lock().unwrap();

        // Delete each child
        for path in filesystem
            .keys()
            .filter(|path| path.starts_with(&self.path))
            .cloned()
            .collect::<Vec<_>>()
        {
            filesystem.remove(&path);
        }

        Ok(())
    }

    fn child_dir_exists(&self, name: impl AsRef<std::path::Path>) -> bool {
        self.filesystem
            .lock()
            .unwrap()
            .get(&self.path.join(name))
            .map(|v| v.is_none())
            .unwrap_or(false)
    }

    fn child_file_exists(&self, name: impl AsRef<std::path::Path>) -> bool {
        self.filesystem
            .lock()
            .unwrap()
            .get(&self.path.join(name))
            .map(|v| v.is_some())
            .unwrap_or(false)
    }
}

/// **IMPORTANT**: On the mock filesystem, this locks the filesystem when in use, so it can cause deadlocks
/// if not dropped before making further changes to the filesystem.
pub struct MockFile<'a> {
    file: MutexGuard<'a, BTreeMap<PathBuf, Option<Vec<u8>>>>,
    path: PathBuf,
    pos: u64,
}

impl<'a> MockFile<'a> {
    pub fn new(
        file: MutexGuard<'a, BTreeMap<PathBuf, Option<Vec<u8>>>>,
        path: PathBuf,
    ) -> std::io::Result<Self> {
        if !file.contains_key(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ));
        }
        Ok(Self { file, path, pos: 0 })
    }

    fn get_data(&mut self) -> std::io::Result<&mut Vec<u8>> {
        self.file
            .get_mut(&self.path)
            .and_then(|v| v.as_mut())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"))
    }

    fn get_cursor(&mut self) -> std::io::Result<Cursor<&mut Vec<u8>>> {
        let pos = self.pos;
        let data = self.get_data()?;
        let mut cursor = Cursor::new(data);
        cursor.set_position(pos);
        Ok(cursor)
    }
}

impl std::io::Write for MockFile<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut cursor = self.get_cursor()?;
        let written = cursor.write(buf)?;
        self.pos = cursor.position();
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // No-op
        Ok(())
    }
}

impl std::io::Read for MockFile<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut cursor = self.get_cursor()?;
        let read = cursor.read(buf)?;
        self.pos = cursor.position();
        Ok(read)
    }
}

impl std::io::Seek for MockFile<'_> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let mut cursor = self.get_cursor()?;
        let new_pos = cursor.seek(pos)?;
        self.pos = cursor.position();
        Ok(new_pos)
    }
}

impl super::FilesystemFile<'_> for MockFile<'_> {
    fn delete(self) -> std::io::Result<()> {
        let mut file = self.file;
        if !file.contains_key(&self.path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ));
        }
        file.remove(&self.path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::new::vfs::*;

    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    // Mock implementation for Filesystem, FilesystemDir, and FilesystemFile
    // Add your mock implementation here...

    #[test]
    fn test_create_directory() {
        let fs = MockFilesystem::new(); // Replace with your mock filesystem implementation
        let root = fs.root();
        let dir_name = "test_dir";
        root.create_child_dir(dir_name).unwrap();
        assert!(root.child_dir_exists(dir_name));
    }

    #[test]
    fn test_create_file() {
        let fs = MockFilesystem::new();
        let root = fs.root();
        let file_name = "test_file.txt";
        root.create_child_file(file_name).unwrap();
        assert!(root.child_file_exists(file_name));
    }

    #[test]
    fn test_write_and_read_file() {
        let fs = MockFilesystem::new();
        let root = fs.root();
        let file_name = "test_file.txt";

        let written_content = b"Hello, world!";
        let mut contents = Vec::new();
        {
            let mut file = root.create_child_file(file_name).unwrap();

            // Write some data to the file
            file.write_all(written_content).unwrap();
            file.flush().unwrap();

            // Read data from the file
            file.seek(SeekFrom::Start(0)).unwrap();
            file.read_to_end(&mut contents).unwrap();
        }

        assert_eq!(contents, written_content);
        assert!(root.child_file_exists(file_name));
    }

    #[test]
    fn test_delete_file() {
        let fs = MockFilesystem::new();
        let root = fs.root();
        let file_name = "test_file.txt";
        root.create_child_file(file_name).unwrap();
        root.delete_file(file_name).unwrap();
        assert!(!root.child_file_exists(file_name));
    }

    #[test]
    fn test_delete_directory() {
        let fs = MockFilesystem::new();
        let root = fs.root();
        let dir_name = "test_dir";
        let child = root.create_child_dir(dir_name).unwrap();
        child.delete_self().unwrap();
        assert!(!root.child_dir_exists(dir_name));
    }
}
