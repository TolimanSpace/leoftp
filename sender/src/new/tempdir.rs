use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

pub struct TempDirProvider {
    base: PathBuf,
}

impl TempDirProvider {
    pub fn new(base: PathBuf) -> Self {
        Self { base }
    }

    pub fn create(&self) -> io::Result<TempDir> {
        let id = uuid::Uuid::new_v4();
        let path = self.base.join(id.to_string());

        TempDir::new(path)
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("_tempfs");

        Self::new(base)
    }
}

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(path: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn make_file(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let path = self.path.join(path);
        std::fs::create_dir_all(path.parent().unwrap())?;
        let file = File::create(path)?;

        Ok(file)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).ok();
    }
}
