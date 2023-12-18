//! A virtualization layer for the filesystem, to allow for unit testing and mocking.
//!
//! Also, a collection of filesystem helper functions, for easier atomic file writes, etc.

mod mock;
pub use mock::*;

use std::path::Path;

/// Represents an open virtual filesystem file. This could be a real file, or a mock file.
///
/// On the mock filesystem, it locks the filesystem when in use, so avoid holding it for too long.
pub trait FilesystemFile: Sized + std::io::Write + std::io::Read + std::io::Seek {}

/// A virtual filesystem, which can be used to create mock filesystems.
pub trait Filesystem: Sized {
    type File: FilesystemFile;

    /// Create an empty file, or overwrite an existing file. Directories are created automatically.
    fn create_file(&self, path: impl AsRef<Path>) -> std::io::Result<()>;
    /// Open an existing file for reading and writing.
    fn open_file(&self, path: impl AsRef<Path>) -> std::io::Result<Self::File>;
    /// Check if a file exists.
    fn file_exists(&self, path: impl AsRef<Path>) -> bool;
    /// Delete a file.
    fn delete_file(&self, path: impl AsRef<Path>) -> std::io::Result<()>;

    /// Open a file if it exists, otherwise create it.
    fn open_or_create_file(&self, path: impl AsRef<Path>) -> std::io::Result<Self::File> {
        if !self.file_exists(&path) {
            self.create_file(&path)?;
        }
        self.open_file(&path)
    }

    /// Create a directory. Parent directories are created automatically.
    fn create_dir(&self, path: impl AsRef<Path>) -> std::io::Result<()>;
    /// Delete a directory and all of its children. Can't delete open files.
    fn delete_dir(&self, path: impl AsRef<Path>) -> std::io::Result<()>;
    /// Check if a directory exists.
    fn dir_exists(&self, path: impl AsRef<Path>) -> bool;

    /// Move a file or directory.
    fn move_path(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> std::io::Result<()>;

    /// Move a single file, overwriting the destination if it exists.
    fn move_file_overwriting(
        &self,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> std::io::Result<()>;
}
