//! A virtualization layer for the filesystem, to allow for unit testing and mocking.
//!
//! Also, a collection of filesystem helper functions, for easier atomic file writes, etc.

mod mock;
pub use mock::*;

use std::path::Path;

/// Represents a virtual filesystem directory. This could be a real directory, or a mock directory.
pub trait FilesystemDir: Sized {
    type File<'a>: FilesystemFile<'a>
    where
        Self: 'a;

    fn create_child_dir(&self, name: impl AsRef<Path>) -> std::io::Result<Self>;
    fn create_child_file(&self, name: impl AsRef<Path>) -> std::io::Result<Self::File<'_>>;

    fn child_dir_exists(&self, name: impl AsRef<Path>) -> bool;
    fn child_file_exists(&self, name: impl AsRef<Path>) -> bool;

    fn delete_file(&self, name: impl AsRef<Path>) -> std::io::Result<()>;
    fn delete_self(self) -> std::io::Result<()>;
}

/// Represents an open virtual filesystem file. This could be a real file, or a mock file.
///
/// On the mock filesystem, it locks the filesystem when in use, so avoid holding it for too long.
pub trait FilesystemFile<'a>: Sized + std::io::Write + std::io::Read + std::io::Seek {
    fn delete(self) -> std::io::Result<()>;
}

/// A virtual filesystem, which can be used to create mock filesystems.
pub trait Filesystem: Sized {
    type Dir: FilesystemDir;
    type File<'a>: FilesystemFile<'a>;

    fn root(&self) -> Self::Dir;
}
