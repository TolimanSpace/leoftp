use std::{io, path::PathBuf};

use crossbeam_channel::Sender;

pub struct InputFolderThreads {
    // TODO: Store join handles
}

impl InputFolderThreads {
    pub fn spawn(
        input_folder: PathBuf,
        pending_folder: PathBuf,
        new_file_snd: Sender<PathBuf>,
    ) -> io::Result<Self> {
        spawn_file_poller(input_folder.clone(), pending_folder, new_file_snd)?;

        let folder = InputFolderThreads {};

        Ok(folder)
    }
}

pub fn spawn_file_poller(
    input_folder: PathBuf,
    pending_folder: PathBuf,
    new_file_snd: Sender<PathBuf>,
) -> io::Result<()> {
    // Add all the files that are already in the pending folder to the queue
    for entry in std::fs::read_dir(&pending_folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            new_file_snd.send(path).unwrap();
        }
    }

    // Spawn a thread that polls the new folder, and moves each file there into the pending folder and
    // notifies the pending queue
    std::thread::spawn(move || loop {
        // Find all the new foles in the new folder
        let mut new_files = Vec::new();
        for entry in std::fs::read_dir(&input_folder).unwrap() {
            // Ensure that the file no longer has open file handles

            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                new_files.push(path);
            }
        }

        for path in new_files {
            let pending_path = pending_folder.join(path.file_name().unwrap());

            let rename_result = std::fs::rename(&path, &pending_path);
            if rename_result.is_err() {
                tracing::error!("Failed to move file to pending folder: {:?}", rename_result);
                continue;
            }

            let snd_result = new_file_snd.send(pending_path);
            if snd_result.is_err() {
                return; // The queue has been removed
            }
        }

        // Sleep to not spam the disk too much
        std::thread::sleep(std::time::Duration::from_millis(100));
    });

    Ok(())
}
