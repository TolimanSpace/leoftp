use std::{path::PathBuf, thread, time::Duration};

mod byte_pipe;
mod runner;

pub fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .compact()
        .init();

    let base_folder = PathBuf::from("/home/arduano/programming/spiralblue/file-downlink/testdata");

    let _runner = runner::TestRunner::new(&base_folder);

    // Sleep forever
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
