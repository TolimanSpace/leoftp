use std::{path::PathBuf, sync::Arc, thread, time::Duration};

use common::control::ControlMessage;

pub fn main() {
    let base_folder = PathBuf::from("./testdata");

    let snd_folder = base_folder.join("snd");
    let rcv_folder = base_folder.join("rcv");

    let snd_input_folder = snd_folder.join("input");
    let snd_workdir_folder = snd_folder.join("workdir");

    let rcv_pending_folder = rcv_folder.join("pending");
    let rcv_finished_folder = rcv_folder.join("finished");

    // Spawn the sender and receiver
    let snd_file_server = sender::FileServer::spawn(snd_input_folder, snd_workdir_folder).unwrap();
    let mut rcv_file_server =
        receiver::Reciever::new(rcv_pending_folder, rcv_finished_folder).unwrap();

    // For the sender, spawn a thread+channel to pass the chunks through, so we can add a timeout when waiting for new chunks
    let (chunks_snd, chunks_rcv) = crossbeam_channel::bounded(10);
    let snd_file_server = Arc::new(snd_file_server);
    let thread_snd_file_server = snd_file_server.clone();
    thread::spawn(move || loop {
        chunks_snd.send(thread_snd_file_server.get_chunk()).unwrap();
    });

    loop {
        let mut send_remaining = 100;
        while let Ok(chunk) = chunks_rcv.recv_timeout(Duration::from_secs(1)) {
            let should_send = rand::random::<bool>();

            if should_send {
                rcv_file_server.receive_chunk(chunk).unwrap();
            }

            send_remaining -= 1;
            if send_remaining == 0 {
                break;
            }
        }

        rcv_file_server.output_finished_files().unwrap();

        for control in rcv_file_server.iter_control_messages() {
            snd_file_server.send_control_msg(control);
        }
        snd_file_server.send_control_msg(ControlMessage::Continue);

        thread::sleep(Duration::from_secs(1));
    }
}
