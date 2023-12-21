use std::{io::Read, path::PathBuf, thread::JoinHandle};

use common::{control::ControlMessage, transport_packet::parse_transport_packet_stream};
use crossbeam_channel::{Receiver, Sender};

mod downlink_session;
mod managed_file;
mod tempdir;
mod storage_manager;

pub struct DownlinkServer {
    files_dir: PathBuf,
    control_rcv: Receiver<ControlMessage>,
    active_session: Option<downlink_session::DownlinkSession>,
}

impl DownlinkServer {
    // pub fn new(files_dir: PathBuf, control_reader: impl 'static + Read + Send) -> Self {
    //     let (control_snd, control_rcv) = crossbeam_channel::unbounded();

    //     spawn_control_reader(control_reader, control_snd.clone());

    //     Self {
    //         files_dir,
    //         control_rcv,
    //     }
    // }
}

pub struct DownlinkServerWriter {}

fn spawn_control_reader(
    control_reader: impl 'static + Read + Send,
    control_snd: Sender<ControlMessage>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let control_parser = parse_transport_packet_stream(control_reader);

        for packet in control_parser {
            let packet = match packet {
                Ok(packet) => packet,
                Err(err) => {
                    tracing::error!("Error parsing control packet: {}", err);
                    continue;
                }
            };

            let control = packet.as_control_message();
            let Some(control) = control else {
                tracing::error!("Received non-control packet in control stream");
                continue;
            };

            let send_result = control_snd.send(control);
            if send_result.is_err() {
                // If the receiver is gone, we can just stop reading
                tracing::info!("Control message receiver disconnected");
                return;
            }
        }
    })
}
