use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
    thread::JoinHandle,
};

use common::{
    binary_serialize::BinarySerialize,
    transport_packet::{parse_transport_packet_stream, TransportPacket, TransportPacketData},
};

use crate::byte_pipe::make_corrupt_pipe;

pub struct TestRunner {
    join_handle: JoinHandle<()>,
    kill: Arc<AtomicBool>,
    input_folder: PathBuf,
    output_folder: PathBuf,
}

impl TestRunner {
    pub fn new(base_folder: &Path) -> TestRunner {
        let snd_folder = base_folder.join("snd");
        let rcv_folder = base_folder.join("rcv");

        let snd_input_folder = snd_folder.join("input");
        let snd_workdir_folder = snd_folder.join("workdir");

        let rcv_pending_folder = rcv_folder.join("pending");
        let rcv_finished_folder = rcv_folder.join("finished");

        // Spawn the sender and receiver
        let snd_file_server = sender::FileServer::spawn_with_part_size(
            snd_input_folder.clone(),
            snd_workdir_folder,
            1048576,
        )
        .unwrap();
        let mut rcv_file_server =
            receiver::Reciever::new(rcv_pending_folder, rcv_finished_folder.clone()).unwrap();

        let kill_flag = Arc::new(AtomicBool::new(false));

        let corrupt_freq = 65536;

        let (mut chunks_snd, chunks_rcv) = make_corrupt_pipe(corrupt_freq, kill_flag.clone());
        let (mut control_snd, control_rcv) = make_corrupt_pipe(corrupt_freq, kill_flag.clone());

        let rcv_join = std::thread::spawn(move || {
            let control_interval = 50;

            let mut chunk_parse = parse_transport_packet_stream(chunks_rcv);

            'outer: loop {
                for _ in 0..control_interval {
                    let chunk = chunk_parse.next();
                    let Some(chunk) = chunk else {
                        break 'outer;
                    };

                    let chunk = chunk.unwrap().as_chunk().expect("Expected chunk");

                    tracing::info!("Received chunk: {}", chunk);

                    rcv_file_server.receive_chunk(chunk).unwrap();
                }

                rcv_file_server.output_finished_files().unwrap();

                for control in rcv_file_server.iter_control_messages() {
                    let packet =
                        TransportPacket::new(TransportPacketData::from_control_message(control));
                    packet.serialize_to_stream(&mut control_snd).unwrap();
                }
            }

            tracing::info!("Receiver thread finished");
        });

        let chunk_snd_server = snd_file_server.clone();
        let chunk_sender = std::thread::spawn(move || loop {
            let chunk = chunk_snd_server.get_chunk().unwrap();
            let packet = TransportPacket::new(TransportPacketData::from_chunk(chunk));
            packet.serialize_to_stream(&mut chunks_snd).unwrap();
        });

        let control_snd_server = snd_file_server.clone();
        let control_receiver = std::thread::spawn(move || {
            let stream = parse_transport_packet_stream(control_rcv);
            for packet in stream {
                let packet = packet.unwrap();
                let control = packet
                    .as_control_message()
                    .expect("Expected control message");

                tracing::info!("Received control message: {}", control);

                control_snd_server.send_control_msg(control).unwrap();
            }

            tracing::info!("Control receiver thread finished");
        });

        let join_handle = std::thread::spawn(move || {
            chunk_sender
                .join()
                .map_err(|e| {
                    tracing::error!("chunk sender thread: {:?}", e.downcast_ref::<String>())
                })
                .ok();
            control_receiver
                .join()
                .map_err(|e| {
                    tracing::error!("control rcv thread: {:?}", e.downcast_ref::<String>())
                })
                .ok();
            rcv_join
                .join()
                .map_err(|e| tracing::error!("rcv join thread: {:?}", e.downcast_ref::<String>()))
                .ok();

            snd_file_server.join();

            tracing::info!("Sender thread finished");
        });

        TestRunner {
            join_handle,
            kill: kill_flag,
            input_folder: snd_input_folder,
            output_folder: rcv_finished_folder,
        }
    }
}
