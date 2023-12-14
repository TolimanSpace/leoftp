use std::{
    io,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use rand::Rng;
struct BytePipeSnd {
    snd: crossbeam_channel::Sender<Vec<u8>>,
    kill_signal: Arc<AtomicBool>,
}

struct BytePipeRcvShared {
    rcv: crossbeam_channel::Receiver<Vec<u8>>,
    kill_signal: Arc<AtomicBool>,
}

struct BytePipeRcv {
    shared: BytePipeRcvShared,

    last_buf: Vec<u8>,
    last_buf_pos: usize,
}

struct BytePipeCorruptRcv {
    shared: BytePipeRcvShared,

    last_buf: Vec<u8>,
    last_buf_pos: usize,

    corrupt_byte_chance: f32,
    skip_bytes_chance: f32,
    max_bytes_skip: usize,
    remaining_bytes_skip: usize,
}

impl BytePipeSnd {
    fn is_killed(&self) -> bool {
        self.kill_signal.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl std::io::Write for BytePipeSnd {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut written = 0;

        if self.is_killed() {
            return Ok(0);
        }

        let vecs = buf.chunks(256).into_iter().map(|chunk| chunk.to_vec());

        'sender: for mut vec in vecs {
            let len = vec.len();
            loop {
                if self.is_killed() {
                    return Ok(written);
                }

                let timeout = Duration::from_millis(100);
                let result = self.snd.send_timeout(vec, timeout);

                // If we got an error, it's either a safe timeout or a broken pipe
                if let Err(err) = result {
                    if !err.is_timeout() {
                        break 'sender;
                    }

                    vec = err.into_inner();
                } else {
                    written += len;
                    break;
                }
            }
        }

        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl BytePipeRcvShared {
    fn is_killed(&self) -> bool {
        self.kill_signal.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn get_next(&self) -> Option<Vec<u8>> {
        loop {
            if self.is_killed() {
                return None;
            }

            let timeout = Duration::from_millis(100);
            let result = self.rcv.recv_timeout(timeout);

            // If we got an error, it's either a safe timeout or a broken pipe
            if let Err(err) = result {
                if !err.is_timeout() {
                    return None;
                }
            } else {
                return result.ok();
            }
        }
    }
}

impl std::io::Read for BytePipeRcv {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut buf_pos = 0;

        // If we still have space in the buffer, read a new buffer
        while buf_pos < buf.len() {
            if self.last_buf_pos == self.last_buf.len() {
                if let Some(next_buf) = self.shared.get_next() {
                    self.last_buf = next_buf;
                } else {
                    break;
                }
                self.last_buf_pos = 0;
            }

            while self.last_buf_pos < self.last_buf.len() && buf_pos < buf.len() {
                buf[buf_pos] = self.last_buf[self.last_buf_pos];
                buf_pos += 1;
                self.last_buf_pos += 1;
            }
        }

        Ok(buf_pos)
    }
}

impl BytePipeCorruptRcv {
    fn should_corrupt_byte(&self) -> bool {
        if rand::random::<f32>() < self.corrupt_byte_chance {
            return true;
        }

        false
    }

    fn should_skip_bytes(&self) -> bool {
        if rand::random::<f32>() < self.skip_bytes_chance {
            return true;
        }

        false
    }

    fn byte_skip_dist(&self) -> usize {
        rand::thread_rng().gen_range(0..self.max_bytes_skip)
    }
}

impl std::io::Read for BytePipeCorruptRcv {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut buf_pos = 0;

        // If we still have space in the buffer, read a new buffer
        while buf_pos < buf.len() {
            if self.last_buf_pos == self.last_buf.len() {
                if let Some(next_buf) = self.shared.get_next() {
                    self.last_buf = next_buf;
                } else {
                    break;
                }
                self.last_buf_pos = 0;
            }

            while self.last_buf_pos < self.last_buf.len() && buf_pos < buf.len() {
                if self.remaining_bytes_skip > 0 {
                    self.remaining_bytes_skip -= 1;
                } else {
                    let mut val = self.last_buf[self.last_buf_pos];
                    if self.should_corrupt_byte() {
                        val = rand::random::<u8>();
                    }
                    buf[buf_pos] = val;
                    buf_pos += 1;

                    if self.should_skip_bytes() {
                        self.remaining_bytes_skip = self.byte_skip_dist();
                    }
                }
                self.last_buf_pos += 1;
            }
        }

        Ok(buf_pos)
    }
}

pub fn make_pipe(kill_signal: Arc<AtomicBool>) -> (impl io::Write, impl io::Read) {
    let (snd, rcv) = crossbeam_channel::bounded(256);
    (
        BytePipeSnd {
            snd,
            kill_signal: kill_signal.clone(),
        },
        BytePipeRcv {
            shared: BytePipeRcvShared { rcv, kill_signal },
            last_buf: Vec::new(),
            last_buf_pos: 0,
        },
    )
}

pub fn make_corrupt_pipe(
    corruption_freq_bytes: usize,
    kill_signal: Arc<AtomicBool>,
) -> (impl io::Write, impl io::Read) {
    let (snd, rcv) = crossbeam_channel::bounded(256);

    let corrupt_byte_chance = 1.0 / (corruption_freq_bytes as f32);

    (
        BytePipeSnd {
            snd,
            kill_signal: kill_signal.clone(),
        },
        BytePipeCorruptRcv {
            shared: BytePipeRcvShared { rcv, kill_signal },
            last_buf: Vec::new(),
            last_buf_pos: 0,

            corrupt_byte_chance: corrupt_byte_chance / 2.0,
            skip_bytes_chance: corrupt_byte_chance / 2.0,
            max_bytes_skip: 200,
            remaining_bytes_skip: 0,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    #[test]
    fn test_byte_pipe() {
        let kill_signal = Arc::new(AtomicBool::new(false));

        let (mut snd, mut rcv) = make_pipe(kill_signal);

        let data = vec![1, 2, 3, 4, 5];
        snd.write_all(&data).unwrap();
        let data = vec![6];
        snd.write_all(&data).unwrap();

        let mut buf = vec![0u8; 3];
        rcv.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![1, 2, 3]);

        rcv.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![4, 5, 6]);
    }
}
