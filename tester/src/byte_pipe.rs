use std::{
    io,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

const PIPE_BUF_SIZE: usize = 4096;
#[derive(Debug, Clone)]
struct PipeDataBuf {
    data: [u8; PIPE_BUF_SIZE],
    len: usize,
}

impl PipeDataBuf {
    fn len(&self) -> usize {
        self.len
    }

    fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }

    fn push(&mut self, val: u8) {
        self.data[self.len] = val;
        self.len += 1;
    }

    fn is_full(&self) -> bool {
        self.len == self.data.len()
    }

    fn clear(&mut self) {
        self.len = 0;
    }
}

impl Default for PipeDataBuf {
    fn default() -> Self {
        Self {
            data: [0u8; PIPE_BUF_SIZE],
            len: 0,
        }
    }
}

impl IntoIterator for PipeDataBuf {
    type Item = u8;
    type IntoIter = PipeDataIter;

    fn into_iter(self) -> Self::IntoIter {
        PipeDataIter { buf: self, pos: 0 }
    }
}

struct PipeDataIter {
    buf: PipeDataBuf,
    pos: usize,
}

impl Iterator for PipeDataIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.buf.len {
            let val = self.buf.data[self.pos];
            self.pos += 1;
            Some(val)
        } else {
            None
        }
    }
}

use rand::Rng;
pub struct BytePipeSnd {
    snd: crossbeam_channel::Sender<PipeDataBuf>,
    kill_signal: Arc<AtomicBool>,

    current_buf: PipeDataBuf,
}

struct BytePipeRcvShared {
    rcv: crossbeam_channel::Receiver<PipeDataBuf>,
    kill_signal: Arc<AtomicBool>,
}

struct BytePipeRcv {
    shared: BytePipeRcvShared,

    last_buf: PipeDataIter,
}

struct BytePipeCorruptRcv {
    shared: BytePipeRcvShared,

    last_buf: PipeDataIter,

    corrupt_byte_chance: f32,
    skip_bytes_chance: f32,
    max_bytes_skip: usize,
    remaining_bytes_skip: usize,
}

impl BytePipeSnd {
    fn is_killed(&self) -> bool {
        self.kill_signal.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn send_buf(&mut self) -> bool {
        loop {
            if self.is_killed() {
                return false;
            }

            let timeout = Duration::from_millis(100);
            let result = self.snd.send_timeout(self.current_buf.clone(), timeout);

            // If we got an error, it's either a safe timeout or a broken pipe
            if let Err(err) = result {
                if !err.is_timeout() {
                    return false;
                }
            } else {
                self.current_buf.clear();
                return true;
            }
        }
    }
}

impl std::io::Write for BytePipeSnd {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut written = 0;

        if self.is_killed() {
            return Ok(0);
        }

        loop {
            while !self.current_buf.is_full() && written < buf.len() {
                self.current_buf.push(buf[written]);
                written += 1;
            }

            if !self.current_buf.is_full() {
                break;
            }

            if !self.send_buf() {
                break;
            }
        }

        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_buf();
        Ok(())
    }
}

impl BytePipeRcvShared {
    fn is_killed(&self) -> bool {
        self.kill_signal.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn get_next(&self) -> Option<PipeDataBuf> {
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
            let Some(next) = self.last_buf.next() else {
                if let Some(next_buf) = self.shared.get_next() {
                    self.last_buf = next_buf.into_iter();
                    continue;
                } else {
                    break;
                }
            };

            buf[buf_pos] = next;
            buf_pos += 1;
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
            let Some(next) = self.last_buf.next() else {
                if let Some(next_buf) = self.shared.get_next() {
                    self.last_buf = next_buf.into_iter();
                    continue;
                } else {
                    break;
                }
            };

            if self.remaining_bytes_skip > 0 {
                self.remaining_bytes_skip -= 1;
            } else {
                let mut val = next;
                if self.should_corrupt_byte() {
                    val = rand::random::<u8>();
                }
                buf[buf_pos] = val;
                buf_pos += 1;

                if self.should_skip_bytes() {
                    self.remaining_bytes_skip = self.byte_skip_dist();
                }
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
            current_buf: Default::default(),
        },
        BytePipeRcv {
            shared: BytePipeRcvShared { rcv, kill_signal },
            last_buf: PipeDataBuf::default().into_iter(),
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
            current_buf: Default::default(),
        },
        BytePipeCorruptRcv {
            shared: BytePipeRcvShared { rcv, kill_signal },
            last_buf: PipeDataBuf::default().into_iter(),

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

        snd.flush().unwrap();

        let mut buf = vec![0u8; 3];
        rcv.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![1, 2, 3]);

        rcv.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![4, 5, 6]);
    }
}
