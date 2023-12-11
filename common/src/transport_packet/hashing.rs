use std::hash::Hasher;

struct ChunkedHasher {
    hasher: twox_hash::XxHash64,
    hash_window: [u8; 16],
    hash_window_idx: usize,
}

impl ChunkedHasher {
    pub fn new() -> Self {
        Self {
            hasher: twox_hash::XxHash64::with_seed(0),
            hash_window: [0; 16],
            hash_window_idx: 0,
        }
    }

    pub fn write(&mut self, buf: &[u8]) {
        // Hash in 16 byte chunks.
        let mut buf_pos = 0;
        while buf_pos < buf.len() {
            let remaining_window = self.hash_window.len() - self.hash_window_idx;
            let remaining_buf = buf.len() - buf_pos;
            let copy_len = remaining_window.min(remaining_buf);
            self.hash_window[self.hash_window_idx..self.hash_window_idx + copy_len]
                .copy_from_slice(&buf[buf_pos..buf_pos + copy_len]);
            self.hash_window_idx += copy_len;
            buf_pos += copy_len;

            if self.hash_window_idx == self.hash_window.len() {
                self.hasher.write(&self.hash_window);
                self.hash_window_idx = 0;
            }
        }
    }

    pub fn result(mut self) -> u64 {
        if self.hash_window_idx > 0 {
            self.hasher.write(&self.hash_window[..self.hash_window_idx]);
        }

        self.hasher.finish()
    }
}

pub struct HashedWriter<W: std::io::Write> {
    writer: W,
    chunked_hasher: ChunkedHasher,
}

impl<W: std::io::Write> HashedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            chunked_hasher: ChunkedHasher::new(),
        }
    }

    pub fn result(self) -> u64 {
        self.chunked_hasher.result()
    }
}

impl<W: std::io::Write> std::io::Write for HashedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(buf)?;
        self.chunked_hasher.write(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub struct HashedReader<R: std::io::Read> {
    reader: R,
    chunked_hasher: ChunkedHasher,
}

impl<R: std::io::Read> HashedReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            chunked_hasher: ChunkedHasher::new(),
        }
    }

    pub fn result(self) -> u64 {
        self.chunked_hasher.result()
    }
}

impl<R: std::io::Read> std::io::Read for HashedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buf)?;
        self.chunked_hasher.write(&buf[..read]);
        Ok(read)
    }
}
