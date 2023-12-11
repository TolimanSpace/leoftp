use std::io::Read;

/// A stream that can wrap around another stream, and limits
/// the maximum number of bytes that can be read from it.
pub struct SubstreamReader<R: Read> {
    remaining_len: usize,
    reader: R,
}

impl<R: Read> SubstreamReader<R> {
    pub fn new(reader: R, len: usize) -> Self {
        Self {
            remaining_len: len,
            reader,
        }
    }

    pub fn reached_end(&self) -> bool {
        self.remaining_len == 0
    }
}

impl<R: Read> Read for SubstreamReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining_len == 0 {
            return Ok(0);
        }

        let to_read = std::cmp::min(self.remaining_len, buf.len());
        let read = self.reader.read(&mut buf[..to_read])?;
        self.remaining_len -= read;
        Ok(read)
    }
}
