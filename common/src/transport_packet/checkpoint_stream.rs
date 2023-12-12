use std::{collections::VecDeque, io::Read};

/// A stream designed for easy checkpointing and rollback.
/// It's used in error tolerant stream parsing, where if an error occurs,
/// the stream can be rolled back to the last checkpoint.
pub struct StreamWithCheckpoints<R: Read> {
    reader: R,
    prev_data: VecDeque<u8>,
    prev_data_pos: usize,
}

impl<R: Read> StreamWithCheckpoints<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            prev_data: VecDeque::new(),
            prev_data_pos: 0,
        }
    }

    /// Checkpoint the current position.
    pub fn checkpoint(&mut self) {
        if self.prev_data.len() >= self.prev_data_pos {
            self.prev_data.drain(..self.prev_data_pos);
            self.prev_data_pos = 0;
        }
    }

    /// Skip ahead by N bytes and checkpoint the new position.
    pub fn skip_and_checkpoint(&mut self, skip_by: usize) {
        self.prev_data_pos += skip_by;

        if self.prev_data_pos > self.prev_data.len() {
            let ahead = self.prev_data_pos - self.prev_data.len();

            // Skip ahead by the ahead amount
            let buf = &mut [0u8; 8];
            let mut remaining = ahead;
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, buf.len());
                self.reader.read_exact(&mut buf[..to_read]).unwrap();
                remaining -= to_read;
            }

            self.prev_data_pos = 0;
            self.prev_data.clear();
        } else {
            self.prev_data.drain(..self.prev_data_pos);
            self.prev_data_pos = 0;
        }
    }

    /// Rollback to the last checkpoint.
    pub fn rollback(&mut self) {
        self.prev_data_pos = 0;
    }

    pub fn rollback_n(&mut self, n: usize) {
        if self.prev_data_pos < n {
            panic!(
                "Cannot rollback {} bytes, only {} bytes are buffered",
                n, self.prev_data_pos
            );
        }
        self.prev_data_pos -= n;
    }

    pub fn rollback_len(&self) -> usize {
        self.prev_data_pos
    }

    pub fn cached_len(&self) -> usize {
        self.prev_data.len() - self.prev_data_pos
    }

    pub fn remaining_cached_len(&self) -> usize {
        self.prev_data.len() - self.prev_data_pos
    }
}

impl<R: Read> Read for StreamWithCheckpoints<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut read = 0;

        let remaining_buffer = self.prev_data.len() - self.prev_data_pos;
        let remaining_buffer_min = std::cmp::min(remaining_buffer, buf.len());

        let buf_for_prev_data = &mut buf[..remaining_buffer_min];

        // Get the slices from the previous data (it's a VecDeque so there's 2 slices)
        let (left, right) = self.prev_data.as_slices();
        let (left, right) = if self.prev_data_pos < left.len() {
            // If the position is in the left slice, cut the left slice
            (&left[self.prev_data_pos..], right)
        } else {
            // If the position is in the right slice, omit the left slice and cut the right slice
            (&[][..], &right[self.prev_data_pos - left.len()..])
        };

        // Copy the previous data to the buffer
        // First the left
        let prev_data_read_left = std::cmp::min(left.len(), buf_for_prev_data.len());
        buf_for_prev_data[..prev_data_read_left].copy_from_slice(&left[..prev_data_read_left]);

        // Then the right
        let buf_remaining = buf_for_prev_data.len() - prev_data_read_left;
        let prev_data_read_right = std::cmp::min(right.len(), buf_remaining);
        buf_for_prev_data[prev_data_read_left..].copy_from_slice(&right[..prev_data_read_right]);

        // Update the position
        self.prev_data_pos += buf_for_prev_data.len();
        read += buf_for_prev_data.len();

        // If there's still more data to read, read it
        if remaining_buffer_min < buf.len() {
            let buf_remaining = &mut buf[remaining_buffer_min..];

            let read_from_stream = self.reader.read(buf_remaining)?;

            // Update the position
            read += read_from_stream;
            self.prev_data_pos += read_from_stream;

            // Append the read data to the previous data
            self.prev_data
                .extend(buf_remaining[..read_from_stream].iter());
        }

        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn expect_read<const CHARS: usize>(
        stream: &mut StreamWithCheckpoints<impl Read>,
        buf: &[u8; CHARS],
    ) {
        let mut read_buf = vec![0u8; buf.len()];
        stream.read_exact(&mut read_buf).unwrap();
        assert_eq!(read_buf, buf);
    }

    #[test]
    fn test_new() {
        let data = b"Hello, world!";
        let reader = Cursor::new(data);
        let mut stream = StreamWithCheckpoints::new(reader);

        assert_eq!(stream.prev_data.len(), 0);
        assert_eq!(stream.prev_data_pos, 0);

        expect_read(&mut stream, b"Hello, world!");
        stream.rollback();
        expect_read(&mut stream, b"Hello, ");
        stream.checkpoint();
        expect_read(&mut stream, b"world!");
        stream.rollback();
        expect_read(&mut stream, b"world!");
    }

    #[test]
    fn read_extra() {
        let data = b"Hello, world!";
        let reader = Cursor::new(data);
        let mut stream = StreamWithCheckpoints::new(reader);

        expect_read(&mut stream, b"Hello");
        stream.rollback();

        let buf = &mut [0u8; 20];
        let read = stream.read(buf).unwrap();

        assert_eq!(read, 13);
        assert_eq!(&buf[..read], b"Hello, world!");
    }

    #[test]
    fn test_skip_and_checkpoint() {
        let data = b"Hello, world!";
        let reader = Cursor::new(data);
        let mut stream = StreamWithCheckpoints::new(reader);

        stream.skip_and_checkpoint(5);
        expect_read(&mut stream, b", wor");

        stream.rollback();
        expect_read(&mut stream, b", wor");

        stream.skip_and_checkpoint(1);
        expect_read(&mut stream, b"d!");
    }

    #[test]
    fn test_skip_and_checkpoint2() {
        let data = b"Hello, world!";
        let reader = Cursor::new(data);
        let mut stream = StreamWithCheckpoints::new(reader);

        expect_read(&mut stream, b"Hello, world!");
        stream.rollback();

        stream.skip_and_checkpoint(5);
        expect_read(&mut stream, b", wor");

        stream.rollback();
        expect_read(&mut stream, b", wor");

        stream.skip_and_checkpoint(1);
        expect_read(&mut stream, b"d!");
    }
}
