pub struct ScramblingWriter<W: std::io::Write> {
    writer: W,
    cum_sum: u8,
}

impl<W: std::io::Write> ScramblingWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer, cum_sum: 0 }
    }
}

impl<W: std::io::Write> std::io::Write for ScramblingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // We scramble by adding the cumulative sum of previous bytes to the next byte.
        for &byte in buf {
            let next = byte.wrapping_add(self.cum_sum);
            self.writer.write_all(&[next])?;
            self.cum_sum = next;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub struct UnscramblingReader<R: std::io::Read> {
    reader: R,
    cum_sum: u8,
}

impl<R: std::io::Read> UnscramblingReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader, cum_sum: 0 }
    }
}

impl<R: std::io::Read> std::io::Read for UnscramblingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buf)?;

        // We unscramble by subtracting the cumulative sum of previous bytes to the next byte.
        for byte in buf {
            let next = byte.wrapping_sub(self.cum_sum);
            self.cum_sum = *byte;
            *byte = next;
        }

        Ok(read)
    }
}
