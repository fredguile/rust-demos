use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::frame::{Error, Frame};

/// Send and receive `Frame` chunks from a remote peer.
///
/// Use an underlying `TcpStream` and an internal buffer which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The content of the write buffer is then written to the socket.
#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // Defaults to 4KB read buffer
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single frame from underlying stream.
    ///
    /// Waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the buffer after the frame has been parsed
    /// is kept there for the next call to `read_frame`.
    pub async fn read_frame(&mut self) -> crate::FnResult<Option<Frame>> {
        loop {
            // Attempt to parse a frame. If enough data has been buffered, a frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There isn't enough buffered data, attempt to read more data from socket.
            // On success, number of bytes is returned. `0` indicates end of stream.
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> crate::FnResult<Option<Frame>> {
        // Track the "current" location in the buffer.
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` fn will have advanced the cursor until the end of the frame.
                // Since cursor had position zero before `Frame::check` was called, we obtain the
                // length of the frame by checking the cursor position.
                let len = buf.position().try_into()?;

                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                // Discard parsed data from the read buffer.
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Write a single `Frame` to the underlying stream.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(array) => {
                // Encode the frame type prefix (for an array, it is `*`).
                self.stream.write_u8(b'*').await?;

                // Encode the length of the array.
                self.write_decimal(array.len() as u64).await?;

                // Encode entries in the array.
                for entry in &**array {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        self.stream.flush().await
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(value) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(value.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(value) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(value.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(value) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*value).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(value) => {
                self.stream.write_u8(b'$').await?;
                self.write_decimal(value.len() as u64).await?;
                self.stream.write_all(value).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_) => unreachable!(),
        }
        Ok(())
    }

    async fn write_decimal(&mut self, value: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", value)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
