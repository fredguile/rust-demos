use bytes::{Buf, Bytes};
use std::{fmt, io::Cursor, num::TryFromIntError, string::FromUtf8Error};

/// A frame in the Redis protocol
#[derive(Clone, Debug)]
pub enum Frame {
    Null,
    Simple(String),
    Integer(u64),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Error(String),
}

#[derive(Debug)]
pub enum Error {
    // Not enough data is available to parse a message
    Incomplete,

    // Invalid message
    Other(crate::GenericError),
}

impl Frame {
    /// Return an empty array.
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push a bulk frame into the array. `self` must be an Array frame.
    /// 
    /// # Panics
    /// 
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Push an integer frame into the array. `self` must be an Array frame.
    /// 
    /// # Panics
    /// 
    /// panics if `self` is not an array
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Push a string frame into the array. `self` must be an Array frame.
    /// 
    /// # Panics
    /// 
    /// panics if `self` is not an array
    pub(crate) fn push_simple(&mut self, value: String) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Simple(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Check if an entire message can be decoded from `src`.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match parse_utils::get_u8(src)? {
            b'+' => {
                parse_utils::get_line(src)?;
                Ok(())
            }
            b'-' => {
                parse_utils::get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = parse_utils::get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == parse_utils::peek_u8(src)? {
                    // skip '-1\r\n'
                    parse_utils::skip(src, 4)
                } else {
                    // read the bulk string
                    let len: usize = parse_utils::get_decimal(src)?.try_into()?;

                    // skip to that number of bytes + 2 (\n\r)
                    parse_utils::skip(src, len + 2)
                }
            }
            b'*' => {
                let len = parse_utils::get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// Parse an already validated (with `check`) message from `src`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match parse_utils::get_u8(src)? {
            b'+' => {
                // read line and convert to `Vec<u8>`
                let line = parse_utils::get_line(src)?.to_vec();

                // convert to String
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // read line and convert to `Vec<u8>`
                let line = parse_utils::get_line(src)?.to_vec();

                // convert to String
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = parse_utils::get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == parse_utils::peek_u8(src)? {
                    let line  = parse_utils::get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // read bulk string
                    let len = parse_utils::get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // skip that number of bytes + 2 (\r\n)
                    parse_utils::skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = parse_utils::get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// Converts the frame to an "unexpected frame" error.
    pub(crate) fn to_error(self) -> crate::GenericError {
        format!("unexpected frame: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value.into())
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_: TryFromIntError) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

mod parse_utils {
    use bytes::Buf;
    use std::io::Cursor;

    use crate::frame::Error;

    pub (super) fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
        if !src.has_remaining() {
            return Err(Error::Incomplete);
        }

        Ok(src.chunk()[0])
    }

    pub (super) fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
        if !src.has_remaining() {
            return Err(Error::Incomplete);
        }

        Ok(src.get_u8())
    }

    pub (super) fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
        if src.remaining() < n {
            return Err(Error::Incomplete);
        }

        src.advance(n);
        Ok(())
    }

    pub (super) fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
        use atoi::atoi;

        let line = get_line(src)?;

        atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
    }

    pub (super) fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8], Error> {
        // scan the bytes directly
        let start = src.position() as usize;

        // scan to the 2nd to last byte
        let end = src.get_ref().len() - 1;

        for i in start..end {
            if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
                // found a line, update position to be after line return
                src.set_position((i + 2) as u64);

                // return the line
                return Ok(&src.get_ref()[start..i]);
            }
        }

        Err(Error::Incomplete)
    }
}
