use bytes::Bytes;
use core::str;
use std::{fmt, vec};

use crate::frame::Frame;

/// Utility for parsing a `Command`.
#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame.
#[derive(Debug)]
pub(crate) enum ParseError {
    // Attempt to extract valud failed due to the frame being fully consumed.
    EndOfStream,

    // All other errors
    Other(crate::GenericError),
}

impl Parse {
    /// Create a new `Parse` to parse content of a frame.
    ///
    /// Returns `Err` if frame isn't an array frame.
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// Return next entry as `String`.
    ///
    /// If next entry cannot be represented as `String`, an error is returned.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!(
                "protocol error; expected simple or bulk frame but got {:?}",
                frame
            )
            .into()),
        }
    }

    /// Return next entry as `Bytes`.
    ///
    /// If next entry cannot be represented as `Bytes`, an error is returned.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple or bulk frame but got {:?}",
                frame
            )
            .into()),
        }
    }

    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &'static str = "protocol error; invalid number";

        match self.next()? {
            Frame::Integer(value) => Ok(value),
            Frame::Simple(s) => atoi::<u64>(s.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expect int frame but got {:?}", frame).into()),
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame but there was more".into())
        }
    }
}

impl std::error::Error for ParseError {}

impl From<String> for ParseError {
    fn from(value: String) -> Self {
        ParseError::Other(value.into())
    }
}

impl From<&str> for ParseError {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(fmt),
            ParseError::Other(err) => err.fmt(fmt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Mocks {
        unsupported_frames: Vec<Frame>,
        empty_frame: Frame,
        string_frame: Frame,
        bytes_frame: Frame,
        integer_frame: Frame,
        long_frame: Frame,
    }

    fn setup() -> Mocks {
        Mocks {
            unsupported_frames: vec![
                Frame::Null,
                Frame::Simple("x".to_string()),
                Frame::Integer(1),
                Frame::Bulk(Bytes::from("x")),
                Frame::Error("error".to_string()),
            ],
            empty_frame: Frame::array(),
            string_frame: (|| {
                let mut frame = Frame::array();
                frame.push_simple("test".to_string());
                frame
            })(),
            bytes_frame: (|| {
                let mut frame = Frame::array();
                frame.push_bulk(Bytes::from("x"));
                frame
            })(),
            integer_frame: (|| {
                let mut frame = Frame::array();
                frame.push_int(1);
                frame
            })(),
            long_frame:  (|| {
                let mut frame = Frame::array();
                frame.push_int(1);
                frame.push_simple("x".to_string());
                frame
            })(),
        }
    }

    #[test]
    fn should_parse_only_supported_frames() {
        let Mocks {
            unsupported_frames,
            empty_frame: supported_frame,
            ..
        } = setup();

        for unsupported_frame in unsupported_frames {
            let expected_error = format!(
                "protocol error; expected array, got {:?}",
                unsupported_frame
            );

            match Parse::new(unsupported_frame) {
                Ok(_) => panic!("shouldn't parse unsupported frame"),
                Err(parse_error) => match parse_error {
                    ParseError::EndOfStream => panic!("shouldn't error with EndOfStream"),
                    ParseError::Other(err) => assert_eq!(err.to_string(), expected_error),
                },
            }
        }

        assert!(matches!(Parse::new(supported_frame), Ok(_)));
    }

    #[test]
    fn should_parse_string() {
        let Mocks { string_frame, .. } = setup();

        match Parse::new(string_frame) {
            Ok(mut parse) => match parse.next_string() {
                Ok(value) => assert_eq!(value, "test"),
                Err(_) => panic!("should return next string from frame"),
            },
            Err(_) => panic!("should parse string frame"),
        }
    }

    #[test]
    fn should_parse_bytes() {
        let Mocks { bytes_frame, .. } = setup();

        match Parse::new(bytes_frame) {
            Ok(mut parse) => match parse.next_bytes() {
                Ok(value) => assert_eq!(value, Bytes::from("x")),
                Err(_) => panic!("should return next bytes from frame"),
            },
            Err(_) => panic!("should parse bytes frame"),
        }
    }

    #[test]
    fn should_parse_integer() {
        let Mocks { integer_frame, .. } = setup();

        match Parse::new(integer_frame) {
            Ok(mut parse) => match parse.next_int() {
                Ok(value) => assert_eq!(value, 1),
                Err(_) => panic!("should return next integer from frame"),
            },
            Err(_) => panic!("should parse integer frame"),
        }
    }

    #[test]
    fn should_iterate_frames() {
        let Mocks { long_frame, .. } = setup();

        let mut parse = Parse::new(long_frame).unwrap();
        assert!(matches!(parse.next(), Ok(_)));
        assert!(matches!(parse.next(), Ok(_)));
        assert!(matches!(parse.finish(), Ok(_)));
    }
}
