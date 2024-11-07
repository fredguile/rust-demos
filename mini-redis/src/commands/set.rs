use bytes::Bytes;
use tracing::debug;
use std::time::Duration;

use crate::{connection::Connection, db::Db, frame::Frame, parse::{Parse, ParseError}};

/// Set the value of a key.
#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// Parse a `Set` instance from a received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::FnResult<Set> {
        // Note: the `SET` string has already been consumed, next values are `key`, `value`` and `expire`
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;

        let mut expire = None;

        // Attempt to parse another string
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // An expiration is specified in secs, next value is an integer
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // An expiration is specified in ms, next value is an integer
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(ParseError::EndOfStream) => {}
            Err(err) => return Err(err.into())
        }

        Ok(Set { key, value, expire })
    }

    /// Apply the `Set` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::FnResult<()> {
        db.set(self.key, self.value, self.expire);

        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);

        if let Some(expire) = self.expire {
            // Expiration in Redis protocol can be specified in two ways:
            // 1. SET key value EX seconds
            // 2. SET key value PX ms
            // We'll use second for greater precision
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(expire.as_millis() as u64);
        }

        frame
    }
}