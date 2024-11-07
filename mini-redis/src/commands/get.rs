use bytes::Bytes;
use tracing::debug;

use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::Parse;

/// Get the value of a key.
#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Get` instance from a received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::FnResult<Get> {
        // Note: the `GET` string has already been consumed, next value is the name of the key to get
        Ok(Get {
            key: parse.next_string()?,
        })
    }

    /// Apply the `Get` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::FnResult<()> {
        let response = if let Some(key) = db.get(&self.key) {
            // If a value is present, it is written to the client using "bulk" frame
            Frame::Bulk(key)
        } else {
            Frame::Null
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
