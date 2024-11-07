use bytes::Bytes;

use crate::{connection::Connection, db::Db, frame::Frame, parse::Parse};

/// Post a message to the given channel.
#[derive(Debug)]
pub struct Publish {
    channel: String,
    message:Bytes,
}

impl Publish {
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// Parse a `Publish` instance from a received frame.
    pub(crate) fn parse_frame(parse: &mut Parse) -> crate::FnResult<Publish> {
         // Note: the `PUBLISH` string has already been consumed, next values are `channel` and `message`
         let channel = parse.next_string()?;
         let message = parse.next_bytes()?;

         Ok(Publish { channel, message })
    }

    /// Apply the `Publish` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::FnResult<()> {
        let num_subscribers = db.publish(&self.channel, self.message);

        let response = Frame::Integer(num_subscribers as u64);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);
        frame
    }
}