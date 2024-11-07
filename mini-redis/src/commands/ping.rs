use bytes::Bytes;
use tracing::debug;

use crate::{
    connection::Connection,
    frame::Frame,
    parse::{Parse, ParseError},
};

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as bulk frame.
#[derive(Default, Debug)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// Parse a `Ping` instance from a received `Frame`.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::FnResult<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Apply the `Ping` command and return the message.
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::FnResult<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        debug!(?response);

        // Write response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Convert the command into an equivalent of `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));

        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }

        frame
    }
}
