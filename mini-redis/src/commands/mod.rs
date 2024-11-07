mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod publish;
pub use publish::Publish;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{connection::Connection, db::Db, frame::Frame, parse::Parse, shutdown::Shutdown};

/// Enumeration of supported Redis commands
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    /// Parse command from receive `Frame`.
    ///
    /// The `Frame` must represent a Redis supported command.
    pub fn from_frame(frame: Frame) -> crate::FnResult<Command> {
        // Frame is decorated with `Parse`
        let mut parse = Parse::new(frame)?;

        // All Redis commands beging with the command name as string.
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frame(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frame(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frame(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // Check if there's any remaing unconsumed fields in the `Parse` value.
        // Maybe raise an error if so.
        parse.finish()?;

        Ok(command)
    }

    /// Apply command to the specified `Db` instance.
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::FnResult<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }

    /// Return command name
    pub(crate) fn get_name(&self) -> &str {
        use Command::*;

        match self {
            Get(_) => "get",
            Set(_) => "set",
            Publish(_) => "publish",
            Subscribe(_) => "subscribe",
            Unsubscribe(_) => "unsubscribe",
            Unknown(cmd) => cmd.get_name(),
            Ping(_) => "ping",
        }
    }
}
