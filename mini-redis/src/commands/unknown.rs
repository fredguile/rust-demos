use tracing::debug;

use crate::{connection::Connection, frame::Frame};

/// Represents an "unknown" command. This is not a real `Redis` command.
#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    /// Apply the `Unknown` responding to client
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::FnResult<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }
}
