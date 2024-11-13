use bytes::Bytes;
use clap::{Parser, Subcommand};
use core::str;
use std::num::ParseIntError;
use std::time::Duration;

use mini_redis::clients::client::Client;
use mini_redis::constants::DEFAULT_PORT;
use mini_redis::FnResult;

#[derive(Parser, Debug)]
#[command(
    name = "mini-redis-cli",
    version,
    author,
    about = "Mini redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        msg: Option<Bytes>,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Bytes,

        #[arg(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    Publish {
        channel: String,
        message: Bytes,
    },
    Subscribe {
        channels: Vec<String>,
    },
}

/// Entrypoint for CLI
#[tokio::main(flavor = "current_thread")]
async fn main() -> FnResult<()> {
    let cli = Cli::parse();

    let addr = format!("{}:{}", cli.host, cli.port);

    let mut client = Client::connect(&addr).await?;

    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;

            if let Ok(value_str) = str::from_utf8(&value) {
                println!("\"{}\"", value_str);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(value_str) = str::from_utf8(&value) {
                    println!("\"{}\"", value_str);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)")
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK")
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }

            let mut subscriber = client.subscribe(channels).await?;

            // Await messages on channels
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "got message from the channel: {}; message = {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
