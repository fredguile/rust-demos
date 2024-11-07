use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

use mini_redis::{constants::DEFAULT_PORT, server, FnResult};

#[derive(Parser, Debug)]
#[command(
    name = "mini-redis-server",
    version,
    author,
    about = "Mini redis server"
)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> FnResult<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

fn set_up_logging() -> FnResult<()> {
    tracing_subscriber::fmt::try_init()
}
