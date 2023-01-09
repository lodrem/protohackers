use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info};

pub(crate) async fn run(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
    let mut buf = [0; 1024];

    while let Ok(n) = socket.read(&mut buf).await {
        if n == 0 {
            break;
        }

        if let Err(e) = socket.write_all(&buf[0..n]).await {
            error!("Failed to write to socket: {:?}", e);
            break;
        }
    }

    info!("Dropping connection {}", remote_addr);

    Ok(())
}
