mod signal;

use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;
use tracing::{error, info, Level};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?;
    rt.block_on(async {
        let addr = "0.0.0.0:8070".parse().unwrap();
        tokio::spawn(serve_tcp(addr));

        signal::shutdown().await;
    });

    Ok(())
}

async fn serve_tcp(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    info!("TCP Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                info!("Accepting socket from {}", remote_addr);
                tokio::spawn(handle_connection(socket, remote_addr));
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }
}

async fn handle_connection(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
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
