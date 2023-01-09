use std::future::Future;
use std::net::SocketAddr;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

pub async fn serve<F, Fut>(addr: SocketAddr, mut handle: F) -> Result<()>
where
    F: Send + 'static + FnMut(TcpStream, SocketAddr) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;

    info!("TCP Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                info!("Accepting socket from {}", remote_addr);
                let rv = handle(socket, remote_addr);
                tokio::spawn(async move {
                    if let Err(e) = rv.await {
                        error!("Failed to handle socket from {}: {:?}", remote_addr, e);
                    }
                    info!("Dropping socket {}", remote_addr);
                });
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }
}
