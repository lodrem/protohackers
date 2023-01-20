use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::tcp;

struct Context<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> Context<R, W>
where
    R: Unpin + AsyncRead + AsyncBufReadExt,
    W: Unpin + AsyncWrite,
{
    pub fn new(r: R, w: W) -> Self {
        Self {
            reader: r,
            writer: w,
        }
    }

    pub async fn incoming(&mut self) -> Result<Vec<u8>> {
        let mut buf = String::new();
        self.reader.read_line(&mut buf).await?;
        info!("Received incoming message: '{}'", buf);
        Ok(buf.into_bytes())
    }
}

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr) -> Result<()> {
    let mut ctx = {
        let (rh, wh) = socket.split();
        let rh = BufReader::new(rh);
        Context::new(rh, wh)
    };
    while let req = ctx.incoming().await? {}

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}
