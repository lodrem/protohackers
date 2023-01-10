use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::tcp;

const UPSTREAM: &'static str = "chat.protohackers.com:16963";

enum Request {
    Message(String),
    Closed,
}

struct Context<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> Context<R, W>
where
    R: AsyncRead + AsyncBufReadExt + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(r: R, w: W) -> Self {
        Self {
            reader: r,
            writer: w,
        }
    }

    pub async fn incoming_request(&mut self) -> Result<Request> {
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(n) if n == 0 => Ok(Request::Closed),
            Ok(_) => {
                line.pop();
                Ok(Request::Message(line))
            }
            Err(e) => Err(anyhow!("Failed to read incoming request: {:?}", e)),
        }
    }

    pub async fn respond(&mut self, mut content: String) -> Result<()> {
        content.push('\n');
        self.writer.write_all(content.as_bytes()).await?;
        Ok(())
    }
}

const TARGET_ADDRESS: &'static str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

fn rewrite_message(message: String) -> String {
    let re = Regex::new(r"(^|\s)7[0-9A-Za-z]{25,34}($|\s)").unwrap();
    let target = re.replace(&message, TARGET_ADDRESS).to_string();

    target
}

async fn handle(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
    let mut upstream = {
        let (rh, wh) = (TcpStream::connect(UPSTREAM).await?).into_split();
        let reader = BufReader::new(rh);
        Context::new(reader, wh)
    };
    let mut downstream = {
        let (rh, wh) = socket.split();
        let reader = BufReader::new(rh);
        Context::new(reader, wh)
    };

    loop {
        tokio::select! {
            req = downstream.incoming_request() => match req {
                Ok(Request::Message(message)) => {
                    info!("{} -> upstream: {}", remote_addr, message);
                    let resp = rewrite_message(message);
                    upstream.respond(resp).await?;
                }
                Ok(Request::Closed) => {
                    info!("Downstream closed connection");
                    break;
                }
                Err(e) => {
                    error!("Failed to read from downstream: {:?}", e);
                    break;
                }
            },
            req = upstream.incoming_request() => match req {
                Ok(Request::Message(message)) => {
                    info!("upstream -> {}: {}", remote_addr, message);
                    let resp = rewrite_message(message);
                    downstream.respond(resp).await?;
                }
                Ok(Request::Closed) => {
                    info!("Upstream closed connection");
                    break;
                }
                Err(e) => {
                    error!("Failed to read from upstream: {:?}", e);
                    break;
                }
            }
        }
    }

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}
