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
    let parts: Vec<&str> = message.split(' ').collect();
    let parts: Vec<String> = parts
        .into_iter()
        .map(|p| {
            if p.starts_with('7')
                && 26 <= p.len()
                && p.len() <= 35
                && p.chars().all(char::is_alphanumeric)
            {
                TARGET_ADDRESS.to_owned()
            } else {
                p.to_owned()
            }
        })
        .collect();
    // let re = Regex::new(r"7[0-9A-Za-z]{25,34}").unwrap();
    // let target = re.replace(&message, TARGET_ADDRESS).to_string();
    let target = parts.join(" ");

    info!("Rewrite '{}' to '{}'", message, target);

    target
}

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr) -> Result<()> {
    let mut upstream = {
        let socket = TcpStream::connect(UPSTREAM).await?;
        let (rh, wh) = socket.into_split();
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
