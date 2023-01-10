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

const TARGET_ADDRESS: &'static str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

fn rewrite_message(message: String) -> String {
    let re = Regex::new(r"7[0-9A-Za-z]{25,34}").unwrap();
    let target = re.replace(&message, TARGET_ADDRESS).to_string();

    target
}

async fn incoming_message<R>(reader: &mut R) -> Result<Request>
where
    R: AsyncRead + AsyncBufReadExt + Unpin,
{
    let mut line = String::new();
    match reader.read_line(&mut line).await {
        Ok(n) if n == 0 => Ok(Request::Closed),
        Ok(_) => {
            line.pop();
            Ok(Request::Message(line))
        }
        Err(e) => Err(anyhow!("Failed to read incoming request: {:?}", e)),
    }
}

async fn outgoing_message<W>(writer: &mut W, mut content: String) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    content.push('\n');
    writer.write_all(content.as_bytes()).await?;
    Ok(())
}

async fn handle(socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
    let (mut upstream_rh, mut upstream_wh) = {
        let (rh, wh) = (TcpStream::connect(UPSTREAM).await?).into_split();
        let reader = BufReader::new(rh);
        (reader, wh)
    };
    let (mut downstream_rh, mut downstream_wh) = {
        let (rh, wh) = socket.into_split();
        let reader = BufReader::new(rh);
        (reader, wh)
    };

    let t1 = tokio::spawn(async move {
        loop {
            match incoming_message(&mut downstream_rh).await {
                Ok(Request::Message(message)) => {
                    info!("{} -> proxy: {}", remote_addr, message);
                    let resp = rewrite_message(message);
                    info!("proxy -> upstream: {}", resp);
                    outgoing_message(&mut upstream_wh, resp).await.unwrap();
                }
                Ok(Request::Closed) => {
                    info!("Downstream closed connection");
                    break;
                }
                Err(e) => {
                    error!("Failed to read from downstream: {:?}", e);
                    break;
                }
            }
        }
    });

    let t2 = tokio::spawn(async move {
        loop {
            match incoming_message(&mut upstream_rh).await {
                Ok(Request::Message(message)) => {
                    info!("upstream -> proxy: {}", message);
                    let resp = rewrite_message(message);
                    info!("proxy -> {}: {}", remote_addr, resp);
                    outgoing_message(&mut downstream_wh, resp).await.unwrap();
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
    });

    tokio::select! {
        _ = t1 => {},
        _ = t2 => {},
    }

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}
