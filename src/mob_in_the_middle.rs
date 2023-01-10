use std::cmp;
use std::net::SocketAddr;

use anyhow::{anyhow, Result};
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
    R: AsyncRead + Unpin + AsyncBufReadExt,
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

    pub async fn respond(&mut self, content: &str) -> Result<()> {
        self.writer.write_all(content.as_bytes()).await?;
        self.writer.write_u8(b'\n').await?;
        self.writer.flush().await?;
        Ok(())
    }
}

const TARGET_ADDRESS: &'static str = "7YWHMfk9JZe0LM0g1ZauHuiSxhI";

#[inline]
fn is_boguscoin_address(s: &str) -> bool {
    info!("Checking address: '{}'", s);
    26 <= s.len() && s.len() <= 35 && s.starts_with('7') && s.chars().all(char::is_alphanumeric)
}

#[inline]
fn rewrite_prefix(original: String) -> String {
    if original.len() < 26 {
        return original;
    }

    let bytes = original.as_bytes();

    let l = if bytes[0] == b' ' { 1 } else { 0 };
    let r = {
        let mut r = l;
        while r < bytes.len() && bytes[r] != b' ' {
            r += 1;
        }
        cmp::min(r, bytes.len())
    };
    if is_boguscoin_address(String::from_utf8_lossy(&bytes[l..r]).as_ref()) {
        // rewrite the prefix
        let mut m = String::new();
        if l == 1 {
            m.push(' ');
        }
        m.push_str(TARGET_ADDRESS);
        m.push_str(String::from_utf8_lossy(&bytes[r..]).as_ref());
        m
    } else {
        original
    }
}

#[inline]
fn rewrite_postfix(original: String) -> String {
    if original.len() < 26 {
        return original;
    }

    let bytes = original.as_bytes();
    let len = bytes.len();

    let r = if bytes[len - 1] == b' ' { len - 1 } else { len };
    let l = {
        let mut l = r as isize - 1;
        while 0 <= l - 1 && bytes[l as usize - 1] != b' ' {
            l -= 1;
        }
        cmp::max(l, 0) as usize
    };
    if is_boguscoin_address(String::from_utf8_lossy(&bytes[l..r]).as_ref()) {
        // rewrite the postfix
        let mut m = String::new();
        m.push_str(String::from_utf8_lossy(&bytes[0..l]).as_ref());
        m.push_str(TARGET_ADDRESS);
        if r == len - 1 {
            m.push(' ');
        }
        m
    } else {
        original
    }
}

fn rewrite_message(mut message: String) -> String {
    info!("original message: '{}'", message);
    message = rewrite_prefix(message);
    info!("after rewriting prefix: '{}'", message);
    message = rewrite_postfix(message);
    info!("after rewriting postfix: '{}'", message);
    message
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
                    upstream.respond(&resp).await?;
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
                    downstream.respond(&resp).await?;
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
