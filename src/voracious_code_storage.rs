use std::net::SocketAddr;

use anyhow::{bail, Result};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::tcp;

enum Request {
    PutFile { filename: String, content: Vec<u8> },
    Closed,
}

enum Response {
    Ready,
}

impl Into<Vec<u8>> for Response {
    fn into(self) -> Vec<u8> {
        match self {
            Self::Ready => b"READY\n",
        }
        .to_vec()
    }
}

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

    pub async fn incoming(&mut self) -> Result<Request> {
        let mut buf = String::new();
        let req = match self.reader.read_line(&mut buf).await? {
            0 => Request::Closed,
            _ => {
                buf.pop();
                info!("Received incoming message: '{}'", buf);
                let parts: Vec<_> = buf.split(' ').collect();
                match parts[0] {
                    "PUT" => {
                        let content_len = parts[2].parse::<usize>().unwrap();
                        let mut content = vec![0; content_len];
                        self.reader.read_exact(&mut content).await?;
                        Request::PutFile {
                            filename: parts[1].to_string(),
                            content,
                        }
                    }
                    typ => {
                        bail!("unknown request type: {}", typ);
                    }
                }
            }
        };
        Ok(req)
    }

    pub async fn outgoing(&mut self, response: Response) -> Result<()> {
        let data: Vec<u8> = response.into();
        self.writer.write_all(&data).await?;
        Ok(())
    }
}

struct Upstream {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Upstream {
    pub async fn connect() -> Result<Self> {
        const ADDR: &'static str = "vcs.protohackers.com:30307";
        let (rh, wh) = TcpStream::connect(ADDR).await?.into_split();
        Ok(Self {
            reader: BufReader::new(rh),
            writer: wh,
        })
    }

    pub async fn send(&mut self, req: Request) -> Result<Response> {
        let resp = match req {
            Request::PutFile { filename, content } => {
                let mut data = format!("PUT {} {}\n", filename, content.len()).into_bytes();
                data.extend(content);
                info!("Sending {} to Upstream", unsafe {
                    String::from_utf8_unchecked(data.clone())
                });
                self.writer.write_all(&data).await?;

                let mut resp = String::new();
                self.reader.read_line(&mut resp).await?;
                info!("Received {} from Upstream", resp);
                Response::Ready
            }
            _ => Response::Ready,
        };
        Ok(resp)
    }
}

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr) -> Result<()> {
    let mut ctx = {
        let (rh, wh) = socket.split();
        let rh = BufReader::new(rh);
        Context::new(rh, wh)
    };
    let mut upstream = Upstream::connect().await?;
    loop {
        let req = ctx.incoming().await?;
        match req {
            Request::PutFile { filename, content } => {
                info!(
                    "Requesting to put content[len={}] into file '{}'",
                    content.len(),
                    filename
                );
                upstream
                    .send(Request::PutFile { filename, content })
                    .await?;
            }
            Request::Closed => {
                info!("Closing the connection");
                break;
            }
        }

        ctx.outgoing(Response::Ready).await?;
    }

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}
