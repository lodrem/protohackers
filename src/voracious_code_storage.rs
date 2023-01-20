use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

struct File {
    data: Bytes,
    revision: u64,
}

#[derive(Clone)]
struct State {
    files: Arc<RwLock<HashMap<String, File>>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn put(&mut self, filename: String, content: Bytes) -> u64 {
        let mut files = self.files.write().unwrap();

        let f = files.entry(filename).or_insert_with(|| File {
            data: content.clone(),
            revision: 1,
        });

        if f.data != content {
            f.revision += 1;
            f.data = content;
        }
        f.revision
    }

    pub fn get(&mut self, filename: String) -> Option<Bytes> {
        let files = self.files.read().unwrap();
        files.get(&filename).map(|f| f.data.clone())
    }
}

enum Request {
    PutFile { filename: String, content: Bytes },
    GetFile { filename: String },
    Closed,
}

enum Error {
    FileNotFound,
}

impl Into<Bytes> for Error {
    fn into(self) -> Bytes {
        match self {
            Self::FileNotFound => Bytes::from_static(b"no such file"),
        }
    }
}

enum Response {
    Ready,
    FileContent(Bytes),
    FileRevision(u64),
    Err(Error),
}

impl Into<Bytes> for Response {
    fn into(self) -> Bytes {
        match self {
            Self::Ready => Bytes::from_static(b"READY\n"),
            Self::FileContent(content) => {
                let mut buf = BytesMut::from(format!("OK {}\n", content.len()).as_bytes());
                buf.put(content);
                buf.put(&b"READY\n"[..]);
                buf.freeze()
            }
            Self::FileRevision(revision) => {
                let mut buf = BytesMut::from(format!("OK r{}\n", revision).as_bytes());
                buf.put(&b"READY\n"[..]);
                buf.freeze()
            }
            Self::Err(e) => {
                let mut buf = BytesMut::from(&b"ERR "[..]);
                buf.put::<Bytes>(e.into());
                buf.put_u8(b'\n');
                buf.put(&b"READY\n"[..]);
                buf.freeze()
            }
        }
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
                            content: Bytes::from(content),
                        }
                    }
                    "GET" => Request::GetFile {
                        filename: parts[1].to_string(),
                    },
                    typ => {
                        bail!("unknown request type: {}", typ);
                    }
                }
            }
        };
        Ok(req)
    }

    pub async fn outgoing(&mut self, response: Response) -> Result<()> {
        let data: Bytes = response.into();
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

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr, mut state: State) -> Result<()> {
    let mut ctx = {
        let (rh, wh) = socket.split();
        let rh = BufReader::new(rh);
        Context::new(rh, wh)
    };

    // send greeting message
    ctx.outgoing(Response::Ready).await?;

    loop {
        match ctx.incoming().await? {
            Request::PutFile { filename, content } => {
                info!(
                    "Requesting to put content[len={}] into file '{}'",
                    content.len(),
                    filename
                );
                let revision = state.put(filename, content);
                ctx.outgoing(Response::FileRevision(revision)).await?;
            }
            Request::GetFile { filename } => {
                let resp = match state.get(filename) {
                    Some(content) => Response::FileContent(content),
                    None => Response::Err(Error::FileNotFound),
                };

                ctx.outgoing(resp).await?;
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
    let listener = TcpListener::bind(addr).await?;
    info!("TCP Server listening on {}", addr);

    let state = State::new();
    loop {
        let state = state.clone();
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                tokio::spawn(async move {
                    info!("Accepting socket from {}", remote_addr);
                    let _ = handle(socket, remote_addr, state).await;
                    info!("Dropping socket {}", remote_addr);
                });
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }

    // let mut upstream = Upstream::connect().await?;

    // upstream
    //     .send(Request::PutFile {
    //         filename: "/foo.txt".to_string(),
    //         content: "Hello, world".to_string().into_bytes(),
    //     })
    //     .await?;
    //

    //
    // upstream.writer.write_all(b"GET /foo.txt\n").await?;
    // let mut buf = String::new();
    // upstream.reader.read_line(&mut buf).await?;
    // info!("Greet from upstream: '{}'", buf);
    //
    // upstream
    //     .send(Request::PutFile {
    //         filename: "/foo.txt".to_string(),
    //         content: "damn you".to_string().into_bytes(),
    //     })
    //     .await?;
    // upstream.writer.write_all(b"GET /foo.txt\n").await?;
    // upstream.writer.write_all(b"GET /baz.txt\n").await?;
    // upstream.writer.write_all(b"GET /baz.txt\n").await?;
    //
    // loop {
    //     let mut buf = String::new();
    //     upstream.reader.read_line(&mut buf).await?;
    //     info!("Received from upstream: '{}'", buf);
    // }
    // Ok(())
}
