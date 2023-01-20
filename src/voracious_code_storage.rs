use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

enum INodeInfo {
    File { filename: String, revision: u64 },
    Directory { filename: String },
}

impl Into<Bytes> for INodeInfo {
    fn into(self) -> Bytes {
        match self {
            Self::File { filename, revision } => Bytes::from(format!("{} r{}", filename, revision)),
            Self::Directory { filename } => Bytes::from(format!("{}/ DIR", filename)),
        }
    }
}

impl Display for INodeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File { filename, revision } => {
                write!(f, "File[{}, revision={}]", filename, revision)
            }
            Self::Directory { filename } => write!(f, "Dir[{}]", filename),
        }
    }
}

struct INode {
    revisions: Vec<Bytes>,
    children: HashMap<String, INode>, // An Inode could be both directory and file
}

impl INode {
    pub fn dir() -> Self {
        Self {
            revisions: Vec::new(),
            children: HashMap::new(),
        }
    }

    pub fn write_file(&mut self, path: String, content: Bytes) -> Result<u64> {
        let (dirs, filename) = Self::split_path_with_filename(path);

        let dir = self.mkdir(dirs)?;
        let f = dir.children.entry(filename).or_insert_with(|| INode {
            revisions: vec![content.clone()],
            children: HashMap::new(),
        });
        if let Some(latest_content) = f.revisions.last() {
            if *latest_content != content {
                f.revisions.push(content);
            }
        } else {
            f.revisions.push(content);
        }
        Ok(f.revisions.len() as u64)
    }

    #[inline]
    pub fn get_file(&self, path: String, revision: Option<u64>) -> Option<Bytes> {
        let (dirs, filename) = Self::split_path_with_filename(path);
        self.cd_dir(dirs)
            .ok()?
            .children
            .get(&filename)
            .and_then(|f| {
                let revision = revision.unwrap_or(f.revisions.len() as u64) as usize;
                if f.revisions.is_empty() || revision == 0 {
                    None
                } else if revision <= f.revisions.len() {
                    Some(f.revisions[revision - 1].clone())
                } else {
                    None
                }
            })
    }

    #[inline]
    pub fn list_dir(&self, path: String) -> Vec<INodeInfo> {
        let dirs = Self::split_path(path);
        match self.cd_dir(dirs) {
            Ok(d) => {
                let mut rv: Vec<_> = d
                    .children
                    .iter()
                    .map(|(filename, inode)| {
                        if inode.children.is_empty() {
                            INodeInfo::File {
                                filename: filename.clone(),
                                revision: inode.revisions.len() as u64,
                            }
                        } else {
                            INodeInfo::Directory {
                                filename: filename.clone(),
                            }
                        }
                    })
                    .collect();

                rv.sort_by_key(|f| match f {
                    INodeInfo::File { filename, .. } => filename.clone(),
                    INodeInfo::Directory { filename } => filename.clone(),
                });

                rv
            }
            Err(_) => {
                vec![]
            }
        }
    }

    #[inline]
    fn mkdir(&mut self, dirs: Vec<String>) -> Result<&mut Self> {
        let mut dir = self;
        for d in dirs {
            dir = dir.children.entry(d).or_insert_with(Self::dir);
        }

        Ok(dir)
    }

    #[inline]
    fn cd_dir(&self, dirs: Vec<String>) -> Result<&Self> {
        let mut dir = self;
        for d in dirs {
            match dir.children.get(&d) {
                Some(next) => dir = next,
                None => bail!("'{}' dir doesn't exist", d),
            }
        }

        Ok(dir)
    }

    #[inline]
    fn split_path(path: String) -> Vec<String> {
        let parts: Vec<_> = path.trim_matches('/').split('/').collect();
        parts
            .into_iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    #[inline]
    fn split_path_with_filename(path: String) -> (Vec<String>, String) {
        let mut dirs = Self::split_path(path);
        let filename = dirs.pop().unwrap();
        (dirs, filename)
    }
}

#[derive(Clone)]
struct State {
    inode: Arc<RwLock<INode>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            inode: Arc::new(RwLock::new(INode::dir())),
        }
    }

    pub fn put(&mut self, path: String, content: Bytes) -> u64 {
        let mut root = self.inode.write().unwrap();
        root.write_file(path, content).expect("write to file")
    }

    pub fn get(&mut self, path: String, revision: Option<u64>) -> Option<Bytes> {
        let root = self.inode.read().unwrap();
        root.get_file(path, revision)
    }

    pub fn list(&mut self, path: String) -> Vec<INodeInfo> {
        let root = self.inode.read().unwrap();
        root.list_dir(path)
    }
}

enum Request {
    PutFile { path: String, content: Bytes },
    GetFile { path: String, revision: Option<u64> },
    ListDir { path: String },
    Unsupported { command: String },
    Closed,
}

#[derive(Debug)]
enum Error {
    FileNotFound,
    IllegalPath,
    IllegalFileContent,
    InvalidCommand(String),
}

impl Into<Bytes> for Error {
    fn into(self) -> Bytes {
        match self {
            Self::FileNotFound => Bytes::from_static(b"no such file"),
            Self::IllegalPath => Bytes::from_static(b"illegal file name"),
            Self::IllegalFileContent => Bytes::from_static(b"text files only"),
            Self::InvalidCommand(cmd) => Bytes::from(format!("illegal method: {}", cmd)),
        }
    }
}

enum Response {
    Ready,
    FileContent(Bytes),
    FileRevision(u64),
    Files(Vec<INodeInfo>),
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
            Self::Files(files) => {
                let mut buf = BytesMut::from(format!("OK {}\n", files.len()).as_bytes());
                for f in files {
                    buf.put::<Bytes>(f.into());
                    buf.put_u8(b'\n');
                }
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

impl Display for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready => write!(f, "Response::Ready"),
            Self::FileContent(content) => write!(f, "FileContent[len={}]", content.len()),
            Self::FileRevision(revision) => write!(f, "FileRevision[{}]", revision),
            Self::Files(files) => write!(f, "Files[len={}]: {}", files.len(), {
                let files: Vec<_> = files.iter().map(|f| format!("{}", f)).collect();
                files.join(",")
            }),
            Self::Err(e) => write!(f, "Error[{:?}]", e),
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
                info!("-> Server: '{}'", buf);
                let parts: Vec<_> = buf.split(' ').collect();
                match parts[0].to_uppercase().as_str() {
                    "PUT" => {
                        let content_len = parts[2].parse::<usize>()?;
                        let mut content = vec![0; content_len];
                        self.reader.read_exact(&mut content).await?;
                        Request::PutFile {
                            path: parts[1].to_string(),
                            content: Bytes::from(content),
                        }
                    }
                    "GET" => {
                        let revision = if parts.len() == 3 {
                            Some(parts[2].trim_start_matches('r').parse::<u64>()?)
                        } else {
                            None
                        };
                        Request::GetFile {
                            path: parts[1].to_string(),
                            revision,
                        }
                    }
                    "LIST" => Request::ListDir {
                        path: parts[1].to_string(),
                    },
                    typ => Request::Unsupported {
                        command: typ.to_string(),
                    },
                }
            }
        };
        Ok(req)
    }

    pub async fn outgoing(&mut self, response: Response) -> Result<()> {
        info!("<- Server: {}", response);
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
            Request::PutFile {
                path: filename,
                content,
            } => {
                let mut data = format!("PUT {} {}\n", filename, content.len()).into_bytes();
                data.extend(content);
                info!("-> Upstream: {}", unsafe {
                    String::from_utf8_unchecked(data.clone()).replace('\n', "<NL>")
                });
                self.writer.write_all(&data).await?;

                let mut resp = String::new();
                self.reader.read_line(&mut resp).await?;
                info!("<- Upstream: {}", resp);
                let mut resp = String::new();
                self.reader.read_line(&mut resp).await?;
                info!("<- Upstream: {}", resp);
                Response::Ready
            }
            _ => Response::Ready,
        };
        Ok(resp)
    }
}

#[inline]
pub fn is_valid_path(path: &str) -> bool {
    if path.contains("//") || !path.starts_with('/') {
        false
    } else {
        path.chars()
            .all(|c| c == '.' || c == '/' || c == '-' || c == '_' || char::is_alphanumeric(c))
    }
}

#[inline]
pub fn is_valid_content(content: Bytes) -> bool {
    content
        .into_iter()
        .map(char::from)
        .all(|c| char::is_ascii(&c))
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
            Request::PutFile { path, content } => {
                if !is_valid_path(&path) {
                    ctx.outgoing(Response::Err(Error::IllegalPath)).await?;
                } else if !is_valid_content(content.clone()) {
                    ctx.outgoing(Response::Err(Error::IllegalFileContent))
                        .await?;
                } else {
                    let revision = state.put(path, content);
                    ctx.outgoing(Response::FileRevision(revision)).await?;
                }
            }
            Request::GetFile { path, revision } => {
                if !is_valid_path(&path) {
                    ctx.outgoing(Response::Err(Error::IllegalPath)).await?;
                } else {
                    let resp = match state.get(path, revision) {
                        Some(content) => Response::FileContent(content),
                        None => Response::Err(Error::FileNotFound),
                    };

                    ctx.outgoing(resp).await?;
                }
            }
            Request::ListDir { path } => {
                if !is_valid_path(&path) {
                    ctx.outgoing(Response::Err(Error::IllegalPath)).await?;
                } else {
                    ctx.outgoing(Response::Files(state.list(path))).await?;
                }
            }
            Request::Unsupported { command } => {
                ctx.outgoing(Response::Err(Error::InvalidCommand(command)))
                    .await?;
            }
            Request::Closed => {
                info!("Closing the connection");
                break;
            }
        }
    }

    Ok(())
}

async fn test_upstream() -> Result<()> {
    let mut upstream = Upstream::connect().await?;

    {
        let mut buf = String::new();
        upstream.reader.read_line(&mut buf).await?;
        info!("Greet from upstream: '{}'", buf);
    }

    upstream
        .writer
        .write_all(b"PUT /not-a-filename 0\n")
        .await?;

    loop {
        let mut buf = String::new();
        upstream.reader.read_line(&mut buf).await?;
        info!("-> Server: {}", buf.replace('\n', "<NL>"));
    }

    bail!("foobar");
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    // test_upstream().await?;
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
}
