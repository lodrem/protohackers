use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

enum INodeInfo {
    File { filename: String, revision: u64 },
    Directory { filename: String },
}

impl From<INodeInfo> for Bytes {
    fn from(value: INodeInfo) -> Self {
        match value {
            INodeInfo::File { filename, revision } => {
                Bytes::from(format!("{filename} r{revision}"))
            }
            INodeInfo::Directory { filename } => Bytes::from(format!("{filename}/ DIR")),
        }
    }
}

impl Display for INodeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File { filename, revision } => {
                write!(f, "File[{filename}, revision={revision}]")
            }
            Self::Directory { filename } => write!(f, "Dir[{filename}]"),
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
        path.trim_matches('/')
            .split('/')
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
    Noop,
    Closed,
}

#[derive(Debug)]
enum Error {
    FileNotFound,
    IllegalPath,
    IllegalFileContent,
    InvalidCommand(String),
    PutUsage,
    GetUsage,
    ListUsage,
}

impl From<Error> for Bytes {
    fn from(value: Error) -> Self {
        match value {
            Error::FileNotFound => Bytes::from_static(b"no such file"),
            Error::IllegalPath => Bytes::from_static(b"illegal file name"),
            Error::IllegalFileContent => Bytes::from_static(b"text files only"),
            Error::InvalidCommand(cmd) => Bytes::from(format!("illegal method: {cmd}")),
            Error::PutUsage => Bytes::from_static(b"usage: PUT file size"),
            Error::GetUsage => Bytes::from_static(b"usage: GET file [revision]"),
            Error::ListUsage => Bytes::from_static(b"usage: LIST dir"),
        }
    }
}

enum Response {
    Ready,
    FileContent(Bytes),
    FileRevision(u64),
    Files(Vec<INodeInfo>),
    Help,
    Err(Error),
}

impl From<Response> for Bytes {
    fn from(value: Response) -> Self {
        match value {
            Response::Ready => Bytes::from_static(b"READY\n"),
            Response::FileContent(content) => {
                let mut buf = BytesMut::from(format!("OK {}\n", content.len()).as_bytes());
                buf.put(content);
                buf.put(&b"READY\n"[..]);
                buf.freeze()
            }
            Response::FileRevision(revision) => {
                let mut buf = BytesMut::from(format!("OK r{revision}\n").as_bytes());
                buf.put(&b"READY\n"[..]);
                buf.freeze()
            }
            Response::Files(files) => {
                let mut buf = BytesMut::from(format!("OK {}\n", files.len()).as_bytes());
                for f in files {
                    buf.put::<Bytes>(f.into());
                    buf.put_u8(b'\n');
                }
                buf.put(&b"READY\n"[..]);
                buf.freeze()
            }
            Response::Help => Bytes::from_static(b"OK usage: HELP|GET|PUT|LIST\nREADY\n"),
            Response::Err(e) => {
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
            Self::FileRevision(revision) => write!(f, "FileRevision[{revision}]"),
            Self::Files(files) => write!(f, "Files[len={}]: {}", files.len(), {
                let files: Vec<_> = files.iter().map(|f| f.to_string()).collect();
                files.join(",")
            }),
            Self::Help => write!(f, "HELP"),
            Self::Err(e) => write!(f, "Error[{e:?}]"),
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
                    "PUT" => self.parse_put_command(parts).await?,
                    "GET" => self.parse_get_command(parts).await?,
                    "LIST" => self.parse_list_command(parts).await?,
                    "HELP" => {
                        self.outgoing(Response::Help).await?;
                        Request::Noop
                    }
                    typ => {
                        self.outgoing(Response::Err(Error::InvalidCommand(typ.to_string())))
                            .await?;
                        Request::Noop
                    }
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

    async fn parse_get_command(&mut self, parts: Vec<&str>) -> Result<Request> {
        if 2 <= parts.len() && parts.len() <= 3 {
            let path = parts[1].to_string();
            if !is_valid_path(&path) || path.ends_with('/') {
                self.outgoing(Response::Err(Error::IllegalPath)).await?;
                return Ok(Request::Noop);
            }
            if parts.len() == 3 {
                if let Ok(revision) = &parts[2][1..].parse::<u64>() {
                    return Ok(Request::GetFile {
                        path,
                        revision: Some(*revision),
                    });
                }
            } else {
                return Ok(Request::GetFile {
                    path,
                    revision: None,
                });
            }
        }

        self.outgoing(Response::Err(Error::GetUsage)).await?;
        Ok(Request::Noop)
    }

    async fn parse_put_command(&mut self, parts: Vec<&str>) -> Result<Request> {
        if parts.len() == 3 {
            let path = parts[1].to_string();

            if let Ok(content_len) = parts[2].parse::<usize>() {
                let content = {
                    let mut buf = vec![0; content_len];
                    self.reader.read_exact(&mut buf).await?;
                    Bytes::from(buf)
                };

                if !is_valid_path(&path) || path.ends_with('/') {
                    self.outgoing(Response::Err(Error::IllegalPath)).await?;
                    return Ok(Request::Noop);
                }
                if !is_valid_content(&content) {
                    self.outgoing(Response::Err(Error::IllegalFileContent))
                        .await?;
                    return Ok(Request::Noop);
                }

                return Ok(Request::PutFile { path, content });
            }
        }

        self.outgoing(Response::Err(Error::PutUsage)).await?;
        Ok(Request::Noop)
    }

    async fn parse_list_command(&mut self, parts: Vec<&str>) -> Result<Request> {
        if parts.len() == 2 {
            let path = parts[1].to_string();

            if !is_valid_path(&path) {
                self.outgoing(Response::Err(Error::IllegalPath)).await?;
                return Ok(Request::Noop);
            }

            return Ok(Request::ListDir { path });
        }

        self.outgoing(Response::Err(Error::ListUsage)).await?;
        Ok(Request::Noop)
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
pub fn is_valid_content(content: &[u8]) -> bool {
    content
        .iter()
        .all(|&c| (9..=11).contains(&c) || (32..=127).contains(&c))
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
                let revision = state.put(path, content);
                ctx.outgoing(Response::FileRevision(revision)).await?;
            }
            Request::GetFile { path, revision } => {
                let resp = match state.get(path, revision) {
                    Some(content) => Response::FileContent(content),
                    None => Response::Err(Error::FileNotFound),
                };

                ctx.outgoing(resp).await?;
            }
            Request::ListDir { path } => {
                ctx.outgoing(Response::Files(state.list(path))).await?;
            }
            Request::Noop => {}
            Request::Closed => {
                info!("Closing the connection");
                break;
            }
        }
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
}
