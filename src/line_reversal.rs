use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use tokio::net::UdpSocket;
use tracing::{info, warn};

type SessionId = u64;

const MESSAGE_CONNECT: &'static str = "connect";
const MESSAGE_DATA: &'static str = "data";
const MESSAGE_ACK: &'static str = "ack";
const MESSAGE_CLOSE: &'static str = "close";

fn backtrack_if_escaped(buf: &[u8], pos: usize) -> bool {
    let mut cnt = 0;
    for i in (0..pos).rev() {
        if buf[i] != b'\\' {
            break;
        }
        cnt += 1;
    }
    cnt % 2 == 1
}

fn split(s: &str) -> Vec<String> {
    let buf = s.as_bytes();
    let mut rv = vec![];

    let mut l = 0;
    while l < buf.len() {
        let mut r = l + 1;
        while r < buf.len() {
            if buf[r] == b'/' {
                if backtrack_if_escaped(&buf, r) {
                    r += 1;
                    continue;
                }
                break;
            }
            r += 1;
        }
        rv.push(String::from_utf8_lossy(&buf[l..r]).to_string());
        l = r + 1;
    }
    rv
}

#[derive(Clone)]
enum Message {
    Connect {
        session: SessionId,
    },
    Data {
        session: SessionId,
        position: u64,
        data: String,
    },
    Ack {
        session: SessionId,
        length: u64,
    },
    Close {
        session: SessionId,
    },
}

impl Into<String> for Message {
    fn into(self) -> String {
        let buf = match self {
            Self::Connect { session } => format!("{}/{}", MESSAGE_CONNECT, session),
            Self::Data {
                session,
                position,
                data,
            } => format!(
                "{}/{}/{}/{}",
                MESSAGE_DATA,
                session,
                position,
                data.replace("\\", "\\\\").replace("/", "\\/")
            ),
            Self::Ack { session, length } => format!("{}/{}/{}", MESSAGE_ACK, session, length),
            Self::Close { session } => format!("{}/{}", MESSAGE_CLOSE, session),
        };

        format!("/{}/", buf)
    }
}

impl TryFrom<String> for Message {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !value.starts_with('/') || !value.ends_with('/') {
            return Err(anyhow!("Invalid message format"));
        }

        let parts = split(value.trim_matches('/'));
        let typ = parts.first().ok_or(anyhow!("No message type provided"))?;
        let message = match typ.as_str() {
            MESSAGE_CONNECT if parts.len() == 2 => {
                let session = parts[1].parse::<SessionId>()?;
                Self::Connect { session }
            }
            MESSAGE_ACK if parts.len() == 3 => {
                let session = parts[1].parse::<SessionId>()?;
                let length = parts[2].parse::<u64>()?;
                Self::Ack { session, length }
            }
            MESSAGE_DATA if parts.len() == 4 => {
                let session = parts[1].parse::<SessionId>()?;
                let position = parts[2].parse::<u64>()?;
                let data = parts[3]
                    .to_string()
                    .replace("\\\\", "\\")
                    .replace("\\/", "/");
                Self::Data {
                    session,
                    position,
                    data,
                }
            }
            MESSAGE_CLOSE if parts.len() == 2 => {
                let session = parts[1].parse::<SessionId>()?;
                Self::Close { session }
            }
            typ => return Err(anyhow!("Invalid message type: {}", typ)),
        };
        Ok(message)
    }
}

struct SessionInner {
    socket: Arc<UdpSocket>,
    id: SessionId,
    addr: SocketAddr,
    incoming: BytesMut,
    incoming_ack_pos: u64,
    outgoing: BytesMut,
    outgoing_ack_pos: u64,
}

struct Session {
    inner: Arc<RwLock<SessionInner>>,
}

impl Session {
    pub fn new(socket: Arc<UdpSocket>, id: SessionId, addr: SocketAddr) -> Self {
        Self {
            inner: Arc::new(RwLock::new(SessionInner {
                socket,
                id,
                addr,
                incoming: BytesMut::new(),
                incoming_ack_pos: 0,
                outgoing: BytesMut::new(),
                outgoing_ack_pos: 0,
            })),
        }
    }

    pub fn incoming(&mut self, buf: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.incoming.put_slice(buf);

        // TODO: ack
    }

    pub fn ack(&mut self, pos: u64) -> Result<()> {
        if pos <= self.outgoing_ack_pos {
            // duplicated ack
            Ok(())
        } else if pos == self.outgoing.len() as u64 {
            // update ack pos
            let mut inner = self.inner.write().unwrap();
            inner.outgoing_ack_pos = pos;
            Ok(())
        } else if pos < self.outgoing.len() as u64 {
            // retransmit all payload
            // TODO
            Ok(())
        } else {
            // close session
            Err(anyhow!("Invalid ack position"))
        }
    }

    pub fn close(&mut self) {}

    async fn send_data(&mut self, position: u64, data: String) -> Result<()> {
        let msg = Message::Data {
            session: self.id,
            position,
            data,
        };
        self.socket
            .send_to(msg.clone().into().as_bytes(), self.addr)
            .await?;

        let inner = self.inner.clone();
        // TODO: retransmission if ack timeout or closed
        tokio::spawn(async move {
            use tokio::time::{sleep, Duration};
            sleep(Duration::from_secs(3)).await;

            let inner = inner.write().unwrap();
            if inner.outgoing_ack_pos < position + data.len() {}
        });
        Ok(())
    }
}

async fn run_loop(socket: Arc<UdpSocket>, remote_addr: SocketAddr, message: Message) -> Result<()> {
    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    info!("Binding UDP address: {}", addr);
    let socket = Arc::new(UdpSocket::bind(addr).await?);

    loop {
        let mut buf = [0; 1024];
        let socket = socket.clone();
        let (n, remote_addr) = socket.recv_from(&mut buf).await?;
        match Message::try_from(String::from_utf8_lossy(&buf[0..n]).to_string()) {
            Ok(message) => {
                tokio::spawn(run_loop(socket, remote_addr, message));
            }
            Err(e) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_split() {
        use super::split;

        let cases = [
            ("", vec![]),
            ("data", vec!["data"]),
            ("foo/bar", vec!["foo", "bar"]),
            ("data/0/hello_world", vec!["data", "0", "hello_world"]),
            ("data/0/hello_world\n", vec!["data", "0", "hello_world\n"]),
            ("data/0/\\/", vec!["data", "0", "\\/"]),
            ("data/\\/0/\\/", vec!["data", "\\/0", "\\/"]),
            (
                "foo\\\\\\//\\/bar/hello\\/world",
                vec!["foo\\\\\\/", "\\/bar", "hello\\/world"],
            ),
            (
                "foo\\\\/\\/bar/hello\\/world",
                vec!["foo\\\\", "\\/bar", "hello\\/world"],
            ),
        ];

        for (input, expected) in cases.into_iter() {
            let expected: Vec<String> = expected.into_iter().map(|s| s.to_string()).collect();
            assert_eq!(split(input), expected);
        }
    }
}
