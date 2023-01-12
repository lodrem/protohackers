use std::cmp;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

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

#[derive(Clone, Debug)]
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

async fn close_session(socket: Arc<UdpSocket>, addr: SocketAddr, session: SessionId) -> Result<()> {
    let data: String = Message::Close { session }.into();
    socket.send_to(data.as_bytes(), addr).await?;
    Ok(())
}

struct Session {
    tx: UnboundedSender<Event>,
    socket: Arc<UdpSocket>,
    id: SessionId,
    addr: SocketAddr,
    incoming: Vec<u8>,
    incoming_sep_pos: usize,
    outgoing: Vec<u8>,
    outgoing_ack_pos: u64,
}

impl Session {
    pub fn new(
        socket: Arc<UdpSocket>,
        tx: UnboundedSender<Event>,
        id: SessionId,
        addr: SocketAddr,
    ) -> Self {
        Self {
            tx,
            socket,
            id,
            addr,
            incoming: Vec::new(),
            incoming_sep_pos: 0,
            outgoing: Vec::new(),
            outgoing_ack_pos: 0,
        }
    }

    pub async fn recv_data(&mut self, pos: u64, buf: &[u8]) -> Result<()> {
        let pos = pos as usize;
        if pos <= self.incoming.len() {
            self.incoming.splice(pos.., buf.to_vec());
        }
        self.send_ack(self.incoming.len() as u64).await?;

        for i in self.incoming_sep_pos..self.incoming.len() {
            if self.incoming[i] == b'\n' {
                let mut data = self.incoming[self.incoming_sep_pos..i].to_vec();
                data.reverse();
                data.push(b'\n');

                self.outgoing.extend(data);
                self.incoming_sep_pos = i + 1;
                break;
            }
        }

        Ok(())
    }

    pub async fn recv_ack(&mut self, pos: u64) -> Result<()> {
        if pos <= self.outgoing_ack_pos {
            // duplicated ack
            Ok(())
        } else if pos == self.outgoing.len() as u64 {
            // update ack pos
            self.outgoing_ack_pos = pos;
            Ok(())
        } else if pos < self.outgoing.len() as u64 {
            // send payload after pos
            self.outgoing_ack_pos = pos;
            self.send_data().await?;
            Ok(())
        } else {
            // close session
            Err(anyhow!("Invalid ack position"))
        }
    }

    pub async fn try_resend_data(&mut self, from_position: u64) -> Result<()> {
        if from_position < self.outgoing_ack_pos {
            return Ok(());
        }

        self.send_data().await?;

        Ok(())
    }

    pub async fn send_close_if_expiry(&mut self, pos: u64) -> Result<bool> {
        if self.outgoing_ack_pos <= pos {
            close_session(self.socket.clone(), self.addr.clone(), self.id).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn send_ack(&mut self, pos: u64) -> Result<()> {
        let data: String = Message::Ack {
            session: self.id,
            length: pos,
        }
        .into();
        self.socket.send_to(data.as_bytes(), self.addr).await?;
        Ok(())
    }

    async fn send_data(&mut self) -> Result<()> {
        let position = self.outgoing_ack_pos;
        if position == self.outgoing.len() as u64 {
            // All data is sent out, skipped.
            return Ok(());
        }
        let (l, r) = {
            let l = position as usize;
            let r = cmp::min(l + 1000, self.outgoing.len() as usize);
            (l, r)
        };
        let data = String::from_utf8_lossy(&self.outgoing[l..r]).to_string();
        let msg = Message::Data {
            session: self.id,
            position,
            data,
        };

        let message: String = msg.clone().into();
        self.socket.send_to(message.as_bytes(), self.addr).await?;

        {
            // Resend data if timeout.
            let session = self.id;
            let tx = self.tx.clone();
            tokio::spawn(async move {
                use tokio::time::{sleep, Duration};
                // Resend data if no ack to the position after 3s.
                sleep(Duration::from_secs(3)).await;
                tx.send(Event::ResendData {
                    session,
                    from_position: position,
                })
                .unwrap();

                // Close session if no ack to the position after 60s.
                sleep(Duration::from_secs(57)).await;
                tx.send(Event::Expire { session, position }).unwrap();
            });
        }

        Ok(())
    }
}

#[derive(Debug)]
enum Event {
    Incoming {
        addr: SocketAddr,
        message: Message,
    },
    ResendData {
        session: SessionId,
        from_position: u64,
    },
    Expire {
        session: SessionId,
        position: u64,
    },
}

async fn run_main_loop(
    socket: Arc<UdpSocket>,
    tx: UnboundedSender<Event>,
    mut rx: UnboundedReceiver<Event>,
) -> Result<()> {
    let mut sessions: HashMap<SessionId, Session> = HashMap::new();

    while let Some(e) = rx.recv().await {
        match e {
            Event::Incoming { addr, message } => match message {
                Message::Connect { session } => {
                    let sess = sessions
                        .entry(session)
                        .or_insert_with(|| Session::new(socket.clone(), tx.clone(), session, addr));
                    sess.send_ack(0).await?;
                }
                Message::Data {
                    session,
                    position,
                    data,
                } => {
                    if let Some(sess) = sessions.get_mut(&session) {
                        sess.recv_data(position, data.as_bytes()).await?;
                    } else {
                        close_session(socket.clone(), addr, session).await?;
                    }
                }
                Message::Ack { session, length } => {
                    if let Some(mut sess) = sessions.remove(&session) {
                        match sess.recv_ack(length).await {
                            Ok(()) => {
                                sessions.insert(session, sess);
                            }
                            Err(_) => close_session(socket.clone(), addr, session).await?,
                        }
                    } else {
                        close_session(socket.clone(), addr, session).await?;
                    }
                }
                Message::Close { session } => {
                    close_session(socket.clone(), addr, session).await?;
                }
            },
            Event::ResendData {
                session,
                from_position,
            } => {
                if let Some(sess) = sessions.get_mut(&session) {
                    sess.try_resend_data(from_position).await?;
                }
            }
            Event::Expire { session, position } => {
                if let Some(mut sess) = sessions.remove(&session) {
                    if !sess.send_close_if_expiry(position).await? {
                        sessions.insert(session, sess);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn run_accept_loop(socket: Arc<UdpSocket>, tx: UnboundedSender<Event>) -> Result<()> {
    loop {
        let mut buf = [0; 1024];
        let socket = socket.clone();
        let (n, remote_addr) = socket.recv_from(&mut buf).await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            match Message::try_from(String::from_utf8_lossy(&buf[0..n]).to_string()) {
                Ok(message) => {
                    tx.send(Event::Incoming {
                        addr: remote_addr,
                        message,
                    })
                    .unwrap();
                }
                Err(e) => {
                    error!(
                        "Failed to parse message from {}, dropped: {:?}",
                        remote_addr, e
                    );
                }
            }
        });
    }
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    info!("Binding UDP address: {}", addr);
    let socket = Arc::new(UdpSocket::bind(addr).await?);

    let (tx, rx) = unbounded_channel();

    tokio::spawn(run_main_loop(socket.clone(), tx.clone(), rx));

    run_accept_loop(socket, tx).await?;

    loop {}
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
