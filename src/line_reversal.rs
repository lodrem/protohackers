use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info, warn};

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

type Activity = (Instant, SessionId, u64);

struct ActivityMarkerInner {
    resend_queue: VecDeque<Activity>,
    expiry_queue: VecDeque<Activity>,
}

#[derive(Clone)]
struct ActivityMarker {
    inner: Arc<Mutex<ActivityMarkerInner>>,
}

impl ActivityMarker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ActivityMarkerInner {
                resend_queue: VecDeque::new(),
                expiry_queue: VecDeque::new(),
            })),
        }
    }

    pub fn mark(&mut self, session: SessionId, when: Instant, position: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.resend_queue.push_back((when, session, position));
        inner.expiry_queue.push_back((when, session, position));
    }

    pub fn next(&mut self) -> (Vec<Activity>, Vec<Activity>) {
        let resend_d = Duration::from_secs(1);
        let expiry_d = Duration::from_secs(60);
        let now = Instant::now();
        let mut resend = vec![];
        let mut expiry = vec![];
        let mut inner = self.inner.lock().unwrap();
        while let Some(item) = inner.resend_queue.pop_front() {
            if now.duration_since(item.0) >= resend_d {
                resend.push(item);
            } else {
                inner.resend_queue.push_front(item);
                break;
            }
        }
        while let Some(item) = inner.expiry_queue.pop_front() {
            if now.duration_since(item.0) >= expiry_d {
                expiry.push(item);
            } else {
                inner.expiry_queue.push_front(item);
                break;
            }
        }

        (resend, expiry)
    }
}

async fn close_session(socket: Arc<UdpSocket>, addr: SocketAddr, session: SessionId) -> Result<()> {
    info!("{} <- Server: CLOSE", session);
    let data: String = Message::Close { session }.into();
    socket.send_to(data.as_bytes(), addr).await?;
    Ok(())
}

struct Session {
    socket: Arc<UdpSocket>,
    id: SessionId,
    addr: SocketAddr,
    incoming: BytesMut,
    incoming_base_idx: usize,
    outgoing: BytesMut,
    outgoing_ack_pos: u64,
    last_active_at: Instant,
    activity_marker: ActivityMarker,
}

impl Session {
    pub fn new(
        socket: Arc<UdpSocket>,
        id: SessionId,
        addr: SocketAddr,
        marker: ActivityMarker,
    ) -> Self {
        Self {
            socket,
            id,
            addr,
            incoming: BytesMut::new(),
            incoming_base_idx: 0,
            outgoing: BytesMut::new(),
            outgoing_ack_pos: 0,
            last_active_at: Instant::now(),
            activity_marker: marker,
        }
    }

    pub async fn recv_data(&mut self, pos: u64, buf: &[u8]) -> Result<()> {
        if self.incoming_ack_pos() != pos {
            self.send_ack().await?;
            return Ok(());
        }

        self.incoming.extend_from_slice(buf);

        let (mut l, r) = (0, self.incoming.len());
        let mut i = l;

        while i < r {
            if self.incoming[i] == b'\n' {
                let mut data = self.incoming[l..i].to_vec();
                data.reverse();
                data.push(b'\n');

                self.outgoing.extend(data);
                l = i + 1; // skip newline
            }
            i += 1;
        }
        if 0 < l {
            self.incoming_base_idx += l;
            self.incoming.advance(l);
        }
        self.send_ack().await?;
        self.send_data().await?;

        Ok(())
    }

    pub async fn recv_ack(&mut self, pos: u64) -> Result<()> {
        if pos < self.outgoing_ack_pos {
            // duplicated ack
            Ok(())
        } else if pos <= self.outgoing.len() as u64 {
            // send payload after pos
            self.last_active_at = Instant::now();
            self.outgoing_ack_pos = pos;
            self.send_data().await?;
            Ok(())
        } else {
            // close session
            Err(anyhow!("Invalid ack position"))
        }
    }

    pub async fn try_resend_data(&mut self, from_position: u64) -> Result<()> {
        if from_position < self.outgoing_ack_pos
            || self.last_active_at.elapsed() <= Duration::from_secs(3)
        {
            return Ok(());
        }
        warn!("{} <- Server: DATA retry as timeout", self.id);

        self.send_data().await?;

        Ok(())
    }

    pub async fn send_close_if_expiry(&mut self, pos: u64) -> Result<bool> {
        if self.outgoing_ack_pos <= pos {
            warn!("{} <- Server: CLOSE as session expiry", self.id);
            close_session(self.socket.clone(), self.addr.clone(), self.id).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn incoming_ack_pos(&self) -> u64 {
        (self.incoming.len() + self.incoming_base_idx) as u64
    }

    pub async fn send_ack(&mut self) -> Result<()> {
        self.send_ack_with_pos(self.incoming_ack_pos()).await
    }

    pub async fn send_ack_with_pos(&mut self, pos: u64) -> Result<()> {
        info!("{} <- Server: ACK {}", self.id, pos);
        let data: String = Message::Ack {
            session: self.id,
            length: pos,
        }
        .into();
        self.socket.send_to(data.as_bytes(), self.addr).await?;
        Ok(())
    }

    async fn send_data(&mut self) -> Result<()> {
        self.last_active_at = Instant::now();
        let position = self.outgoing_ack_pos;
        if position == self.outgoing.len() as u64 {
            info!("{} <- Server: all data had been sent out, skipped", self.id);
            // All data is sent out, skipped.
            return Ok(());
        }
        let (l, r) = {
            let l = position as usize;
            let r = cmp::min(l + 900, self.outgoing.len() as usize);
            (l, r)
        };
        let data = String::from_utf8_lossy(&self.outgoing[l..r]).to_string();
        info!(
            "{} <- Server: at {} DATA '{}'",
            self.id,
            position,
            data.replace('\n', "<NL>")
        );
        let msg = Message::Data {
            session: self.id,
            position,
            data,
        };

        let message: String = msg.clone().into();
        self.socket.send_to(message.as_bytes(), self.addr).await?;

        self.activity_marker.mark(self.id, Instant::now(), position);

        Ok(())
    }
}

#[derive(Debug)]
enum Event {
    Incoming {
        addr: SocketAddr,
        message: Message,
    },
    Timeout {
        resend_activities: Vec<Activity>,
        expiry_activities: Vec<Activity>,
    },
}

async fn run_main_loop(
    socket: Arc<UdpSocket>,
    tx: UnboundedSender<Event>,
    mut rx: UnboundedReceiver<Event>,
) -> Result<()> {
    info!("Running main loop");
    let mut sessions: HashMap<SessionId, Session> = HashMap::new();
    let marker = ActivityMarker::new();

    {
        let mut marker = marker.clone();
        tokio::spawn(async move {
            use tokio::time::sleep;
            loop {
                sleep(Duration::from_secs(1)).await;

                let (resend, expiry) = marker.next();

                if resend.is_empty() && expiry.is_empty() {
                    continue;
                }

                tx.send(Event::Timeout {
                    resend_activities: resend,
                    expiry_activities: expiry,
                })
                .unwrap();
            }
        });
    }

    while let Some(e) = rx.recv().await {
        match e {
            Event::Incoming { addr, message } => match message {
                Message::Connect { session } => {
                    info!("{} -> Server: CONNECT as {}", session, session);
                    let sess = sessions.entry(session).or_insert_with(|| {
                        Session::new(socket.clone(), session, addr, marker.clone())
                    });
                    sess.send_ack_with_pos(0).await?;
                }
                Message::Data {
                    session,
                    position,
                    data,
                } => {
                    info!(
                        "{} -> Server: at {} DATA '{}'",
                        session,
                        position,
                        data.replace('\n', "<NL>")
                    );
                    if let Some(sess) = sessions.get_mut(&session) {
                        sess.recv_data(position, data.as_bytes()).await?;
                    } else {
                        close_session(socket.clone(), addr, session).await?;
                    }
                }
                Message::Ack { session, length } => {
                    info!("{} -> Server: ACK {}", session, length);
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
                    info!("{} -> Server: CLOSE", session);
                    close_session(socket.clone(), addr, session).await?;
                    sessions.remove(&session);
                }
            },
            Event::Timeout {
                resend_activities,
                expiry_activities,
            } => {
                for (_, session, position) in resend_activities {
                    if let Some(sess) = sessions.get_mut(&session) {
                        sess.try_resend_data(position).await?;
                    }
                }

                for (_, session, position) in expiry_activities {
                    if let Some(mut sess) = sessions.remove(&session) {
                        if !sess.send_close_if_expiry(position).await? {
                            sessions.insert(session, sess);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn run_accept_loop(socket: Arc<UdpSocket>, tx: UnboundedSender<Event>) -> Result<()> {
    info!("Running accepting loop");
    loop {
        let mut buf = [0; 1024];
        let socket = socket.clone();
        let (n, remote_addr) = socket.recv_from(&mut buf).await?;
        let tx = tx.clone();
        if n > 1000 {
            warn!("Packet ({}) too large, should not be greater than 1000", n);
            continue;
        }
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
