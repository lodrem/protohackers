use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

#[derive(Clone)]
struct Room {
    users: Arc<RwLock<HashMap<String, UnboundedSender<Event>>>>,
}

impl Room {
    pub fn new() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn join(&mut self, name: &str) -> Result<UnboundedReceiver<Event>> {
        let mut users = self.users.write().unwrap();
        if users.contains_key(name) {
            return Err(anyhow!("duplicated username '{}', rejected", name));
        }

        let (tx, rx) = unbounded_channel();

        users
            .iter()
            .for_each(|(_, tx)| tx.send(Event::UserJoined(name.to_owned())).unwrap());
        users.insert(name.to_owned(), tx);

        Ok(rx)
    }

    pub fn leave(&mut self, name: &str) {
        let mut users = self.users.write().unwrap();
        if users.remove(name).is_some() {
            users
                .iter()
                .for_each(|(_, tx)| tx.send(Event::UserLeft(name.to_owned())).unwrap());
        }
    }

    pub fn list(&self) -> Vec<String> {
        self.users
            .read()
            .unwrap()
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    pub async fn broadcast_event(&mut self, e: Event) -> Result<()> {
        let users = self.users.read().unwrap();
        users.iter().for_each(|(_, tx)| tx.send(e.clone()).unwrap());

        Ok(())
    }
}

#[derive(Debug, Clone)]
enum Event {
    UserJoined(String),
    UserLeft(String),
    Incoming(String, String),
}

struct Inbox<W> {
    name: String,
    writer: W,
}

impl<W> Inbox<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(name: String, writer: W) -> Self {
        Self { name, writer }
    }

    pub async fn process_event(&mut self, e: Event) -> Result<()> {
        match e {
            Event::UserJoined(name) if name != self.name => {
                let msg = format!("* {} has entered the room\n", name);
                self.send_message(&msg).await?;
            }
            Event::UserLeft(name) if name != self.name => {
                let msg = format!("* {} has left the room\n", name);
                self.send_message(&msg).await?;
            }
            Event::Incoming(name, message) if name != self.name => {
                let msg = format!("[{}] {}", name, message);
                self.send_message(&msg).await?;
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn presence_notify(&mut self, users: Vec<String>) -> Result<()> {
        let room_info = {
            let users: Vec<String> = users.into_iter().filter(|u| u != &self.name).collect();
            format!("* The room contains: {}\n", users.join(" "))
        };

        self.send_message(&room_info).await?;

        Ok(())
    }

    async fn send_message(&mut self, msg: &str) -> Result<()> {
        self.writer.write_all(msg.as_bytes()).await?;
        Ok(())
    }
}

fn is_valid_name(name: &str) -> bool {
    name.chars().all(char::is_alphanumeric)
}

async fn handle(socket: TcpStream, remote_addr: SocketAddr, mut state: Room) -> Result<()> {
    info!("Accepting socket from {}", remote_addr);

    let (rh, mut wh) = socket.into_split();
    let mut reader = BufReader::new(rh);

    // prompt name
    let name = {
        const GREET_MESSAGE: &'static str = "Welcome to budgetchat! What shall I call you?\n";
        wh.write_all(GREET_MESSAGE.as_bytes()).await?;
        let mut name = String::new();
        reader.read_line(&mut name).await?;
        name.pop();
        name
    };
    if !is_valid_name(&name) {
        wh.write_all("Illegal name".as_bytes()).await?;
        return Ok(());
    }

    let users = state.list();
    let mut outgoing = state.join(&name)?;
    let mut inbox = Inbox::new(name.clone(), wh);

    // handle outgoing message
    tokio::spawn(async move {
        inbox
            .presence_notify(users)
            .await
            .expect("send presence notification");
        while let Some(e) = outgoing.recv().await {
            inbox
                .process_event(e)
                .await
                .expect("process outgoing event");
        }
    });

    // handle incoming message
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) if n > 0 => {
                state
                    .broadcast_event(Event::Incoming(name.to_owned(), line))
                    .await?
            }
            Ok(_) => break,
            Err(e) => {
                error!("Failed to read from user {}: {:?}", name, e);
                break;
            }
        }
    }

    state.leave(&name);
    info!("Dropping socket {}", remote_addr);

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("TCP Server listening on {}", addr);

    let state = Room::new();

    loop {
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                tokio::spawn(handle(socket, remote_addr, state.clone()));
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }
}
