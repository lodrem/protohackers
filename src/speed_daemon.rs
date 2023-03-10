use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::{error, info};

const INCOMING_PLATE: u8 = 0x20;
const INCOMING_HEARTBEAT_REQUEST: u8 = 0x40;
const OUTGOING_ERROR: u8 = 0x10;
const OUTGOING_TICKET: u8 = 0x21;
const OUTGOING_HEARTBEAT: u8 = 0x41;
const ROLE_CAMERA: u8 = 0x80;
const ROLE_DISPATCHER: u8 = 0x81;

#[derive(Clone, Debug)]
struct Ticket {
    plate: String,
    road: u16,
    from_mile: u16,
    from_timestamp: u32,
    to_mile: u16,
    to_timestamp: u32,
    speed: u16,
}

#[derive(Clone, Debug)]
enum Role {
    Camera { road: u16, mile: u16, limit: u16 },
    Dispatcher { roads: Vec<u16> },
}

enum Incoming {
    Role(Role),
    Plate { plate: String, timestamp: u32 },
    HeartbeatRequest { interval: u32 },
}

#[derive(Debug)]
enum Outgoing {
    WithRole(Role),
    Error { message: String },
    Ticket(Ticket),
    Heartbeat,
}

impl From<Outgoing> for Bytes {
    fn from(value: Outgoing) -> Self {
        match value {
            Outgoing::Error { message } => {
                let mut buf = BytesMut::new();
                buf.put_u8(OUTGOING_ERROR);
                let s = message.into_bytes();
                buf.put_u8(s.len() as u8);
                buf.put_slice(&s);
                buf.freeze()
            }
            Outgoing::Heartbeat => Bytes::from_static(&[OUTGOING_HEARTBEAT]),
            Outgoing::Ticket(Ticket {
                plate,
                road,
                from_mile,
                from_timestamp,
                to_mile,
                to_timestamp,
                speed,
            }) => {
                let mut buf = BytesMut::new();
                buf.put_u8(OUTGOING_TICKET);
                {
                    let s = plate.into_bytes();
                    buf.put_u8(s.len() as u8);
                    buf.put_slice(&s);
                }
                buf.put_u16(road);
                buf.put_u16(from_mile);
                buf.put_u32(from_timestamp);
                buf.put_u16(to_mile);
                buf.put_u32(to_timestamp);
                buf.put_u16(speed);
                buf.freeze()
            }
            _ => Bytes::new(),
        }
    }
}

struct OutgoingProcessor<W> {
    writer: W,
}

impl<W> OutgoingProcessor<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(w: W) -> Self {
        Self { writer: w }
    }

    pub async fn error(&mut self, e: anyhow::Error) -> Result<()> {
        self.send(Outgoing::Error {
            message: format!("Unexpected error: {e:?}"),
        })
        .await
    }

    pub async fn heartbeat(&mut self) -> Result<()> {
        self.send(Outgoing::Heartbeat).await
    }

    pub async fn send(&mut self, outgoing: Outgoing) -> Result<()> {
        let data: Bytes = outgoing.into();
        self.writer.write_all(data.as_ref()).await?;

        Ok(())
    }
}

struct IncomingProcessor<R> {
    reader: R,
}

impl<R> IncomingProcessor<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(r: R) -> Self {
        Self { reader: r }
    }

    pub async fn next(&mut self) -> Result<Incoming> {
        match self.reader.read_u8().await? {
            ROLE_CAMERA => {
                let road = self.reader.read_u16().await?;
                let mile = self.reader.read_u16().await?;
                let limit = self.reader.read_u16().await?;
                Ok(Incoming::Role(Role::Camera { road, mile, limit }))
            }
            ROLE_DISPATCHER => {
                let num_roads = self.reader.read_u8().await? as usize;
                let mut roads = Vec::with_capacity(num_roads);
                for _ in 0..num_roads {
                    roads.push(self.reader.read_u16().await?);
                }
                Ok(Incoming::Role(Role::Dispatcher { roads }))
            }
            INCOMING_HEARTBEAT_REQUEST => {
                let interval = self.reader.read_u32().await?;
                Ok(Incoming::HeartbeatRequest { interval })
            }
            INCOMING_PLATE => {
                let plate = {
                    let len = self.reader.read_u8().await? as usize;
                    let mut buf = vec![0; len];
                    self.reader.read_exact(&mut buf).await?;
                    String::from_utf8_lossy(&buf).to_string()
                };

                let timestamp = self.reader.read_u32().await?;
                Ok(Incoming::Plate { plate, timestamp })
            }
            typ => Err(anyhow!("Invalid incoming message type: {}", typ)),
        }
    }
}

#[derive(Debug, Clone)]
enum Event {
    RegisterCamera {
        id: String,
        road: u16,
        mile: u16,
        limit: u16,
        tx: UnboundedSender<Outgoing>,
    },
    UnregisterCamera {
        id: String,
    },
    RegisterDispatcher {
        id: String,
        roads: Vec<u16>,
        tx: UnboundedSender<Outgoing>,
    },
    UnregisterDispatcher {
        id: String,
    },
    ObservePlate {
        road: u16,
        mile: u16,
        plate: String,
        timestamp: u32,
    },
}

#[derive(Clone)]
struct Channel {
    tx: UnboundedSender<Event>,
}

impl Channel {
    pub fn new(tx: UnboundedSender<Event>) -> Self {
        Self { tx }
    }
    pub fn register_camera(
        &mut self,
        id: String,
        tx: UnboundedSender<Outgoing>,
        road: u16,
        mile: u16,
        limit: u16,
    ) -> Result<()> {
        self.tx.send(Event::RegisterCamera {
            id,
            road,
            mile,
            limit,
            tx,
        })?;
        Ok(())
    }
    pub fn unregister_camera(&mut self, id: String) -> Result<()> {
        self.tx.send(Event::UnregisterCamera { id })?;
        Ok(())
    }
    pub fn register_dispatcher(
        &mut self,
        id: String,
        tx: UnboundedSender<Outgoing>,
        roads: Vec<u16>,
    ) -> Result<()> {
        self.tx.send(Event::RegisterDispatcher { id, roads, tx })?;
        Ok(())
    }
    pub fn unregister_dispatcher(&mut self, id: String) -> Result<()> {
        self.tx.send(Event::UnregisterDispatcher { id })?;
        Ok(())
    }
    pub fn observe_plate(
        &mut self,
        road: u16,
        mile: u16,
        plate: String,
        timestamp: u32,
    ) -> Result<()> {
        self.tx.send(Event::ObservePlate {
            road,
            mile,
            plate,
            timestamp,
        })?;
        Ok(())
    }
}

async fn run_outgoing_loop<W>(
    mut rx: UnboundedReceiver<Outgoing>,
    mut outgoing: OutgoingProcessor<W>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut role: Option<Role> = None;

    while let Some(event) = rx.recv().await {
        match event {
            Outgoing::WithRole(r) => {
                role = Some(r);
            }
            Outgoing::Ticket(ticket) => match role {
                Some(Role::Dispatcher { .. }) => {
                    outgoing.send(Outgoing::Ticket(ticket)).await?;
                }
                _ => panic!("Should send ticket to dispatcher."),
            },
            Outgoing::Heartbeat => outgoing.heartbeat().await?,
            Outgoing::Error { message } => {
                outgoing
                    .error(anyhow!("Unexpected error: {}", message))
                    .await?;
                break;
            }
        }
    }

    Ok(())
}

async fn run_heartbeat_loop(interval: Duration, tx: UnboundedSender<Outgoing>) -> Result<()> {
    if interval.is_zero() {
        return Ok(());
    }
    use tokio::time::sleep;
    loop {
        tx.send(Outgoing::Heartbeat)?;
        sleep(interval).await;
    }
}

struct HeartbeatLoop {
    handle: Option<JoinHandle<Result<()>>>,
}

impl HeartbeatLoop {
    pub fn new() -> Self {
        Self { handle: None }
    }

    pub fn reset(&mut self, interval: Duration, tx: UnboundedSender<Outgoing>) {
        if let Some(l) = self.handle.take() {
            l.abort();
        }

        if !interval.is_zero() {
            self.handle = Some(tokio::spawn(run_heartbeat_loop(interval, tx)));
        }
    }
}

async fn handle(socket: TcpStream, remote_addr: SocketAddr, mut channel: Channel) -> Result<()> {
    let id = remote_addr.to_string();
    let (mut incoming, outgoing) = {
        let (rh, wh) = socket.into_split();
        (IncomingProcessor::new(rh), OutgoingProcessor::new(wh))
    };

    let mut heartbeat_loop = HeartbeatLoop::new();
    let mut role: Option<Role> = None;
    let (tx, rx) = unbounded_channel();

    tokio::spawn(run_outgoing_loop(rx, outgoing));

    loop {
        match incoming.next().await {
            Ok(Incoming::Role(r)) => {
                if role.is_some() {
                    // Role had been already assigned.
                    tx.send(Outgoing::Error {
                        message: "Should receive plate from camera".to_string(),
                    })?;
                    break;
                }
                role = Some(r.clone());

                match r.clone() {
                    Role::Dispatcher { roads } => {
                        info!("{} -> server: set role as Dispatcher", remote_addr);
                        channel.register_dispatcher(id.clone(), tx.clone(), roads)?;
                        tx.send(Outgoing::WithRole(r))?;
                    }
                    Role::Camera { road, mile, limit } => {
                        info!("{} -> server: set role as Camera", remote_addr);
                        channel.register_camera(id.clone(), tx.clone(), road, mile, limit)?;
                        tx.send(Outgoing::WithRole(r))?;
                    }
                }
            }
            Ok(Incoming::HeartbeatRequest { interval }) => {
                info!(
                    "{} -> server: request heartbeat with interval {}",
                    remote_addr, interval
                );
                let interval = Duration::from_millis(interval as u64 * 100);
                heartbeat_loop.reset(interval, tx.clone());
            }
            Ok(Incoming::Plate { plate, timestamp }) => match role {
                Some(Role::Camera { road, mile, .. }) => {
                    info!(
                        "{} -> server: observe plate {} in {}/{} at {}",
                        remote_addr, plate, road, mile, timestamp
                    );
                    channel.observe_plate(road, mile, plate, timestamp)?
                }
                _ => {
                    tx.send(Outgoing::Error {
                        message: "Should receive plate from camera".to_string(),
                    })?;
                    break;
                }
            },
            Err(e) => {
                tx.send(Outgoing::Error {
                    message: format!("Unexpected error: {e:?}"),
                })?;
                break;
            }
        }
    }

    match role {
        Some(Role::Dispatcher { .. }) => channel.unregister_dispatcher(id)?,
        Some(Role::Camera { .. }) => channel.unregister_camera(id)?,
        None => {}
    }
    Ok(())
}

async fn run_main_loop(mut rx: UnboundedReceiver<Event>) -> Result<()> {
    let mut dispatchers = HashMap::new();
    let mut cameras = HashMap::new();
    let mut plates: HashMap<(String, u16), Vec<(u16, u32)>> = HashMap::new();
    let mut tickets: HashMap<String, HashSet<u32>> = HashMap::new();
    let mut roads = HashMap::new();
    let mut road_to_dispatchers: HashMap<u16, HashSet<String>> = HashMap::new();
    let mut pending_tickets: VecDeque<Ticket> = VecDeque::new();

    while let Some(e) = rx.recv().await {
        match e {
            Event::RegisterDispatcher { id, roads, tx } => {
                dispatchers.insert(id.clone(), (roads.clone(), tx.clone()));
                for road in roads.iter() {
                    road_to_dispatchers
                        .entry(*road)
                        .or_default()
                        .insert(id.clone());
                }
                pending_tickets.retain(|ticket| {
                    if roads.contains(&ticket.road) {
                        info!(
                            "server -> {}: dispatch ticket for plate {} in {}",
                            id, ticket.plate, ticket.road
                        );
                        tx.send(Outgoing::Ticket(ticket.clone())).unwrap();
                        true
                    } else {
                        false
                    }
                });
            }
            Event::UnregisterDispatcher { id } => {
                if let Some((roads, _tx)) = dispatchers.remove(&id) {
                    for road in roads {
                        road_to_dispatchers.get_mut(&road).unwrap().remove(&id);
                    }
                }
            }
            Event::RegisterCamera {
                id,
                road,
                mile,
                limit,
                tx,
            } => {
                cameras.insert(id, (road, mile, limit, tx));
                roads.insert(road, limit);
            }
            Event::UnregisterCamera { id } => {
                cameras.remove(&id);
            }
            Event::ObservePlate {
                road,
                mile,
                plate,
                timestamp,
            } => {
                let dates = tickets.entry(plate.clone()).or_default();
                let miles = plates.entry((plate.clone(), road)).or_default();
                let limit = *roads.get(&road).unwrap();

                let d1 = (timestamp as f64 / 86400.0).floor() as u32;
                if !dates.contains(&d1) {
                    for (m2, t2) in miles.iter() {
                        let d2 = (*t2 as f64 / 86400.0).floor() as u32;
                        if dates.contains(&d2) {
                            info!(
                                "plate {} already mark ticket at {}, skip comparing this date",
                                plate, d2
                            );
                            continue;
                        }
                        let distance = mile.abs_diff(*m2);
                        let interval = timestamp.abs_diff(*t2);
                        let speed = (distance as f64 / (interval as f64 / 60.0 / 60.0)).round();
                        if (speed - limit as f64) < 0.5 {
                            info!(
                                "plate {} on road {} with speed {} and {}, no ticket",
                                plate, road, speed, limit,
                            );
                            continue;
                        }
                        let ((d1, t1, m1), (d2, t2, m2)) = if timestamp < *t2 {
                            ((d1, timestamp, mile), (d2, *t2, *m2))
                        } else {
                            ((d2, *t2, *m2), (d1, timestamp, mile))
                        };
                        for d in d1..=d2 {
                            info!("plate {} mark ticket at {}", plate, d);
                            dates.insert(d);
                        }
                        let ticket = Ticket {
                            plate: plate.clone(),
                            road,
                            from_mile: m1,
                            from_timestamp: t1,
                            to_mile: m2,
                            to_timestamp: t2,
                            speed: (speed * 100.0).floor() as u16,
                        };
                        let ds = road_to_dispatchers.entry(road).or_default();
                        if let Some(id) = ds.iter().next().cloned() {
                            info!(
                                "server -> {}: dispatch ticket for {} on {}",
                                id, plate, road
                            );
                            let (_, tx) = dispatchers.get(&id).unwrap();
                            tx.send(Outgoing::Ticket(ticket)).unwrap();
                        } else {
                            info!("no available dispatcher for {} on {}", plate, road);
                            // No available dispatchers
                            pending_tickets.push_back(ticket);
                        }
                    }
                }

                miles.push((mile, timestamp));
                miles.sort_by(|(_, t1), (_, t2)| t1.cmp(t2));
            }
        }
    }

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("TCP Server listening on {}", addr);

    let (tx, rx) = unbounded_channel();
    let channel = Channel::new(tx.clone());

    tokio::spawn(run_main_loop(rx));

    loop {
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                let channel = channel.clone();
                tokio::spawn(async move {
                    info!("Accepting socket from {}", remote_addr);
                    let _ = handle(socket, remote_addr, channel).await;
                    info!("Dropping socket {}", remote_addr);
                });
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }
}
