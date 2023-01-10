use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{error, info};

#[derive(Clone)]
struct Job {
    pub id: u64,
    pub queue: String,
    pub priority: u64,
    pub detail: serde_json::Value,
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Job {}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

type WatcherTx = watch::Sender<()>;
type WatcherRx = watch::Receiver<()>;

struct Inner {
    watcher_tx: WatcherTx,
    watcher_rx: WatcherRx,
    queues: HashMap<String, BinaryHeap<Job>>,
    id_generator: u64,
    living_jobs: HashMap<u64, Job>,
}
impl Inner {
    fn next_job_id(&mut self) -> u64 {
        let id = self.id_generator;
        self.id_generator += 1;
        id
    }
}

#[derive(Clone)]
struct State {
    inner: Arc<Mutex<Inner>>,
}

impl State {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(());
        Self {
            inner: Arc::new(Mutex::new(Inner {
                watcher_tx: tx,
                watcher_rx: rx,
                queues: HashMap::new(),
                id_generator: 1,
                living_jobs: HashMap::new(),
            })),
        }
    }

    pub fn put_job(&mut self, queue: String, priority: u64, detail: serde_json::Value) -> Job {
        let mut inner = self.inner.lock().unwrap();
        let job = Job {
            id: inner.next_job_id(),
            queue: queue.clone(),
            priority,
            detail,
        };

        let q = {
            if !inner.queues.contains_key(&queue) {
                inner.queues.insert(queue.clone(), BinaryHeap::new());
            }
            inner.queues.get_mut(&queue).unwrap()
        };

        q.push(job.clone());
        inner.living_jobs.insert(job.id, job.clone());
        // notify watcher
        inner.watcher_tx.send(()).unwrap();

        job
    }

    pub fn get_job(&mut self, queues: &[String]) -> Option<Job> {
        let mut inner = self.inner.lock().unwrap();

        let mut candidates = BinaryHeap::new();
        for q in queues.iter() {
            if let Some(queue) = inner.queues.get(q) {
                if let Some(job) = queue.peek() {
                    candidates.push(job.clone());
                }
            }
        }

        match candidates.pop() {
            Some(job) => {
                inner.queues.get_mut(&job.queue).unwrap().pop().unwrap();
                Some(job)
            }
            None => None,
        }
    }

    pub fn abort_job(&mut self, job: Job) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.living_jobs.contains_key(&job.id) {
            return Err(anyhow!("Job doesn't exist"));
        }

        inner.queues.get_mut(&job.queue).unwrap().push(job);
        // notify watcher
        inner.watcher_tx.send(()).unwrap();
        Ok(())
    }

    pub fn delete_job(&mut self, id: u64) -> Option<Job> {
        let mut inner = self.inner.lock().unwrap();
        match inner.living_jobs.remove(&id) {
            Some(job) => {
                // remove from queue
                let q = inner.queues.remove(&job.queue).unwrap();
                inner.queues.insert(
                    job.queue.clone(),
                    q.into_iter().filter(|job| job.id != id).collect(),
                );
                Some(job)
            }
            _ => None,
        }
    }

    pub fn watch(&mut self) -> WatcherRx {
        self.inner.lock().unwrap().watcher_rx.clone()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "lowercase")]
enum Request {
    Put {
        queue: String,
        #[serde(rename = "pri")]
        priority: u64,
        job: serde_json::Value,
    },
    Get {
        queues: Vec<String>,
        #[serde(default)]
        wait: bool,
    },
    Delete {
        id: u64,
    },
    Abort {
        id: u64,
    },
    Closed,
}

impl Request {
    pub fn from_str(payload: &str) -> Result<Self> {
        let data: Self = serde_json::from_str(payload)?;
        Ok(data)
    }
}

struct WorkingQueue {
    state: State,
    jobs: HashMap<u64, Job>,
}

impl WorkingQueue {
    pub fn new(state: State) -> Self {
        Self {
            state,
            jobs: HashMap::new(),
        }
    }

    pub fn insert(&mut self, job: Job) {
        self.jobs.insert(job.id, job);
    }

    pub fn abort(&mut self, id: u64) -> Result<Job> {
        match self.jobs.remove(&id) {
            Some(job) => {
                self.state.abort_job(job.clone())?;
                Ok(job)
            }
            _ => Err(anyhow!("Job hasn't been assigned to the client")),
        }
    }
}

impl Drop for WorkingQueue {
    fn drop(&mut self) {
        self.jobs.iter().for_each(|(_, job)| {
            let _ = self.state.abort_job(job.clone());
        });
    }
}

struct Context<W, R> {
    reader: R,
    writer: W,
}

impl<W, R> Context<W, R>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + AsyncBufReadExt + Unpin,
{
    pub fn new(w: W, r: R) -> Self {
        Self {
            reader: r,
            writer: w,
        }
    }

    pub async fn incoming_request(&mut self) -> Result<Request> {
        let mut line = String::new();
        match self.reader.read_line(&mut line).await {
            Ok(n) if n == 0 => Ok(Request::Closed),
            Ok(_) => Request::from_str(&line),
            Err(e) => Err(anyhow!("Failed to read from socket: {:?}", e)),
        }
    }

    pub async fn respond(&mut self, data: serde_json::Value) -> Result<()> {
        let mut resp = data.to_string();
        resp.push('\n');
        self.writer.write_all(resp.as_bytes()).await?;
        Ok(())
    }

    pub async fn ok(&mut self, id: u64) -> Result<()> {
        self.respond(json!({
            "status": "ok",
            "id": id,
        }))
        .await
    }

    pub async fn no_job(&mut self) -> Result<()> {
        self.respond(json!({
            "status": "no-job",
        }))
        .await
    }
}

async fn handle(mut socket: TcpStream, remote_addr: SocketAddr, mut state: State) -> Result<()> {
    info!("Accepting socket from {}", remote_addr);

    let mut ctx = {
        let (rh, wh) = socket.split();
        let reader = BufReader::new(rh);
        Context::new(wh, reader)
    };
    let mut working_jobs = WorkingQueue::new(state.clone());
    let mut watcher = state.watch();

    loop {
        match ctx.incoming_request().await {
            Ok(Request::Get { queues, wait }) => loop {
                match state.get_job(&queues) {
                    Some(job) => {
                        working_jobs.insert(job.clone());
                        ctx.respond(json!({
                            "status": "ok",
                            "id": job.id,
                            "job": job.detail,
                            "pri": job.priority,
                            "queue": job.queue,
                        }))
                        .await?;
                        break;
                    }
                    _ if !wait => {
                        ctx.no_job().await?;
                        break;
                    }
                    _ => {
                        watcher.changed().await?;
                        continue;
                    }
                }
            },
            Ok(Request::Put {
                queue,
                priority,
                job,
            }) => {
                let job = state.put_job(queue, priority, job);
                ctx.ok(job.id).await?;
            }
            Ok(Request::Abort { id }) => match working_jobs.abort(id) {
                Ok(_) => ctx.ok(id).await?,
                _ => ctx.no_job().await?,
            },
            Ok(Request::Delete { id }) => match state.delete_job(id) {
                Some(_) => ctx.ok(id).await?,
                None => ctx.no_job().await?,
            },
            Ok(Request::Closed) => {
                info!("Client closed the connection");
                break;
            }
            Err(e) => {
                error!("Failed to get incoming request: {:?}", e);
                ctx.respond(json!({
                    "status": "error",
                    "message": format!("Unknown error occurred: {:?}", e),
                }))
                .await?;
            }
        }
    }

    info!("Dropping socket {}", remote_addr);

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("TCP Server listening on {}", addr);

    let state = State::new();

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
