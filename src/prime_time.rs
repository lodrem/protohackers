use std::net::SocketAddr;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::tcp;

const EXPECTED_METHOD: &'static str = "isPrime";

#[derive(Deserialize)]
struct Request {
    pub method: String,
    pub number: f64,
}

impl Request {
    pub fn from_str(payload: &str) -> Result<Self> {
        let data: Self = serde_json::from_str(payload)?;
        Ok(data)
    }
}

#[derive(Serialize)]
struct Response {
    pub method: String,
    pub prime: bool,
}

async fn respond<W>(writer: &mut W, data: Response) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let data = format!(
        "{}\n",
        serde_json::to_string(&data).expect("serialize response")
    );
    info!("Outgoing response: {}", data.trim());
    writer.write_all(data.as_bytes()).await?;
    Ok(())
}

async fn fail<T>(writer: &mut T) -> Result<()>
where
    T: AsyncWrite + Unpin,
{
    respond(
        writer,
        Response {
            method: String::from("invalid"),
            prime: false,
        },
    )
    .await
}

fn is_prime(n: f64) -> bool {
    if n <= 1.0 || n.floor() != n {
        return false;
    }
    let limit = n.sqrt() as u64;
    let n = n as u64;
    for i in 2..=limit {
        if n % i == 0 {
            return false;
        }
    }
    true
}

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr) -> Result<()> {
    let (rh, mut wh) = socket.split();
    let mut reader = BufReader::new(rh);
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;

        info!("Incoming request: {}", line.trim());

        match Request::from_str(&line) {
            Ok(req) if req.method == EXPECTED_METHOD => {
                respond(
                    &mut wh,
                    Response {
                        method: EXPECTED_METHOD.to_string(),
                        prime: is_prime(req.number),
                    },
                )
                .await?;
            }
            Err(e) => {
                error!("Failed to deserialize request {}: {:?}", line.trim(), e);
                fail(&mut wh).await?;
                break;
            }
            _ => {
                error!("Unexpected request: {}", line);
                fail(&mut wh).await?;
                break;
            }
        }
    }

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}
