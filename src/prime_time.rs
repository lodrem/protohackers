use std::net::SocketAddr;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info};

const EXPECTED_METHOD: &'static str = "isPrime";

#[derive(Deserialize)]
struct Request {
    pub method: String,
    pub number: isize,
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

async fn respond<W>(writer: &mut W, data: Response)
where
    W: AsyncWrite + Unpin,
{
    let data = format!(
        "{}\n",
        serde_json::to_string(&data).expect("serialize response")
    );
    info!("Outgoing response: {}", data.trim());
    if let Err(e) = writer.write_all(data.as_bytes()).await {
        error!("Failed to respond data: {:?}", e);
    }
}

async fn fail<T>(writer: &mut T)
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
    .await;
}

fn is_prime(n: isize) -> bool {
    if n <= 1 {
        return false;
    }
    for i in 2..=(n as f64).sqrt() as isize {
        if n % i == 0 {
            return false;
        }
    }
    true
}

pub async fn run(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
    let (rh, mut wh) = socket.split();
    let mut reader = BufReader::new(rh);
    loop {
        let mut line = String::new();

        if let Err(e) = reader.read_line(&mut line).await {
            error!("Failed to read line from socket: {:?}", e);
            break;
        }

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
                .await
            }
            Err(e) => {
                error!("Failed to deserialize request {}: {:?}", line.trim(), e);
                fail(&mut wh).await;
                break;
            }
            _ => {
                error!("Unexpected request: {}", line);
                fail(&mut wh).await;
                break;
            }
        }
    }

    info!("Dropping connection {}", remote_addr);

    Ok(())
}
