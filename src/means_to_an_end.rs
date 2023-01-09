use std::collections::BTreeMap;
use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

struct State {
    m: BTreeMap<i32, i32>,
}

impl State {
    pub fn new() -> Self {
        Self { m: BTreeMap::new() }
    }

    pub fn insert(&mut self, price: i32, timestamp: i32) {
        self.m.insert(timestamp, price);
    }

    pub fn query(&self, from: i32, end: i32) -> i32 {
        if from > end {
            return 0;
        }
        let prices = self.m.range(from..=end);
        let cnt = prices.clone().count() as i128;
        let sum: i128 = prices.map(|(_, &v)| v as i128).sum();

        if cnt == 0 {
            0
        } else {
            (sum / cnt) as i32
        }
    }
}

pub async fn run(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
    let (mut rh, mut wh) = socket.split();
    let mut state = State::new();

    loop {
        let op = rh.read_u8().await?;
        let lhs = rh.read_i32().await?;
        let rhs = rh.read_i32().await?;
        match op {
            b'I' => {
                info!("Inserting price {} at {} from {}", lhs, rhs, remote_addr);
                state.insert(lhs, rhs);
            }
            b'Q' => {
                info!(
                    "Querying price from {} to {} from {}",
                    lhs, rhs, remote_addr
                );
                let rv = state.query(lhs, rhs);
                info!("Writing query result {} to {}", rv, remote_addr);
                wh.write_i32(rv).await?;
            }
            _ => {
                warn!("Unknown operation: {}", op);
                break;
            }
        };
    }

    info!("Dropping connection {}", remote_addr);

    Ok(())
}
