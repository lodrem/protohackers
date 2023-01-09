use std::collections::BTreeMap;
use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::tcp;

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

async fn handle(mut socket: TcpStream, _remote_addr: SocketAddr) -> Result<()> {
    let (mut rh, mut wh) = socket.split();
    let mut state = State::new();

    loop {
        let op = rh.read_u8().await?;
        let lhs = rh.read_i32().await?;
        let rhs = rh.read_i32().await?;
        match op {
            b'I' => state.insert(rhs, lhs),
            b'Q' => wh.write_i32(state.query(lhs, rhs)).await?,
            _ => {
                break;
            }
        };
    }

    Ok(())
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    tcp::serve(addr, handle).await
}
