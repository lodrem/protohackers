use std::collections::BTreeMap;
use std::net::SocketAddr;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info};

pub async fn run(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
    let (mut rh, mut wh) = socket.split();
    let mut m = BTreeMap::new();

    loop {
        let msg_type = rh.read_u8().await?;
        match msg_type {
            b'I' => {
                let timestamp = rh.read_i32().await?;
                let price = rh.read_i32().await?;
                info!(
                    "Inserting price {} at {} from {}",
                    price, timestamp, remote_addr
                );
                m.insert(timestamp, price);
            }
            b'Q' => {
                let min_time = rh.read_i32().await?;
                let max_time = rh.read_i32().await?;
                info!(
                    "Querying price from {} to {} from {}",
                    min_time, max_time, remote_addr
                );
                let mut rv = 0.0;
                let mut cnt = 0;
                for (_, &v) in m.range(min_time..=max_time) {
                    rv += v as f64;
                    cnt += 1;
                }
                let rv = if cnt == 0 {
                    0
                } else {
                    (rv / cnt as f64) as i32
                };
                info!("Writing query result {} to {}", rv, remote_addr);
                if let Err(e) = wh.write_i32(rv).await {
                    error!("Failed to write query result: {:?}", e);
                    break;
                }
            }
            _ => {}
        };
    }

    info!("Dropping connection {}", remote_addr);

    Ok(())
}
