use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use tokio::net::UdpSocket;
use tracing::{info, warn};

pub async fn run(addr: SocketAddr) -> Result<()> {
    info!("Binding UDP address: {}", addr);
    let socket = Arc::new(UdpSocket::bind(addr).await?);
    let kv = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let mut buf = [0; 1024];
        let socket = socket.clone();
        let (n, remote_addr) = socket.recv_from(&mut buf).await?;

        let kv = kv.clone();
        tokio::spawn(async move {
            let req = String::from_utf8_lossy(&buf[0..n]).to_string();
            match req.split_once('=') {
                Some((k, v)) => {
                    info!("Inserting key-value pair: {} = {}", k, v);
                    kv.write().unwrap().insert(k.to_owned(), v.to_owned());
                }
                None if &req == "version" => {
                    const VERSION: &str = "version=Lodrem's Key-Value Store 1.0";
                    if let Err(e) = socket.send_to(VERSION.as_bytes(), remote_addr).await {
                        warn!("Failed to send response to {}: {:?}", remote_addr, e);
                    }
                }
                None => {
                    info!("Querying key-value pair: {}", req);
                    let resp = {
                        let v = kv.read().unwrap().get(&req).cloned().unwrap_or_default();
                        format!("{req}={v}")
                    };

                    if let Err(e) = socket.send_to(resp.as_bytes(), remote_addr).await {
                        warn!("Failed to send response to {}: {:?}", remote_addr, e);
                    }
                }
            }
        });
    }
}
