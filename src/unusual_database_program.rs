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
            let req = {
                let mut req = String::from_utf8_lossy(&buf[0..n]).to_string();
                req.pop();
                req
            };
            match req.split_once('=') {
                Some((k, v)) => {
                    info!("Inserting key-value pair: {} = {}", k, v);
                    kv.write().unwrap().insert(k.to_owned(), v.to_owned());
                }
                None => {
                    let resp = {
                        let v = kv.read().unwrap().get(&req).cloned().unwrap_or_default();
                        format!("{}={}", req, v)
                    };

                    if let Err(e) = socket.send_to(resp.as_bytes(), remote_addr).await {
                        warn!("Failed to send response to {}: {:?}", remote_addr, e);
                    }
                }
            }
        });
    }
}
