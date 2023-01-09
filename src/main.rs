mod signal;

mod means_to_an_end;
mod prime_time;
mod smock_test;

use std::future::Future;
use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;
use tracing::{error, info, Level};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct App {
    #[arg(short, default_value = "0.0.0.0:8070")]
    addr: String,

    #[arg(short)]
    cmd: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let app: App = App::parse();

    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?;
    rt.block_on(async {
        let addr = app.addr.parse().unwrap();
        tokio::spawn(async move {
            info!("Try to invoke command {}", app.cmd);
            if let Err(e) = match app.cmd.as_str() {
                "smoke_test" => serve_tcp(addr, smock_test::run).await,
                "prime_time" => serve_tcp(addr, prime_time::run).await,
                "means_to_an_end" => serve_tcp(addr, means_to_an_end::run).await,
                c => Err(anyhow!("Invalid command: {}", c)),
            } {
                error!("Failed to run command {}: {:?}", app.cmd, e);
                std::process::exit(1);
            }
        });

        signal::shutdown().await;
    });

    Ok(())
}

async fn serve_tcp<F, Fut>(addr: SocketAddr, mut handle: F) -> Result<()>
where
    F: Send + 'static + FnMut(TcpStream, SocketAddr) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let listener = TcpListener::bind(addr).await?;

    info!("TCP Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                info!("Accepting socket from {}", remote_addr);
                let rv = handle(socket, remote_addr);
                tokio::spawn(async move {
                    if let Err(e) = rv.await {
                        error!("Failed to handle socket from {}: {:?}", remote_addr, e);
                    }
                    info!("Dropping socket {}", remote_addr);
                });
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }
}
