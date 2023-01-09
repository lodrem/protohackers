mod signal;
mod tcp;

mod budget_chat;
mod means_to_an_end;
mod prime_time;
mod smock_test;

use anyhow::{anyhow, Result};
use clap::Parser;
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
                "smoke_test" => smock_test::run(addr).await,
                "prime_time" => prime_time::run(addr).await,
                "means_to_an_end" => means_to_an_end::run(addr).await,
                "budget_chat" => budget_chat::run(addr).await,
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
