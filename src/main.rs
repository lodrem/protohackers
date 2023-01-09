use tokio::runtime;
use tracing::{info, Level};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?;
    rt.block_on(async {
        info!("Starting server");
    });

    Ok(())
}
