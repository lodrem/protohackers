mod signal;

use std::net::SocketAddr;

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::runtime;
use tracing::{error, info, Level};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()?;
    rt.block_on(async {
        let addr = "0.0.0.0:8070".parse().unwrap();
        tokio::spawn(serve_tcp(addr));

        signal::shutdown().await;
    });

    Ok(())
}

async fn serve_tcp(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    info!("TCP Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, remote_addr)) => {
                info!("Accepting socket from {}", remote_addr);
                // tokio::spawn(handle_echoserver(socket, remote_addr));
                tokio::spawn(prime_time::handle(socket, remote_addr));
            }
            Err(e) => {
                error!("Failed to accept socket: {:?}", e);
            }
        }
    }
}

// 0. Smoke test
mod smock_test {
    use std::net::SocketAddr;

    use anyhow::Result;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tracing::{error, info};

    async fn handle(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
        let mut buf = [0; 1024];

        while let Ok(n) = socket.read(&mut buf).await {
            if n == 0 {
                break;
            }

            if let Err(e) = socket.write_all(&buf[0..n]).await {
                error!("Failed to write to socket: {:?}", e);
                break;
            }
        }

        info!("Dropping connection {}", remote_addr);

        Ok(())
    }
}

// 1. prime time
mod prime_time {
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
        pub number: i128,
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

    fn is_prime(n: i128) -> bool {
        if n <= 1 {
            return false;
        }
        for i in 2..n {
            if n % i == 0 {
                return false;
            }
        }
        true
    }

    pub async fn handle(mut socket: TcpStream, remote_addr: SocketAddr) -> Result<()> {
        let (rh, mut wh) = socket.split();
        let mut reader = BufReader::new(rh);
        loop {
            let mut line = String::new();

            if let Err(e) = reader.read_line(&mut line).await {
                error!("Failed to read line from socket: {:?}", e);
                break;
            }

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
                    error!("Failed to deserialize request {}: {:?}", line, e);
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
}
