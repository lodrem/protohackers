use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

pub async fn shutdown() {
    tokio::select! {
        () = recv_signal_and_shutdown(SignalKind::interrupt()) => {}
        () = recv_signal_and_shutdown(SignalKind::terminate()) => {}
    };

    info!("recv signal and shutting down");
}

async fn recv_signal_and_shutdown(kind: SignalKind) {
    signal(kind).expect("register signal handler").recv().await;
}
