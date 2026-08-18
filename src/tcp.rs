use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use mongodb::bson::DateTime;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{config::Config, metrics::AppMetrics, mongo_writer::IngestRecord};

pub async fn serve(
    listener: TcpListener,
    sender: mpsc::Sender<IngestRecord>,
    config: Config,
    metrics: Arc<AppMetrics>,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut connections = JoinSet::new();
    let mut next_connection_id = 1_u64;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, remote_addr) = accepted.context("failed to accept TCP connection")?;
                let connection_id = next_connection_id;
                next_connection_id = next_connection_id.wrapping_add(1);

                connections.spawn(handle_connection(
                    stream,
                    remote_addr,
                    connection_id,
                    sender.clone(),
                    config.read_buffer_bytes,
                    metrics.clone(),
                    cancellation.clone(),
                ));
            }
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = joined {
                    tracing::error!(%error, "TCP connection task panicked");
                }
            }
        }
    }

    drop(sender);
    while let Some(joined) = connections.join_next().await {
        if let Err(error) = joined {
            tracing::error!(%error, "TCP connection task panicked during shutdown");
        }
    }
    tracing::info!("TCP listener stopped");
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    remote_addr: SocketAddr,
    connection_id: u64,
    sender: mpsc::Sender<IngestRecord>,
    read_buffer_bytes: usize,
    metrics: Arc<AppMetrics>,
    cancellation: CancellationToken,
) {
    let _guard = ConnectionGuard::new(metrics.clone());
    let mut buffer = vec![0_u8; read_buffer_bytes];
    let mut chunk_index = 0_u64;
    tracing::debug!(connection_id, %remote_addr, "TCP connection opened");

    loop {
        let read = tokio::select! {
            _ = cancellation.cancelled() => break,
            read = stream.read(&mut buffer) => read,
        };

        let bytes_read = match read {
            Ok(0) => break,
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                metrics.tcp_read_errors_total.inc();
                tracing::warn!(%error, connection_id, %remote_addr, "TCP read failed");
                break;
            }
        };

        metrics.record_received(bytes_read);
        let record = IngestRecord {
            connection_id,
            chunk_index,
            remote_addr: remote_addr.to_string(),
            received_at: DateTime::now(),
            payload: buffer[..bytes_read].to_vec(),
        };
        chunk_index = chunk_index.wrapping_add(1);

        metrics.queue_depth.inc();
        let queued = tokio::select! {
            _ = cancellation.cancelled() => false,
            result = sender.send(record) => result.is_ok(),
        };
        if !queued {
            metrics.queue_depth.dec();
            break;
        }
    }

    tracing::debug!(connection_id, %remote_addr, "TCP connection closed");
}

struct ConnectionGuard {
    metrics: Arc<AppMetrics>,
}

impl ConnectionGuard {
    fn new(metrics: Arc<AppMetrics>) -> Self {
        metrics.connections_total.inc();
        metrics.connections_active.inc();
        Self { metrics }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.connections_active.dec();
    }
}
