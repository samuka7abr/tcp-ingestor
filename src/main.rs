mod config;
mod metrics;
mod mongo_writer;
mod tcp;

use anyhow::{Context, Result};
use config::Config;
use metrics::AppMetrics;
use mongodb::{Client, bson::doc, options::ClientOptions};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = Config::from_env().context("invalid configuration")?;
    let metrics = AppMetrics::new()?;

    let mut mongo_options = ClientOptions::parse(&config.mongodb_uri)
        .await
        .context("failed to parse MONGODB_URI")?;
    mongo_options.app_name = Some("tcp-ingestor".to_owned());
    let mongo = Client::with_options(mongo_options).context("failed to create MongoDB client")?;
    mongo
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .context("MongoDB is unavailable")?;
    metrics.mongo_up.set(1);

    let collection = mongo
        .database(&config.mongodb_database)
        .collection(&config.mongodb_collection);
    let tcp_listener = TcpListener::bind(config.tcp_addr)
        .await
        .with_context(|| format!("failed to bind TCP listener at {}", config.tcp_addr))?;
    let metrics_listener = TcpListener::bind(config.metrics_addr)
        .await
        .with_context(|| format!("failed to bind metrics server at {}", config.metrics_addr))?;

    tracing::info!(
        tcp_addr = %config.tcp_addr,
        metrics_addr = %config.metrics_addr,
        database = %config.mongodb_database,
        collection = %config.mongodb_collection,
        "tcp-ingestor started"
    );

    let cancellation = CancellationToken::new();
    let (sender, receiver) = mpsc::channel(config.queue_capacity);

    let writer = tokio::spawn(mongo_writer::run(
        collection,
        receiver,
        config.clone(),
        metrics.clone(),
    ));
    let metrics_server = tokio::spawn(metrics::serve(
        metrics_listener,
        metrics.clone(),
        cancellation.clone(),
    ));
    let tcp_server = tokio::spawn(tcp::serve(
        tcp_listener,
        sender,
        config,
        metrics,
        cancellation.clone(),
    ));

    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for Ctrl+C")?;
    tracing::info!("shutdown signal received");
    cancellation.cancel();

    tcp_server.await.context("TCP listener task panicked")??;
    writer.await.context("MongoDB writer task panicked")?;
    metrics_server
        .await
        .context("metrics server task panicked")??;
    tracing::info!("tcp-ingestor stopped");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
