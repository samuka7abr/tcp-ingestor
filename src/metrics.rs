use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub struct AppMetrics {
    registry: Registry,
    pub connections_active: IntGauge,
    pub connections_total: IntCounter,
    pub bytes_received_total: IntCounter,
    pub chunks_received_total: IntCounter,
    pub bytes_persisted_total: IntCounter,
    pub chunks_persisted_total: IntCounter,
    pub chunks_failed_total: IntCounter,
    pub tcp_read_errors_total: IntCounter,
    pub mongo_write_errors_total: IntCounter,
    pub queue_depth: IntGauge,
    pub mongo_up: IntGauge,
    pub batch_size: Histogram,
    pub mongo_write_duration_seconds: Histogram,
}

impl AppMetrics {
    pub fn new() -> Result<Arc<Self>> {
        let registry = Registry::new_custom(Some("tcp_ingestor".to_owned()), None)
            .context("failed to create Prometheus registry")?;

        let metrics = Self {
            registry,
            connections_active: IntGauge::new("connections_active", "Active TCP connections")?,
            connections_total: IntCounter::new("connections_total", "Accepted TCP connections")?,
            bytes_received_total: IntCounter::new(
                "bytes_received_total",
                "Bytes read from TCP connections",
            )?,
            chunks_received_total: IntCounter::new(
                "chunks_received_total",
                "Chunks read from TCP connections",
            )?,
            bytes_persisted_total: IntCounter::new(
                "bytes_persisted_total",
                "Bytes successfully persisted to MongoDB",
            )?,
            chunks_persisted_total: IntCounter::new(
                "chunks_persisted_total",
                "Chunks successfully persisted to MongoDB",
            )?,
            chunks_failed_total: IntCounter::new(
                "chunks_failed_total",
                "Chunks lost after a MongoDB write failure",
            )?,
            tcp_read_errors_total: IntCounter::new(
                "tcp_read_errors_total",
                "TCP socket read errors",
            )?,
            mongo_write_errors_total: IntCounter::new(
                "mongo_write_errors_total",
                "Failed MongoDB batch writes",
            )?,
            queue_depth: IntGauge::new("queue_depth", "Chunks queued or waiting to be written")?,
            mongo_up: IntGauge::new("mongo_up", "Whether the latest MongoDB operation succeeded")?,
            batch_size: Histogram::with_opts(
                HistogramOpts::new("batch_size", "Number of chunks per MongoDB write")
                    .buckets(vec![1.0, 10.0, 50.0, 100.0, 250.0, 500.0, 1_000.0]),
            )?,
            mongo_write_duration_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "mongo_write_duration_seconds",
                    "MongoDB batch write latency in seconds",
                )
                .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
            )?,
        };

        metrics.register_all()?;
        Ok(Arc::new(metrics))
    }

    fn register_all(&self) -> Result<()> {
        macro_rules! register {
            ($metric:expr) => {
                self.registry
                    .register(Box::new($metric.clone()))
                    .context("failed to register Prometheus metric")?;
            };
        }

        register!(self.connections_active);
        register!(self.connections_total);
        register!(self.bytes_received_total);
        register!(self.chunks_received_total);
        register!(self.bytes_persisted_total);
        register!(self.chunks_persisted_total);
        register!(self.chunks_failed_total);
        register!(self.tcp_read_errors_total);
        register!(self.mongo_write_errors_total);
        register!(self.queue_depth);
        register!(self.mongo_up);
        register!(self.batch_size);
        register!(self.mongo_write_duration_seconds);
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut output)
            .context("failed to encode Prometheus metrics")?;
        Ok(output)
    }

    pub fn record_received(&self, bytes: usize) {
        self.bytes_received_total.inc_by(bytes as u64);
        self.chunks_received_total.inc();
    }

    pub fn record_persisted(&self, chunks: usize, bytes: usize, duration: Duration) {
        self.chunks_persisted_total.inc_by(chunks as u64);
        self.bytes_persisted_total.inc_by(bytes as u64);
        self.batch_size.observe(chunks as f64);
        self.mongo_write_duration_seconds
            .observe(duration.as_secs_f64());
        self.mongo_up.set(1);
    }

    pub fn record_write_failure(&self, chunks: usize, duration: Duration) {
        self.mongo_write_errors_total.inc();
        self.chunks_failed_total.inc_by(chunks as u64);
        self.batch_size.observe(chunks as f64);
        self.mongo_write_duration_seconds
            .observe(duration.as_secs_f64());
        self.mongo_up.set(0);
    }
}

pub async fn serve(
    listener: TcpListener,
    metrics: Arc<AppMetrics>,
    cancellation: CancellationToken,
) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(metrics);

    axum::serve(listener, app)
        .with_graceful_shutdown(cancellation.cancelled_owned())
        .await
        .context("metrics HTTP server failed")
}

async fn metrics_handler(State(metrics): State<Arc<AppMetrics>>) -> Response {
    match metrics.encode() {
        Ok(body) => (
            [(header::CONTENT_TYPE, TextEncoder::new().format_type())],
            body,
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, "could not encode metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn health_handler(State(metrics): State<Arc<AppMetrics>>) -> StatusCode {
    if metrics.mongo_up.get() == 1 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_are_namespaced_and_encoded() {
        let metrics = AppMetrics::new().expect("metrics should be created");
        metrics.record_received(42);

        let encoded = String::from_utf8(metrics.encode().expect("metrics should encode"))
            .expect("metrics should be UTF-8");

        assert!(encoded.contains("tcp_ingestor_bytes_received_total 42"));
    }
}
