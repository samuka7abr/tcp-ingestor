use std::{env, net::SocketAddr, time::Duration};

use anyhow::{Context, Result, anyhow, bail};

#[derive(Clone, Debug)]
pub struct Config {
    pub tcp_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub mongodb_uri: String,
    pub mongodb_database: String,
    pub mongodb_collection: String,
    pub queue_capacity: usize,
    pub batch_size: usize,
    pub batch_flush_interval: Duration,
    pub read_buffer_bytes: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let batch_flush_ms = parse_env("BATCH_FLUSH_MS", 500_u64)?;
        if batch_flush_ms == 0 {
            bail!("BATCH_FLUSH_MS must be greater than zero");
        }

        Ok(Self {
            tcp_addr: parse_env(
                "TCP_ADDR",
                "0.0.0.0:7000".parse().expect("valid default TCP_ADDR"),
            )?,
            metrics_addr: parse_env(
                "METRICS_ADDR",
                "0.0.0.0:9898".parse().expect("valid default METRICS_ADDR"),
            )?,
            mongodb_uri: env_or("MONGODB_URI", "mongodb://localhost:27017"),
            mongodb_database: env_or("MONGODB_DATABASE", "tcp_ingestor"),
            mongodb_collection: env_or("MONGODB_COLLECTION", "traffic"),
            queue_capacity: positive("QUEUE_CAPACITY", 10_000)?,
            batch_size: positive("BATCH_SIZE", 500)?,
            batch_flush_interval: Duration::from_millis(batch_flush_ms),
            read_buffer_bytes: positive("READ_BUFFER_BYTES", 8 * 1024)?,
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn positive(key: &str, default: usize) -> Result<usize> {
    let value = parse_env(key, default)?;
    if value == 0 {
        bail!("{key} must be greater than zero");
    }
    Ok(value)
}

fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(raw) => raw
            .parse()
            .map_err(|error| anyhow!("invalid value for {key} ({raw}): {error}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("could not read {key}")),
    }
}
