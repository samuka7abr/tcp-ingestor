use std::{sync::Arc, time::Instant};

use mongodb::{
    Collection,
    bson::{Binary, Bson, DateTime, Document, doc, spec::BinarySubtype},
};
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{config::Config, metrics::AppMetrics};

#[derive(Debug)]
pub struct IngestRecord {
    pub connection_id: u64,
    pub chunk_index: u64,
    pub remote_addr: String,
    pub received_at: DateTime,
    pub payload: Vec<u8>,
}

pub async fn run(
    collection: Collection<Document>,
    mut receiver: mpsc::Receiver<IngestRecord>,
    config: Config,
    metrics: Arc<AppMetrics>,
) {
    let mut batch = Vec::with_capacity(config.batch_size);
    let mut flush_timer = tokio::time::interval(config.batch_flush_interval);
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            record = receiver.recv() => {
                match record {
                    Some(record) => {
                        metrics.queue_depth.dec();
                        batch.push(record);
                        if batch.len() >= config.batch_size {
                            flush(&collection, &mut batch, &metrics).await;
                        }
                    }
                    None => {
                        flush(&collection, &mut batch, &metrics).await;
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush(&collection, &mut batch, &metrics).await;
            }
        }
    }

    tracing::info!("MongoDB writer stopped");
}

async fn flush(
    collection: &Collection<Document>,
    batch: &mut Vec<IngestRecord>,
    metrics: &AppMetrics,
) {
    if batch.is_empty() {
        return;
    }

    let records = std::mem::take(batch);
    let chunks = records.len();
    let bytes = records.iter().map(|record| record.payload.len()).sum();
    let documents = records.into_iter().map(to_document).collect::<Vec<_>>();
    let started_at = Instant::now();

    match collection.insert_many(documents).await {
        Ok(_) => metrics.record_persisted(chunks, bytes, started_at.elapsed()),
        Err(error) => {
            metrics.record_write_failure(chunks, started_at.elapsed());
            tracing::error!(%error, chunks, bytes, "MongoDB batch write failed; batch discarded");
        }
    }
}

fn to_document(record: IngestRecord) -> Document {
    let size_bytes = record.payload.len() as i64;
    doc! {
        "connection_id": record.connection_id as i64,
        "chunk_index": record.chunk_index as i64,
        "remote_addr": record.remote_addr,
        "received_at": record.received_at,
        "size_bytes": size_bytes,
        "payload": Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: record.payload,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_becomes_binary_bson_document() {
        let document = to_document(IngestRecord {
            connection_id: 7,
            chunk_index: 3,
            remote_addr: "127.0.0.1:1234".to_owned(),
            received_at: DateTime::from_millis(123),
            payload: vec![0, 159, 146, 150],
        });

        assert_eq!(document.get_i64("connection_id"), Ok(7));
        assert_eq!(document.get_i64("chunk_index"), Ok(3));
        assert_eq!(document.get_i64("size_bytes"), Ok(4));
        assert!(document.get_binary_generic("payload").is_ok());
    }
}
