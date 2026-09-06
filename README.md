# TCP Ingestor <img src="https://raw.githubusercontent.com/TheZoq2/ferris/master/rustacean-flat-happy.svg" height="40"/>

**Async Rust service that receives TCP streams, persists raw traffic to MongoDB, and publishes metrics to Prometheus.** The local stack already includes a provisioned Grafana dashboard to track ingestion spikes.

---

### Tech Stack

**Language**
<p>
  <img src="https://skillicons.dev/icons?i=rust,mongodb,prometheus,grafana,docker" height="36"/>
</p>

---

## Architecture

```
TCP clients ──> Rust listener ──> bounded queue ──> batch writer ──> MongoDB
                       │                                  │
                       └──────── metrics ─────────────────┴──> Prometheus ──> Grafana
```

TCP is a stream and has no message boundaries. Because of that, each read of up to `READ_BUFFER_BYTES` bytes is stored as a `chunk`; applications that need to reconstruct a protocol should use `connection_id` and `chunk_index`.

Each document in `tcp_ingestor.traffic` contains:

- `connection_id`: local connection identifier;
- `chunk_index`: chunk position within the connection, starting at zero;
- `remote_addr`: client IP and port;
- `received_at`: timestamp of the read;
- `size_bytes`: chunk size;
- `payload`: raw bytes as BSON Binary.

## Bring everything up

Requires Docker with Compose:

```
docker compose up --build -d
docker compose ps
```

Available services:

- TCP: `localhost:7000`
- metrics/health: `http://localhost:9898/metrics` and `/health`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)

The **TCP Ingestor** dashboard appears automatically in the folder of the same name.

> **Hosts running Linux kernel 6.19 to 7.0.13:** MongoDB detects a TCMalloc incompatibility and aborts startup. The supported fix is to use kernel 7.0.14+ or a kernel prior to the affected range; see the [official production notes](https://www.mongodb.com/docs/manual/administration/production-notes/). The Compose variable `MONGODB_IMAGE` lets you test another tag, but it does not fix the host incompatibility.

## Generate test traffic

Send 50 MiB of random data:

```
head -c 50M /dev/urandom | nc localhost 7000
```

Check persistence and metrics:

```
docker compose exec mongodb mongosh tcp_ingestor --quiet --eval 'db.traffic.countDocuments()'
curl http://localhost:9898/metrics
```

## Run the binary locally

With a MongoDB instance reachable at `localhost:27017`:

```
cargo run
```

Copy `.env.example` as a reference for the settings. The process reads environment variables directly; it does not load `.env` on its own.

| Variable             | Default                       | Description                            |
| --------------------- | ---------------------------- | ------------------------------------ |
| `TCP_ADDR`            | `0.0.0.0:7000`                | TCP listener address             |
| `METRICS_ADDR`        | `0.0.0.0:9898`                | HTTP address for metrics and health   |
| `MONGODB_URI`         | `mongodb://localhost:27017`   | MongoDB URI                       |
| `MONGODB_DATABASE`    | `tcp_ingestor`                | target database                       |
| `MONGODB_COLLECTION`  | `traffic`                     | target collection                       |
| `QUEUE_CAPACITY`      | `10000`                       | queue limit with backpressure      |
| `BATCH_SIZE`          | `500`                         | maximum chunks per write         |
| `BATCH_FLUSH_MS`      | `500`                         | maximum time before flushing a batch |
| `READ_BUFFER_BYTES`   | `8192`                        | maximum size of each read/chunk |
| `RUST_LOG`            | `info`                        | `tracing` log filter          |

## Key metrics

- `tcp_ingestor_bytes_received_total`: bytes read from the network;
- `tcp_ingestor_bytes_persisted_total`: bytes confirmed by MongoDB;
- `tcp_ingestor_connections_active`: current connections;
- `tcp_ingestor_queue_depth`: queue pressure;
- `tcp_ingestor_mongo_write_duration_seconds`: batch latency;
- `tcp_ingestor_chunks_failed_total`: chunks dropped due to write failure;
- `tcp_ingestor_mongo_up`: result of the most recent MongoDB operation.

The dashboard computes throughput with `rate(...bytes_received_total) * 8`, showing bits per second and making spikes visible within a 2-second scrape window.

## Guarantees of this first cut

The queue applies backpressure when MongoDB can't keep up with the input, and shutdown via `Ctrl+C` drains the data already queued. An `insert_many` failure is counted, but the batch is dropped; client acknowledgment, persistent retry, and a dead-letter queue are out of scope for this initial cut and are the natural next steps if ingestion needs an *at-least-once* guarantee.

## Quality

```
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

---

<p align="center">Made with 🦀 and coffee by <a href="https://github.com/samuka7abr">@samuka7abr</a></p>
