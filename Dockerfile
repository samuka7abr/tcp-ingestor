FROM rust:1.93.0-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 ingestor

COPY --from=builder /app/target/release/tcp-ingestor /usr/local/bin/tcp-ingestor

USER ingestor
EXPOSE 7000 9898
ENTRYPOINT ["tcp-ingestor"]
