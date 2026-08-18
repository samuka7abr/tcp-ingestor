# TCP Ingestor

Serviço assíncrono em Rust que recebe streams TCP, persiste o tráfego bruto no MongoDB e publica métricas para Prometheus. A stack local já inclui um dashboard Grafana provisionado para acompanhar picos de ingestão.

## Arquitetura

```text
clientes TCP ──> listener Rust ──> fila limitada ──> writer em lote ──> MongoDB
                       │                                  │
                       └──────── métricas ────────────────┴──> Prometheus ──> Grafana
```

O TCP é um stream e não possui fronteiras de mensagem. Por isso, cada leitura de até `READ_BUFFER_BYTES` bytes é armazenada como um `chunk`; aplicações que precisem reconstruir um protocolo devem usar `connection_id` e `chunk_index`.

Cada documento em `tcp_ingestor.traffic` contém:

- `connection_id`: identificador local da conexão;
- `chunk_index`: posição do chunk dentro da conexão, começando em zero;
- `remote_addr`: IP e porta do cliente;
- `received_at`: instante da leitura;
- `size_bytes`: tamanho do chunk;
- `payload`: bytes brutos em BSON Binary.

## Subir tudo

Requer Docker com Compose:

```bash
docker compose up --build -d
docker compose ps
```

Serviços disponíveis:

- TCP: `localhost:7000`
- métricas/health: `http://localhost:9898/metrics` e `/health`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)

O dashboard **TCP Ingestor** aparece automaticamente na pasta de mesmo nome.

> **Hosts com kernel Linux 6.19 a 7.0.13:** o MongoDB detecta uma incompatibilidade do TCMalloc e interrompe a inicialização. A solução suportada é usar kernel 7.0.14+ ou um kernel anterior à faixa afetada; consulte as [notas oficiais de produção](https://www.mongodb.com/docs/manual/administration/production-notes/). A variável de Compose `MONGODB_IMAGE` permite testar outro tag, mas não corrige a incompatibilidade do host.

## Gerar tráfego de teste

Envie 50 MiB aleatórios:

```bash
head -c 50M /dev/urandom | nc localhost 7000
```

Confira a persistência e as métricas:

```bash
docker compose exec mongodb mongosh tcp_ingestor --quiet --eval 'db.traffic.countDocuments()'
curl http://localhost:9898/metrics
```

## Rodar o binário localmente

Com um MongoDB acessível em `localhost:27017`:

```bash
cargo run
```

Copie `.env.example` como referência para as configurações. O processo lê variáveis de ambiente diretamente; ele não carrega `.env` sozinho.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `TCP_ADDR` | `0.0.0.0:7000` | endereço do listener TCP |
| `METRICS_ADDR` | `0.0.0.0:9898` | endereço HTTP de métricas e health |
| `MONGODB_URI` | `mongodb://localhost:27017` | URI do MongoDB |
| `MONGODB_DATABASE` | `tcp_ingestor` | database de destino |
| `MONGODB_COLLECTION` | `traffic` | collection de destino |
| `QUEUE_CAPACITY` | `10000` | limite da fila com backpressure |
| `BATCH_SIZE` | `500` | máximo de chunks por escrita |
| `BATCH_FLUSH_MS` | `500` | tempo máximo até descarregar um lote |
| `READ_BUFFER_BYTES` | `8192` | tamanho máximo de cada leitura/chunk |
| `RUST_LOG` | `info` | filtro de logs do `tracing` |

## Métricas principais

- `tcp_ingestor_bytes_received_total`: bytes lidos da rede;
- `tcp_ingestor_bytes_persisted_total`: bytes confirmados pelo MongoDB;
- `tcp_ingestor_connections_active`: conexões atuais;
- `tcp_ingestor_queue_depth`: pressão na fila;
- `tcp_ingestor_mongo_write_duration_seconds`: latência dos lotes;
- `tcp_ingestor_chunks_failed_total`: chunks descartados por falha de escrita;
- `tcp_ingestor_mongo_up`: resultado da operação MongoDB mais recente.

O dashboard calcula throughput com `rate(...bytes_received_total) * 8`, exibindo bits por segundo e permitindo enxergar os picos em uma janela de scrape de 2 segundos.

## Garantias deste primeiro corte

A fila aplica backpressure quando o MongoDB não acompanha a entrada, e o desligamento via `Ctrl+C` drena os dados já enfileirados. Uma falha de `insert_many` é contabilizada, mas o lote é descartado; confirmação ao cliente, retry persistente e dead-letter queue ficam fora deste init e são os próximos passos naturais caso a ingestão precise de garantia *at-least-once*.

## Qualidade

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
