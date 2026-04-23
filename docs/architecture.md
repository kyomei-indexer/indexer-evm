# Architecture

`indexer-evm` is a single Rust binary that pulls blocks from an EVM source, decodes logs (and optionally traces) against user-supplied ABIs, and writes them into Postgres. Everything else — fan-out, views, aggregations, reorg handling — is an optional subsystem around that core loop.

## Components

```mermaid
flowchart LR
    subgraph Sources[Block sources]
      RPC[JSON-RPC]
      ERPC[eRPC proxy]
      HS[HyperSync]
    end

    subgraph Core[Core pipeline]
      direction TB
      CS[ChainSyncer]
      W[Worker pool]
      AD[AbiDecoder]
      RD[Reorg detector]
      FW[Factory watcher]
    end

    subgraph Storage[Postgres]
      DATA[(data_schema\nraw_events, factory_children)]
      SYNC[(sync_schema\ndecoded tables, checkpoints)]
      USER[(user_schema\nSQL views)]
    end

    subgraph Out[Fan-out & ops]
      REDIS[Redis Streams]
      WH[Webhooks]
      CSV[CSV export]
      API[HTTP API\n/health /readiness /sync /metrics]
    end

    Sources --> CS
    CS --> W
    W --> AD
    W --> RD
    W --> FW
    AD --> DATA
    AD --> SYNC
    SYNC -. views .-> USER
    W --> REDIS
    W --> WH
    W --> CSV
    Core --> API
```

## Request flow for a single block range

A worker fetches a range, decodes, persists, and fans out. Everything inside the dashed box happens inside one `COPY`-backed transaction so partial writes never land.

```mermaid
sequenceDiagram
    participant W as Worker
    participant S as BlockSource
    participant AD as AbiDecoder
    participant R as Reorg detector
    participant DB as Postgres
    participant Q as Redis / Webhooks / CSV

    W->>S: get_blocks(range, filter)
    S-->>W: Vec<BlockWithLogs>
    W->>AD: decode(logs)
    AD-->>W: decoded events
    W->>R: check finality for tip blocks
    R-->>W: stable | unstable | reorg(depth)

    rect rgb(245, 245, 245)
    note right of W: Atomic persist
    W->>DB: COPY raw_events
    W->>DB: COPY event_<type> (decoded)
    W->>DB: UPSERT checkpoint
    end

    W->>Q: publish events (if enabled)
```

## Module map

All code lives under [src/](../src). Each module is self-contained; cross-module contracts are small traits defined in [src/sources/mod.rs](../src/sources/mod.rs) and [src/types.rs](../src/types.rs).

| Module | Responsibility |
|---|---|
| [`main.rs`](../src/main.rs) | CLI entry, config loading, ABI loading, pool/migration/view setup, top-level task wiring. |
| [`config/`](../src/config) | Strongly-typed YAML config, env-var expansion, validation. |
| [`types.rs`](../src/types.rs) | Shared domain types — `BlockRange`, `BlockWithLogs`, `LogFilter`, `BlockWithTraces`. |
| [`sources/`](../src/sources) | Block-source trait + implementations (`rpc`, `erpc`, `hypersync`), plus `fallback`, `concurrency`, `traces`, `transactions`, and adaptive range sizing. |
| [`abi/`](../src/abi) | ABI-driven event decoder and function decoder, SQL schema generation. |
| [`db/`](../src/db) | Postgres pool, migrations, per-concern writers. |
| [`sync/`](../src/sync) | Orchestration: chain syncer, workers, retry, progress, trace + account syncers, view indexer. |
| [`reorg/`](../src/reorg) | Reorg detection and finality tracking. |
| [`factory/`](../src/factory) | Dynamic child-contract discovery (watcher + backfill). |
| [`filter/`](../src/filter) | Predicate evaluation for per-event filters. |
| [`queue/`](../src/queue) | Redis Streams publisher + HTTP webhook publisher. |
| [`export/`](../src/export) | CSV writer. |
| [`api/`](../src/api) | axum server with health, readiness, sync, metrics routes. |
| [`metrics.rs`](../src/metrics.rs) | Prometheus counters/gauges/histograms. |

## Startup sequence

What [`main.rs`](../src/main.rs) does, in order:

```mermaid
flowchart TD
    A[Parse CLI + load .env] --> B[Parse config YAML]
    B --> C[Init tracing]
    C --> D[Resolve ABI paths]
    D --> E[Load ABIs into AbiDecoder]
    E --> F[Connect DB pool with retry]
    F --> G[Run migrations]
    G --> H[Create / rebuild decoded tables]
    H --> I[Install user-schema views]
    I --> J[Connect Redis - optional]
    J --> K[Start API server\nhealth, sync, metrics]
    K --> L[Start Factory watcher]
    K --> M[Spawn ChainSyncer per chain]
    M --> N[Workers run until cancellation]
    N --> O[Graceful shutdown on SIGINT/SIGTERM]
```

### Relevant source

- Top-level wiring: [src/main.rs](../src/main.rs)
- Config parsing: [src/config/mod.rs](../src/config/mod.rs)
- Pool + retry: [src/db/mod.rs:61-113](../src/db/mod.rs#L61-L113)
- Sync orchestration: [src/sync/chain_syncer.rs](../src/sync/chain_syncer.rs)
