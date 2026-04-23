# indexer-evm

A fast, fault-tolerant **EVM blockchain indexer** written in Rust.

This project is extracted from the broader [Kyomei](https://github.com/) stack and focuses on the piece most teams actually need day-to-day: a single, self-contained binary that ingests on-chain data, decodes it against your ABIs, and writes it into Postgres — reliably, at full RPC speed, with zero framework lock-in.

It exists to solve the real problems dev teams hit in production:

- **Maintainability** — configuration is a single YAML file. Contracts, factories, filters, webhooks, views, traces, and aggregations all live in one place. No code generation, no bespoke DSL, no required TypeScript layer.
- **Robustness** — reorg detection, adaptive RPC batching, exponential-backoff retries, `logsBloom` validation, graceful shutdown, idempotent restarts, and health/readiness probes are built in rather than left to the operator.
- **Speed** — multi-worker historic sync, HyperSync support, `COPY`-based Postgres inserts, Rayon-parallel decoding, and a shared RPC semaphore to stay under provider rate limits.
- **Operability** — Prometheus metrics on `/metrics`, structured JSON logs, `/health` + `/readiness` endpoints, and optional Redis Streams / webhook fan-out for downstream consumers.

> The internal package is still `kyomei-indexer` (schema names, metric names, and Redis stream keys depend on it). That is intentional — renaming those would break existing deployments of the upstream.

---

## Feature overview

| Area | What's supported | Docs |
|---|---|---|
| **Block sources** | Plain JSON-RPC, [eRPC](https://github.com/erpc/erpc) proxy, [HyperSync](https://envio.dev/hypersync) — swap via one config key. Automatic fallback RPC per source. | [block-sources](./docs/block-sources.md) |
| **Contracts** | Static addresses or **factory-discovered** children (e.g. Uniswap V2 pairs). Per-contract `start_block` and ABI. | [factory-contracts](./docs/factory-contracts.md) |
| **Decoding** | Events and function calls decoded against user-supplied ABI JSON files. Per-event tables with `p_*` parameter columns, ready to query. | [abi-decoding](./docs/abi-decoding.md) |
| **Filters** | Per-event conditional filters (`eq / neq / gt / gte / lt / lte / contains`) to skip noisy events at ingest time. | [filters](./docs/filters.md) |
| **Call traces** | `debug_traceBlockByNumber` (geth/reth) and `trace_block` (parity/erigon). Captures calldata, return values, gas, and call-tree position. | [traces](./docs/traces.md) |
| **Accounts** | Track specific addresses for `transaction:from/to` and internal ETH `transfer:from/to` (the latter via traces). | [accounts](./docs/accounts.md) |
| **Reorgs** | Dependency-ordered reorg detection with configurable `max_reorg_depth` and chain-aware finality. Unstable blocks are rewritten atomically. | [reorg-handling](./docs/reorg-handling.md) |
| **Views** | Periodic view-function reads (`totalSupply`, `balanceOf`, etc.) at configurable block intervals. | [views](./docs/views.md) |
| **Aggregations** | TimescaleDB **continuous aggregates** with default `event_count` / `tx_count` and user-defined metrics per event table. | [aggregations](./docs/aggregations.md) |
| **Export / fan-out** | CSV export, Redis Streams publisher, HTTP webhooks with retries. | [fan-out](./docs/fan-out.md) |
| **Deployment** | 3-schema layout (`data_schema` / `sync_schema` / `user_schema`) enabling zero-downtime cutovers. | [schema-model](./docs/schema-model.md) |
| **Multi-chain** | One process, one database, one API — N chains in parallel. | [multi-chain](./docs/multi-chain.md) |
| **Observability** | Prometheus `/metrics`, JSON logs via `tracing`, `/health`, `/readiness`, sync progress via `/sync`. | [observability](./docs/observability.md) |
| **Sync engine** | Parallel workers, checkpointed backfill, live follow, `--rebuild-decoded` replay. | [sync-engine](./docs/sync-engine.md) |

---

## Quick start

### Prerequisites

- Rust 1.94+ (matches `Dockerfile`)
- PostgreSQL 14+ (TimescaleDB strongly recommended for aggregations + hypertables)
- *(Optional)* Redis 7+ for event streaming

### Run locally

```bash
# 1. Clone + build
cargo build --release

# 2. Configure
cp config.example.yaml config.yaml
# edit config.yaml — set RPC URL, start block, and your ABIs

# 3. Run
DATABASE_URL="postgres://postgres:postgres@localhost:5432/indexer" \
  ./target/release/kyomei-indexer --config config.yaml
```

The API then serves on `http://localhost:8080`:

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness — 200 if DB (and Redis when configured) are reachable |
| `GET /readiness` | Readiness — 200 once initial backfill is caught up |
| `GET /sync` | Per-chain sync progress |
| `GET /metrics` | Prometheus scrape endpoint |

### Run with Docker

```bash
docker build -t indexer-evm .

docker run --rm \
  -e DATABASE_URL="postgres://postgres:postgres@host.docker.internal:5432/indexer" \
  -v "$(pwd)/config.yaml:/app/config/config.yaml:ro" \
  -v "$(pwd)/abis:/app/abis:ro" \
  -p 8080:8080 \
  indexer-evm
```

### Rebuild decoded tables

After an ABI change or to recover from a crash that left decoded rows incomplete, re-project the decoded schema from the raw events without re-hitting RPC:

```bash
./target/release/kyomei-indexer --config config.yaml --rebuild-decoded
```

---

## Configuration at a glance

Per-feature pages live under [docs/](./docs/README.md) — each page covers one capability with a Mermaid diagram of the flow. A commented end-to-end example lives in [config.example.yaml](./config.example.yaml). The shape:

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 20

schema:
  data_schema: "kyomei_data"       # shared raw events + factory children
  sync_schema: "uniswap_v2_sync"   # decoded tables + worker checkpoints
  user_schema: "uniswap_v2"        # SQL views — what your app queries

chain:
  id: 1
  name: "ethereum"

source:
  type: "rpc"                      # rpc | erpc | hypersync
  url: "https://eth.llamarpc.com"
  fallback_rpc: "https://rpc.ankr.com/eth"

sync:
  start_block: 10000835
  parallel_workers: 4
  blocks_per_request: 10000
  blocks_per_worker: 250000
  checkpoint_interval: 500
  retry:
    max_retries: 5
    validate_logs_bloom: true      # catch broken eth_getLogs responses

contracts:
  - name: "UniswapV2Factory"
    address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    abi_path: "./abis/UniswapV2Factory.json"
    start_block: 10000835

  - name: "UniswapV2Pair"
    factory:                        # dynamic address discovery
      address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
      event: "PairCreated"
      parameter: "pair"
    abi_path: "./abis/UniswapV2Pair.json"
```

Everything else — webhooks, traces, accounts, aggregations, multi-chain, CSV export — is optional and additive. See [config.example.yaml](./config.example.yaml#L137-L309).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                           indexer-evm (Rust)                          │
│                                                                       │
│  ┌───────────────┐    ┌────────────────┐    ┌──────────────────┐     │
│  │ BlockSource   │ →  │ ChainSyncer    │ →  │ AbiDecoder       │     │
│  │ rpc / erpc /  │    │ (workers +     │    │ events + traces  │     │
│  │ hypersync     │    │  checkpoints)  │    │ → per-event tbls │     │
│  └───────────────┘    └────────┬───────┘    └─────────┬────────┘     │
│          │                     │                      │              │
│          ▼                     ▼                      ▼              │
│   Adaptive range       Reorg detector          Postgres (COPY)       │
│   + shared semaphore   + finality watcher      + TimescaleDB agg.    │
│   + fallback RPC       + rewrites              + views (user schema) │
│                                                                       │
│  ┌──────────────┐   ┌───────────────┐   ┌────────────────────────┐   │
│  │ Factory      │   │ View Indexer  │   │ Fan-out                │   │
│  │ Watcher      │   │ (periodic     │   │ Redis Streams /        │   │
│  │ (discovers   │   │  read calls)  │   │ Webhooks / CSV         │   │
│  │  children)   │   │               │   │                        │   │
│  └──────────────┘   └───────────────┘   └────────────────────────┘   │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │ axum HTTP API — /health /readiness /sync /metrics             │   │
│  └───────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

### Module map

All code lives under [src/](./src):

| Module | Responsibility |
|---|---|
| [`main.rs`](./src/main.rs) | CLI entry, config loading, ABI loading, pool/migration/view setup, top-level task wiring. |
| [`config/`](./src/config) | Strongly-typed YAML config (single-chain and multi-chain formats), env-var expansion, validation. |
| [`types.rs`](./src/types.rs) | Shared domain types — `BlockRange`, `BlockWithLogs`, `LogFilter`, `BlockWithTraces`, etc. |
| [`sources/`](./src/sources) | Block-source abstraction and implementations: [`rpc`](./src/sources/rpc.rs), [`erpc`](./src/sources/erpc.rs), [`hypersync`](./src/sources/hypersync.rs), plus [`fallback`](./src/sources/fallback.rs), [`concurrency`](./src/sources/concurrency.rs), [`traces`](./src/sources/traces.rs), [`transactions`](./src/sources/transactions.rs). Adaptive block-range sizing. |
| [`abi/`](./src/abi) | ABI-driven decoding: [`decoder.rs`](./src/abi/decoder.rs) (events) and [`function_decoder.rs`](./src/abi/function_decoder.rs) (calls/traces), plus SQL schema generation. |
| [`db/`](./src/db) | Postgres pool, migrations, and per-concern writers ([`events`](./src/db/events.rs), [`decoded`](./src/db/decoded.rs), [`traces`](./src/db/traces.rs), [`accounts`](./src/db/accounts.rs), [`factory`](./src/db/factory.rs), [`workers`](./src/db/workers.rs), [`views`](./src/db/views.rs)). `COPY`-based inserts. |
| [`sync/`](./src/sync) | Orchestration: [`chain_syncer`](./src/sync/chain_syncer.rs), [`worker`](./src/sync/worker.rs), [`trace_syncer`](./src/sync/trace_syncer.rs), [`account_syncer`](./src/sync/account_syncer.rs), [`view_indexer`](./src/sync/view_indexer.rs), [`progress`](./src/sync/progress.rs), [`retry`](./src/sync/retry.rs). |
| [`reorg/`](./src/reorg) | [`detector`](./src/reorg/detector.rs) + [`finality`](./src/reorg/finality.rs) for safe tip handling. |
| [`factory/`](./src/factory) | Dynamic child-contract discovery — [`watcher`](./src/factory/watcher.rs) (live) + [`backfill`](./src/factory/backfill.rs) (historic). |
| [`filter/`](./src/filter) | Predicate evaluation for per-event filters defined in config. |
| [`queue/`](./src/queue) | Event fan-out — [`publisher`](./src/queue/publisher.rs) (Redis Streams) and [`webhook`](./src/queue/webhook.rs) (HTTP). |
| [`export/`](./src/export) | [`csv`](./src/export/csv.rs) — per-event-type CSV writer. |
| [`api/`](./src/api) | axum server exposing [`health`](./src/api/health.rs) and [`sync`](./src/api/sync.rs). |
| [`metrics.rs`](./src/metrics.rs) | Prometheus counters/gauges/histograms: blocks indexed, events stored, RPC latency, reorgs, DB copy duration, consecutive errors, etc. |

### The 3-schema model (why zero-downtime works)

The indexer separates storage into three logical schemas:

| Schema | Contents | Lifetime |
|---|---|---|
| `data_schema` (default `kyomei_data`) | Raw events, factory children — the source of truth. | Shared across all deployments. |
| `sync_schema` (e.g. `uniswap_v2_sync`) | Decoded per-event tables, worker checkpoints, trace/account tables. | **Per-deployment.** Same name → resume from checkpoint. New name → rebuilt from `raw_events`. |
| `user_schema` (e.g. `uniswap_v2`) | Stable SQL views. Your app queries these. | Points at the currently-active `sync_schema`. |

Zero-downtime deployment:

1. Start a new indexer instance with a *new* `sync_schema` name.
2. It replays raw events into decoded tables (no RPC traffic required for history already seen).
3. When `/readiness` returns 200, swap the views in `user_schema` to the new `sync_schema`.
4. Drain and stop the old instance.

Full protocol and SQL in [docs/schema-model.md](./docs/schema-model.md).

---

## Robustness, in detail

These are the failure modes the indexer is specifically built to survive:

- **Flaky RPC** — exponential backoff with configurable `max_retries`; shared semaphore caps concurrent requests; automatic fallback to a secondary RPC URL.
- **Broken `eth_getLogs`** — providers occasionally return zero logs for a block whose `logsBloom` says otherwise. `validate_logs_bloom: true` detects and retries this.
- **"Range too big" errors** — `AdaptiveRange` halves the block window on failure, grows 5 % on success, and honors explicit size hints from RPC error messages ([src/sources/mod.rs:9-52](./src/sources/mod.rs#L9-L52)).
- **Reorgs** — unstable blocks (within `max_reorg_depth` of tip) are tracked and atomically rewritten when hashes change; metrics counter `kyomei_reorgs_detected_total` surfaces frequency.
- **Crash mid-flight** — progress is checkpointed every `checkpoint_interval` blocks; `--rebuild-decoded` re-projects decoded tables from raw events without replaying RPC.
- **Silent stalls** — `max_live_consecutive_errors` aborts the process after N errors instead of appearing healthy while making no progress.
- **Startup race** — DB pool retries with backoff for up to 5 attempts ([src/db/mod.rs:61-113](./src/db/mod.rs#L61-L113)); ABI paths are resolved relative to the config file so relative paths work identically in dev and in Docker.
- **Graceful shutdown** — SIGINT/SIGTERM cancellation token propagates to all workers; in-flight batches finish and checkpoint before the process exits.

---

## Observability

### Prometheus metrics

All metrics are labeled with `chain_id`:

- `kyomei_blocks_indexed_total{phase=historic|live}`
- `kyomei_events_stored_total`
- `kyomei_rpc_requests_total{status=ok|err}` · `kyomei_rpc_latency_seconds`
- `kyomei_sync_current_block` · `kyomei_sync_chain_tip`
- `kyomei_consecutive_errors`
- `kyomei_reorgs_detected_total`
- `kyomei_traces_indexed_total{phase}` · `kyomei_account_events_indexed_total{event_type}`
- `kyomei_db_copy_duration_seconds`

Definitions in [src/metrics.rs](./src/metrics.rs).

### Logs

`tracing` with `tracing-subscriber`. Configure via `logging.level` and `logging.format` (`pretty` | `json`). All log lines include `chain_id` / `chain_name` when available.

---

## Testing

```bash
cargo test            # unit tests
cargo check           # fast type-check
cargo clippy          # lints
```

Integration tests live under [tests/](./tests). Fixtures (ABIs, canned RPC responses) under [tests/fixtures/](./tests/fixtures).

---

## Releasing

Releases are cut from `main` via a `vX.Y.Z` tag. The tag push triggers [.github/workflows/release.yml](./.github/workflows/release.yml), which:

1. Creates a draft GitHub Release with auto-generated notes.
2. Cross-builds release binaries for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`, and `aarch64-apple-darwin`.
3. Packages each binary with `README.md`, the `docs/` directory, and `config.example.yaml` into `kyomei-indexer-<tag>-<target>.tar.gz`, plus a sibling `.sha256` checksum.
4. Uploads all archives to the GitHub Release.
5. Publishes the release (removes the draft flag) once all artifacts are uploaded.

CI on PRs and pushes to `main` runs `fmt + clippy + test` via [.github/workflows/ci.yml](./.github/workflows/ci.yml) to keep the tag surface green.

The easiest path to cut a release is the `Makefile`:

```bash
make release VERSION=0.2.0
```

That target runs `fmt --check`, `clippy`, and `test` locally, bumps `Cargo.toml`, refreshes `Cargo.lock`, commits `release: v0.2.0`, creates an annotated tag `v0.2.0`, and pushes both `main` and the tag. See [Makefile](./Makefile) for other targets (`build`, `check`, `fmt`, `clippy`, `test`, `run`, `docker`, `tag-only`).

---

## Project layout

```
indexer-evm/
├── src/                    # Rust source (module map above)
├── tests/                  # Integration tests + fixtures
├── docs/                   # Per-feature documentation (start at docs/README.md)
├── Cargo.toml              # Binary crate: kyomei-indexer
├── Cargo.lock              # Reproducible builds
├── Dockerfile              # Multi-stage build, debian-slim runtime
├── config.example.yaml     # Annotated config — the fastest way to learn options
└── README.md
```

---

## License

MIT. Inherited from the upstream Kyomei repository.
