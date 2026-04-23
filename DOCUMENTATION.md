# Kyomei Indexer Documentation

High-performance EVM blockchain indexer built in Rust. Indexes smart contract events from any EVM chain into TimescaleDB with automatic ABI decoding, factory contract discovery, reorg handling, and optional real-time streaming via Redis.

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture Overview](#architecture-overview)
- [Configuration Reference](#configuration-reference)
- [Data Sources](#data-sources)
- [Contract Configuration](#contract-configuration)
- [Factory Contracts](#factory-contracts)
- [Database Schema](#database-schema)
- [Sync Engine](#sync-engine)
- [Reorg Handling](#reorg-handling)
- [API Endpoints](#api-endpoints)
- [Prometheus Metrics](#prometheus-metrics)
- [Deployment](#deployment)
- [Event Filters](#event-filters)
- [View Function Indexing](#view-function-indexing)
- [Webhook Notifications](#webhook-notifications)
- [CSV Export](#csv-export)
- [Call Trace Indexing](#call-trace-indexing)
- [Account/Transaction Tracking](#accounttransaction-tracking)
- [Continuous Aggregates](#continuous-aggregates)
- [Multi-Chain Mode](#multi-chain-mode)
- [Configuration Examples](#configuration-examples)
- [Crash Recovery and Decoded Table Rebuild](#crash-recovery-and-decoded-table-rebuild)
- [Zero-Downtime Deployments](#zero-downtime-deployments)
- [Performance Internals](#performance-internals)
- [Tuning Guide](#tuning-guide)

---

## Quick Start

### Prerequisites

- **TimescaleDB** (PostgreSQL with TimescaleDB extension)
- **Redis** (for event streaming)
- **Rust 1.94+** (for building from source) or Docker

### Running with Docker

```bash
docker run -v ./config:/app/config -v ./abis:/app/abis \
  -e DATABASE_URL="postgresql://user:pass@host:5432/kyomei" \
  -e REDIS_URL="redis://host:6379" \
  -p 8080:8080 \
  kyomei-indexer --config /app/config/config.yaml
```

### Running from Source

```bash
# Build
cargo build --release -p kyomei-indexer

# Run
./target/release/kyomei-indexer --config config.yaml

# Rebuild decoded tables from existing raw_events (after crash or ABI change)
./target/release/kyomei-indexer --config config.yaml --rebuild-decoded
```

### Minimal Setup

1. Copy `config.example.yaml` to `config.yaml`
2. Set `DATABASE_URL` and `REDIS_URL` environment variables (or edit directly)
3. Place your contract ABI JSON files in an `abis/` directory
4. Configure your contracts in `config.yaml`
5. Run the indexer

---

## Architecture Overview

The indexer operates in two phases:

```
┌──────────────────────────────────────────────────────┐
│                  HISTORIC SYNC                        │
│                                                       │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
│  │Worker 0 │  │Worker 1 │  │Worker 2 │  ...          │
│  │0-250K   │  │250K-500K│  │500K-750K│              │
│  └────┬────┘  └────┬────┘  └────┬────┘              │
│       │            │            │                     │
│       └────────────┼────────────┘                     │
│                    ▼                                  │
│           Factory Reconciliation                      │
│      (fills gaps from parallel sync)                  │
│                    │                                  │
│                    ▼                                  │
│  ┌──────────────────────────────────────────┐        │
│  │          Transition to Live               │        │
│  └──────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────┐
│                   LIVE SYNC                           │
│                                                       │
│  Single worker, per-block processing                  │
│  Reorg detection + factory child tracking             │
│  Atomic writes (events + decoded + checkpoint)        │
└──────────────────────────────────────────────────────┘
```

**Key features:**
- Single-worker sequential sync (recommended) with dynamic factory discovery
- Parallel historic sync with configurable workers (1-32) **(Beta)**
- Adaptive block range sizing based on RPC errors
- Adaptive concurrency control (shared semaphore)
- Automatic factory child discovery and backfill
- PostgreSQL COPY protocol for high-throughput inserts
- ON CONFLICT DO NOTHING for idempotent/crash-safe writes
- Graceful shutdown with CancellationToken (SIGINT/SIGTERM)
- Event filters for selective storage
- View function indexing (periodic `eth_call` snapshots)
- Webhook notifications (fire-and-forget HTTP POST)
- CSV export for data pipelines
- TimescaleDB continuous aggregates (auto-refreshing rollups)
- Multi-chain mode (single process, multiple chains)
- Pre-computed ABI signature index for fast decoding
- Zero-copy event processing (move semantics, no intermediate allocations)
- Concurrent address batch fetching within workers
- Resume-aware progress tracking (preserves percentage across restarts)

---

## Configuration Reference

Configuration is loaded from a YAML file. All string values support environment variable expansion using `${VAR_NAME}` syntax.

### database

PostgreSQL (TimescaleDB) connection settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_string` | string | *required* | PostgreSQL connection string. Supports `${ENV_VAR}` expansion. |
| `pool_size` | integer | `20` | Maximum number of connections in the pool. |

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 20
```

The pool is configured with:
- 10s connection acquire timeout
- 300s idle connection timeout
- Automatic retry on connection failure (5 attempts with exponential backoff)

### redis

Redis connection for event streaming via Redis Streams.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *required* | Redis connection URL. |
| `batch_notification_interval` | integer | `1000` | Emit batch notification every N blocks during historic sync. |

```yaml
redis:
  url: "${REDIS_URL}"
  batch_notification_interval: 1000
```

### schema

Database schema names for data organization. The 3-schema architecture enables zero-downtime deployments.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `data_schema` | string | `"kyomei_data"` | Internal schema for raw events and factory children. Shared across deployments. |
| `sync_schema` | string | *required* | Per-deployment schema for decoded tables and sync workers. |
| `user_schema` | string | *required* | User-facing schema where SQL views are created for querying. |

```yaml
schema:
  data_schema: "kyomei_data"
  sync_schema: "my_project_sync"
  user_schema: "my_project"
```

The indexer creates three schemas:
- **data_schema**: Contains shared tables (`raw_events`, `factory_children`) that persist across deployments. Multiple indexers can write here concurrently (ON CONFLICT DO NOTHING).
- **sync_schema**: Contains per-deployment tables (`sync_workers`, decoded event tables). Same name on restart = resume from checkpoint. New name = rebuild decoded tables from raw_events automatically.
- **user_schema**: Contains SQL views like `my_project.event_uniswap_v2_pair_swap` that point to decoded tables in the active `sync_schema`.

This architecture enables **zero-downtime Kubernetes deployments** — see [Zero-Downtime Deployments](#zero-downtime-deployments).

### chain

EVM chain identification.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | integer | *required* | EVM chain ID (must be > 0). Used for multi-chain support and finality defaults. |
| `name` | string | *required* | Human-readable chain name (for logging). |

```yaml
chain:
  id: 1
  name: "ethereum"
```

### source

Block data source configuration. Three source types are supported.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | *required* | One of: `"rpc"`, `"erpc"`, `"hypersync"`. |

See [Data Sources](#data-sources) for detailed configuration per source type.

### sync

Sync engine configuration. Controls parallelism, batching, and retry behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start_block` | integer | *required* | Block number to start indexing from (must be > 0). |
| `parallel_workers` | integer | `4` | Number of parallel workers for historic sync. Min 1, max 32. **`1` is recommended** for production with factory contracts. Values `2+` enable multi-worker mode (Beta). |
| `blocks_per_request` | integer | `1000` | Maximum blocks per RPC/source request. |
| `blocks_per_worker` | integer | `250000` | Blocks assigned to each historic sync worker. |
| `event_buffer_size` | integer | `10000` | Max events buffered in memory before flushing. |
| `checkpoint_interval` | integer | `500` | Save worker progress every N blocks. |
| `max_live_consecutive_errors` | integer | `100` | Abort live sync after this many consecutive errors. |
| `max_concurrent_requests` | integer | `parallel_workers * 2` | Max concurrent RPC requests (shared semaphore). |
| `addresses_per_batch` | integer | `500` | Max contract addresses per RPC request within a worker. Splits into parallel batches within the same worker when exceeded. |
| `retry` | object | see below | Retry and validation settings. |

```yaml
sync:
  start_block: 10000835
  parallel_workers: 4
  blocks_per_request: 10000
  blocks_per_worker: 250000
  checkpoint_interval: 500
```

#### sync.retry

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | integer | `5` | Maximum retry attempts per RPC call. |
| `initial_backoff_ms` | integer | `1000` | Initial backoff delay in ms (doubles each retry). |
| `max_backoff_ms` | integer | `30000` | Maximum backoff delay cap in ms. |
| `validate_logs_bloom` | boolean | `true` | Validate logsBloom to detect missing logs from RPC. |
| `bloom_validation_retries` | integer | `3` | Extra retries specifically for bloom validation failures. |

```yaml
sync:
  retry:
    max_retries: 5
    initial_backoff_ms: 1000
    max_backoff_ms: 30000
    validate_logs_bloom: true
    bloom_validation_retries: 3
```

### contracts

List of smart contracts to index. See [Contract Configuration](#contract-configuration).

### webhooks

List of webhook endpoints for event notifications. See [Webhook Notifications](#webhook-notifications).

### aggregations

TimescaleDB continuous aggregate settings. See [Continuous Aggregates](#continuous-aggregates).

### export

Data export settings (CSV). See [CSV Export](#csv-export).

### reorg

Chain reorganization detection settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_reorg_depth` | integer | `1000` | Maximum reorg depth to track (rolling window of block hashes). |
| `finality_blocks` | integer | *chain-specific* | Override the built-in finality threshold. |

```yaml
reorg:
  max_reorg_depth: 1000
  # finality_blocks: 65  # optional override
```

Built-in finality defaults by chain:

| Chain | Chain ID | Finality Blocks |
|-------|----------|----------------|
| Ethereum | 1 | 65 |
| Polygon | 137 | 200 |
| Arbitrum | 42161 | 240 |
| Optimism | 10 | 240 |
| Base | 8453 | 240 |
| BSC | 56 | 15 |
| Avalanche | 43114 | 12 |
| Fantom | 250 | 5 |
| Gnosis | 100 | 20 |
| zkSync Era | 324 | 50 |
| Linea | 59144 | 100 |
| Scroll | 534352 | 100 |
| Other | * | 65 |

### api

HTTP API server configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | `"0.0.0.0"` | Bind address. |
| `port` | integer | `8080` | Port number. |

```yaml
api:
  host: "0.0.0.0"
  port: 8080
```

### logging

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | string | `"info"` | Log level: `trace`, `debug`, `info`, `warn`, `error`. |
| `format` | string | `"pretty"` | Output format: `"pretty"` (human-readable) or `"json"` (structured). |

```yaml
logging:
  level: "info"
  format: "json"
```

The `RUST_LOG` environment variable overrides the configured level if set.

---

## Data Sources

### RPC (Standard JSON-RPC)

Direct connection to any EVM JSON-RPC node. Best for simplicity and small-to-medium indexing jobs.

```yaml
source:
  type: "rpc"
  url: "https://eth.llamarpc.com"
  fallback_rpc: "https://rpc.ankr.com/eth"  # optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *required* | Primary RPC endpoint URL. |
| `fallback_rpc` | string | *optional* | Fallback RPC endpoint, used when primary fails. |

**Features:**
- Adaptive block range: automatically shrinks on "range too big" errors, grows on success
- LogsBloom validation: detects when RPC returns empty logs for blocks that should have events
- Log cross-validation: verifies log block hashes match the requested block
- Exponential backoff with jitter (±25%) to prevent thundering herd
- Error classification: rate limits, range errors, and transient errors handled differently

**Fallback behavior:** On any error (rate limit, range too big, transient), the request is retried on the fallback RPC before applying backoff. The fallback has its own independent retry/adaptive range logic.

### eRPC (Enhanced RPC Proxy)

For deployments using [eRPC](https://github.com/erpc/erpc) as an RPC proxy/aggregator.

```yaml
source:
  type: "erpc"
  url: "http://localhost:4000"
  project_id: "my-project"       # optional
  fallback_rpc: "https://eth.llamarpc.com"  # optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *required* | eRPC endpoint URL. |
| `project_id` | string | *optional* | eRPC project ID for request routing. |
| `fallback_rpc` | string | *optional* | Fallback RPC when eRPC is unavailable. |

**eRPC advantages:**
- Automatic retry across multiple upstream RPCs
- Circuit breaker for failing upstreams
- Hedge policy (parallel requests for latency)
- Rate limit budget management
- `eth_getLogs` range auto-splitting

Since eRPC handles retries internally, the indexer caps client-side retries at 2 to avoid double retry storms.

### HyperSync (Envio)

For fastest historical indexing using [Envio HyperSync](https://docs.envio.dev/docs/HyperSync/overview). HyperSync serves pre-indexed data via a specialized API, significantly faster than standard RPC for historical data.

```yaml
source:
  type: "hypersync"
  url: "https://eth.hypersync.xyz"          # optional for known chains
  api_token: "${HYPERSYNC_API_TOKEN}"       # optional
  fallback_rpc: "https://eth.llamarpc.com"  # optional, auto-resolved for known chains
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *auto-resolved* | HyperSync API URL. Auto-resolved for known chains. |
| `api_token` | string | *optional* | HyperSync API token for authenticated access. |
| `fallback_rpc` | string | *auto-resolved* | RPC for reorg detection. Auto-resolved for known chains. |

**How it works:**
- Historical blocks/logs fetched from HyperSync's pre-indexed API (much faster than RPC)
- Handles pagination automatically (HyperSync may return partial responses with `next_block`)
- Falls back to RPC for `get_block_hash` calls (needed for reorg detection)
- Known chains get automatic fallback RPC URLs (Ethereum, Polygon, Arbitrum, Optimism, Base, BSC, Avalanche, etc.)

**Supported chains with auto-resolution:**

| Chain | Chain ID | Default Fallback RPC |
|-------|----------|---------------------|
| Ethereum | 1 | `https://eth.llamarpc.com` |
| Sepolia | 11155111 | `https://rpc.sepolia.org` |
| Polygon | 137 | `https://polygon.llamarpc.com` |
| Arbitrum | 42161 | `https://arb1.arbitrum.io/rpc` |
| Optimism | 10 | `https://mainnet.optimism.io` |
| Base | 8453 | `https://mainnet.base.org` |
| BSC | 56 | `https://bsc-dataseed.binance.org` |
| Avalanche | 43114 | `https://api.avax.network/ext/bc/C/rpc` |
| Gnosis | 100 | `https://rpc.gnosischain.com` |
| Fantom | 250 | `https://rpc.ftm.tools` |
| zkSync Era | 324 | `https://mainnet.era.zksync.io` |
| Linea | 59144 | `https://rpc.linea.build` |
| Scroll | 534352 | `https://rpc.scroll.io` |

For unlisted chains, you must provide `fallback_rpc` explicitly.

---

## Contract Configuration

Each entry in `contracts` defines a smart contract to index.

```yaml
contracts:
  - name: "MyToken"
    address: "0x1234...abcd"
    abi_path: "./abis/MyToken.json"
    start_block: 18000000
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | *required* | Unique identifier for this contract. Used in table/view names. |
| `address` | string | *conditional* | Fixed contract address. Required if `factory` is not set. |
| `factory` | object | *conditional* | Factory configuration. Required if `address` is not set. |
| `abi_path` | string | *required* | Path to the ABI JSON file. Relative paths resolve from the config file's directory. |
| `start_block` | integer | *optional* | Override the global `sync.start_block` for this contract. |
| `filters` | list | `[]` | Event filters — only store events matching all conditions. See [Event Filters](#event-filters). |
| `views` | list | `[]` | View functions to periodically call and store. See [View Function Indexing](#view-function-indexing). |

**Rules:**
- Each contract must have either `address` OR `factory`, not both.
- The `name` is used to generate database table and view names (converted to snake_case).
- ABI files should be standard Solidity ABI JSON arrays.

---

## Factory Contracts

Factory contracts dynamically deploy child contracts. The indexer automatically discovers children by watching factory events and indexes their logs.

```yaml
contracts:
  # The factory contract itself
  - name: "UniswapV2Factory"
    address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    abi_path: "./abis/UniswapV2Factory.json"

  # The child contract type (discovered dynamically)
  - name: "UniswapV2Pair"
    factory:
      address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
      event: "PairCreated"
      parameter: "pair"
    abi_path: "./abis/UniswapV2Pair.json"
```

### Factory Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `address` | string | *required* | Address of the factory contract. |
| `event` | string | *required* | Name of the creation event to watch (e.g., `"PairCreated"`). |
| `parameter` | string or list | *required* | Event parameter(s) containing the child address(es). |
| `child_contract_name` | string | contract's `name` | Optional override for child contract name (for ABI lookup). |
| `child_abi_path` | string | contract's `abi_path` | Optional separate ABI for child contracts. |

### Single vs Multiple Parameters

**Single parameter** — one child address per event:
```yaml
factory:
  address: "0x5C69bEe..."
  event: "PairCreated"
  parameter: "pair"
```

**Multiple parameters** — multiple child addresses per event:
```yaml
factory:
  address: "0x1234..."
  event: "PoolCreated"
  parameter:
    - "token0"
    - "token1"
```

### How Factory Discovery Works

1. **Detection**: The indexer watches for the specified `event` emitted by the factory `address`. When seen, it extracts child contract addresses from the specified `parameter`(s).

2. **Storage**: Discovered children are persisted in the `factory_children` table with their creation block, transaction hash, and factory address.

3. **Backfill**: When a child is discovered, the indexer immediately backfills all events for that child from its creation block to the current indexed block.

4. **Parallel sync reconciliation**: During parallel historic sync, workers process different block ranges independently. If Worker A discovers a factory child, Worker B (processing a different range) won't initially know about it. After all historic workers complete, a **reconciliation pass** runs — it queries all discovered children from the database and re-fetches their events across the full historic range in parallel. Since writes use ON CONFLICT DO NOTHING, already-indexed events are safely skipped.

5. **Live tracking**: Once in live sync mode, all known children (loaded from DB) are included in the log filter. New children discovered during live sync are immediately added to the active filter.

---

## Database Schema

The indexer creates and manages three PostgreSQL schemas.

### Data Schema (shared)

Default: `kyomei_data`

Contains tables shared across deployments. Multiple indexers can write here concurrently using ON CONFLICT DO NOTHING.

#### raw_events (TimescaleDB hypertable)

Stores all raw blockchain event logs.

```sql
raw_events (
    chain_id        INTEGER,
    block_number    BIGINT,
    tx_index        INTEGER,
    log_index       INTEGER,
    block_hash      TEXT,
    block_timestamp BIGINT,
    tx_hash         TEXT,
    address         TEXT,
    topic0          TEXT,    -- event signature hash
    topic1          TEXT,    -- indexed param 1
    topic2          TEXT,    -- indexed param 2
    topic3          TEXT,    -- indexed param 3
    data            TEXT,           -- non-indexed params (hex)
    PRIMARY KEY (chain_id, block_number, tx_index, log_index)
)
```

Indexes:
- `(chain_id, address, topic0, block_number)` — fast event lookups by contract + event type
- `(chain_id, block_timestamp)` — time-based queries

#### factory_children

Tracks all dynamically discovered child contracts.

```sql
factory_children (
    chain_id              INTEGER,
    factory_address       TEXT,
    child_address         TEXT,
    contract_name         TEXT,
    created_at_block      BIGINT,
    created_at_tx_hash    TEXT,
    created_at_log_index  INTEGER,
    metadata              TEXT,
    child_abi             TEXT,
    PRIMARY KEY (chain_id, child_address)
)
```

### Sync Schema (per-deployment)

Configured via `sync_schema`. Contains tables specific to each deployment.

#### Decoded event tables

One table per event type, automatically generated from the contract ABI. Example for a `Transfer(address indexed from, address indexed to, uint256 value)` event:

```sql
event_my_token_transfer (
    chain_id        INTEGER,
    block_number    BIGINT,
    tx_index        INTEGER,
    log_index       INTEGER,
    block_hash      TEXT,
    block_timestamp BIGINT,
    tx_hash         TEXT,
    address         TEXT,
    -- Decoded parameters:
    "from"          TEXT,
    "to"            TEXT,
    value           NUMERIC(78, 0)
)
```

Each decoded table is also a TimescaleDB hypertable with compression enabled.

#### sync_workers

Tracks sync progress for crash recovery.

```sql
sync_workers (
    chain_id        INTEGER,
    worker_id       INTEGER,
    range_start     BIGINT,
    range_end       BIGINT,
    current_block   BIGINT,
    status          TEXT,   -- 'historical' or 'live'
    PRIMARY KEY (chain_id, worker_id)
)
```

### User Schema (views)

Configured via `user_schema`.

Contains SQL views that point to decoded tables in the active `sync_schema`. Views are named: `event_{contract_name}_{event_name}` (snake_case). Factory contract views include a subquery against `data_schema.factory_children` for address filtering.

Example query:
```sql
SELECT * FROM my_project.event_uniswap_v2_pair_swap
WHERE block_timestamp > extract(epoch from now() - interval '1 hour')
ORDER BY block_number DESC
LIMIT 100;
```

---

## Sync Engine

The indexer supports two indexing strategies, selected automatically based on the `parallel_workers` configuration value.

### Indexing Strategies

#### Single-Worker Mode (`parallel_workers: 1`) — Recommended

The default and recommended strategy. One worker processes the entire block range sequentially from `start_block` to chain tip, ensuring factory children are discovered and added to the filter before the next batch is fetched. This guarantees zero missed events from dynamically deployed contracts.

**How it works:**
1. **Fetch**: Retrieves blocks in batches of `blocks_per_request`. If the number of contract addresses exceeds `addresses_per_batch`, the worker splits them into concurrent sub-batches within the same block range (via `try_join_all`) and merges results — no events are missed.
2. **Factory discovery**: Before consuming block data, the worker scans for factory creation events and immediately adds new child addresses to the active filter.
3. **Insert**: Raw events and decoded events are written to the database using COPY protocol in a single transaction.
4. **Checkpoint**: Progress is saved every `checkpoint_interval` blocks to the `sync_workers` table.
5. **Transition**: Once the chain tip is reached, the indexer transitions to live sync.

**Resume behavior**: On restart, the worker resumes from its last checkpoint. Progress percentage is calculated from the original `start_block` (not the resume point), so if you were at 20% when the process stopped, it shows ~20% on resume.

**Performance optimizations in single-worker mode:**
- Pre-computed ABI signature index (`SignatureEntry`) — table names and parameter columns are cached at ABI registration time, avoiding repeated string computation during decode.
- `decode_value_raw` — decodes hex values without allocating a `0x`-prefixed intermediate string.
- u128 fast path for uint decoding — values up to 128 bits use native integer parsing instead of U256.
- Move semantics (`into_raw_event`) — log entries are consumed (moved) instead of cloned when building raw event records.
- Concurrent address sub-batching — when a worker has more addresses than `addresses_per_batch`, sub-batches are fetched concurrently via `futures::future::try_join_all` and merged using move-based entry merging (no log cloning).
- Chunked factory COPY — factory children inserts are chunked into 5,000-row batches to avoid oversized COPY payloads.

**Best for**: Production indexing with factory contracts, ordered processing, strict-RPC environments, or any scenario where missing events is unacceptable.

```yaml
sync:
  parallel_workers: 1  # single-worker sequential mode (recommended)
```

#### Multi-Worker Mode (`parallel_workers: 2+`) — Beta

> **Beta**: Multi-worker mode is functional but considered beta. For production deployments with factory contracts, single-worker mode is recommended to guarantee zero missed child events. Multi-worker mode requires a post-sync reconciliation pass that may re-fetch large ranges.

A two-step parallel approach that splits work across multiple workers.

**Step 1 — Initial parallel sync:**
- Determines the oldest `start_block` across all configured contracts
- Splits the block range `[oldest_start, chain_tip]` evenly across `parallel_workers` workers
- Each worker fetches events for ALL contracts + known factory children from the database
- If the total number of addresses exceeds `addresses_per_batch`, each worker internally splits addresses into parallel sub-batches and merges results (no new workers are created)

**Step 2 — Factory children backfill:**
- After Step 1 discovers new factory children, they are backfilled
- Uses the same block-range splitting strategy across `parallel_workers` workers
- Address batching within workers applies the same `addresses_per_batch` limit

```
Total range: block 10,000,000 → 11,000,000 (1M blocks)
Workers: 4, blocks_per_worker: 250,000

Worker 1: blocks 10,000,000 → 10,249,999
Worker 2: blocks 10,250,000 → 10,499,999
Worker 3: blocks 10,500,000 → 10,749,999
Worker 4: blocks 10,750,000 → 10,999,999
```

**Best for**: Large block ranges without factory contracts, fast historical catch-up when factory correctness is not critical.

```yaml
sync:
  parallel_workers: 4   # multi-worker parallel mode (beta)
  addresses_per_batch: 500  # max addresses per RPC request within a worker
```

> **Warning**: In multi-worker mode, if Worker A discovers a factory child at block 500K, Worker B (processing blocks 0-250K) won't know about it during its run. The post-sync reconciliation pass handles this, but it means a second fetch over historical ranges. Single-worker mode avoids this entirely.

### Historic Sync (Common Behavior)

Each worker (whether single or multi):
- Fetches blocks in batches of `blocks_per_request`
- Splits addresses exceeding `addresses_per_batch` into concurrent sub-batches (fetched via `try_join_all`, merged by block number)
- Checkpoints progress every `checkpoint_interval` blocks
- Detects and backfills factory children within its range
- Respects graceful shutdown signals
- Uses move semantics to avoid cloning event data between processing stages

**Crash recovery**: On restart, the indexer reads saved worker checkpoints and resumes from where each worker left off. The strategy (single/multi) only affects `start_fresh` — resume always uses existing worker state. Progress percentage reflects the full range from `start_block`, not just the remaining range.

### Adaptive Block Range

The indexer automatically adjusts the number of blocks per request:

- **Shrink** on "range too big" / "too many results" errors → divide by 2
- **Grow** on success → multiply by 1.05
- **Min**: 10 blocks, **Max**: configured `blocks_per_request`
- Parses RPC error messages for suggested range sizes (Alchemy, QuickNode, etc.)

### Adaptive Concurrency

A shared semaphore limits concurrent RPC requests across all workers:

- **Grow** on successful requests → add 1 permit (up to max)
- **Shrink** on rate limit errors → halve permits (minimum 1)
- Prevents thundering herd when multiple workers hit the same RPC

### Live Sync

After historic sync completes, a single worker processes new blocks as they arrive:

- Per-block processing with reorg detection
- Atomic writes: raw events + decoded events + worker checkpoint in a single DB transaction
- Factory child tracking continues (new children added to filter immediately)
- Configurable max consecutive errors before abort (`max_live_consecutive_errors`)

### Graceful Shutdown

The indexer handles SIGINT/SIGTERM signals:
1. Sets cancellation flag
2. Workers finish their current batch
3. Workers write a final checkpoint
4. Process exits cleanly

This prevents mid-write corruption and ensures clean resume on restart.

---

## Reorg Handling

Chain reorganizations (reorgs) occur when the blockchain's canonical chain changes. The indexer detects and handles these automatically.

**Detection**: During live sync, the indexer maintains a rolling window of `(block_number, block_hash)` pairs. For each new block, it verifies the parent hash matches the known hash for the previous block. A mismatch indicates a reorg.

**Recovery**:
1. Walk back to find the common ancestor (last block where hashes agree)
2. Delete all data from affected blocks: raw events, decoded events, factory children
3. Rebuild the log filter (some factory children may have been removed)
4. Resume indexing from the common ancestor + 1

The `max_reorg_depth` setting limits how far back the detector looks. The `finality_blocks` setting determines when a block is considered final (no longer subject to reorgs).

---

## API Endpoints

### GET /health

Liveness probe. Checks that the process is running and all connections (PostgreSQL, Redis) are healthy. Returns **200 OK** if everything is reachable, **503 Service Unavailable** if any connection is down.

**Healthy (200 OK):**
```json
{"status": "ok", "database": "connected", "redis": "connected"}
```

**Degraded (503 Service Unavailable):**
```json
{"status": "degraded", "database": "connected", "redis": "disconnected"}
```

Use this as a Kubernetes **liveness probe** — if it fails, the process should be restarted.

### GET /readiness

Readiness probe. Returns **200 OK** only when the indexer is in live sync mode (caught up with the chain). Returns **503 Service Unavailable** during historic sync or initialization.

**Ready (200 OK):**
```json
{"status": "ready", "mode": "live"}
```

**Not ready (503 Service Unavailable):**
```json
{"status": "syncing", "mode": "historical"}
```

Use this as a Kubernetes **readiness probe** — prevents routing traffic to the indexer until it's fully synced.

### GET /sync

Current sync status with source and chain information.

```json
{
  "synced": false,
  "current_block": 10500000,
  "chain_tip": 18000000,
  "percentage": 6.67,
  "mode": "historical",
  "source": "hypersync",
  "chain_id": 1,
  "chain_name": "ethereum",
  "historic_workers": 4
}
```

| Field | Type | Description |
|-------|------|-------------|
| `synced` | boolean | Whether the indexer has caught up to the chain tip. |
| `current_block` | integer | Last indexed block number. During historic sync, this is the minimum block across all workers. |
| `chain_tip` | integer | Latest known block from the chain (derived from DB state). |
| `percentage` | float | Sync progress from `start_block` to `chain_tip` (0-100). |
| `mode` | string | `"historical"`, `"live"`, or `"initializing"`. |
| `source` | string | Data source type: `"rpc"`, `"erpc"`, or `"hypersync"`. |
| `chain_id` | integer | EVM chain ID being indexed. |
| `chain_name` | string | Human-readable chain name. |
| `historic_workers` | integer | Number of active historic sync workers (0 when in live mode). |

### GET /metrics

Prometheus metrics in text exposition format. See [Prometheus Metrics](#prometheus-metrics).

---

## Prometheus Metrics

All metrics are labeled with `chain_id`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kyomei_blocks_indexed_total` | counter | `chain_id`, `phase` | Total blocks indexed. Phase: `historic` or `live`. |
| `kyomei_events_stored_total` | counter | `chain_id` | Total events stored in DB. |
| `kyomei_rpc_requests_total` | counter | `chain_id`, `status` | RPC requests by status: `ok`, `error`, `rate_limited`. |
| `kyomei_rpc_latency_seconds` | histogram | `chain_id` | RPC request latency distribution. |
| `kyomei_sync_current_block` | gauge | `chain_id` | Current block being processed. |
| `kyomei_sync_chain_tip` | gauge | `chain_id` | Latest known chain tip block. |
| `kyomei_consecutive_errors` | gauge | `chain_id` | Current consecutive error count. |
| `kyomei_reorgs_detected_total` | counter | `chain_id` | Total chain reorganizations detected. |
| `kyomei_traces_indexed_total` | counter | `chain_id`, `phase` | Total call traces indexed. Phase: `historic` or `live`. |
| `kyomei_account_events_indexed_total` | counter | `chain_id`, `event_type` | Total account events indexed. |
| `kyomei_db_copy_duration_seconds` | histogram | `chain_id` | Duration of PostgreSQL COPY operations. |

**Example Grafana queries:**

```promql
# Indexing speed (blocks/sec)
rate(kyomei_blocks_indexed_total[5m])

# Sync lag
kyomei_sync_chain_tip - kyomei_sync_current_block

# RPC error rate
rate(kyomei_rpc_requests_total{status="error"}[5m])
  / rate(kyomei_rpc_requests_total[5m])

# P99 RPC latency
histogram_quantile(0.99, rate(kyomei_rpc_latency_seconds_bucket[5m]))

# Trace indexing rate
rate(kyomei_traces_indexed_total[5m])

# Account events by type
sum by (event_type) (rate(kyomei_account_events_indexed_total[5m]))
```

---

## Deployment

### Docker

```dockerfile
# Volumes:
#   /app/config  — mount your config.yaml here
#   /app/abis    — mount your ABI JSON files here
# Port: 8080 (API + metrics)

docker run \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/abis:/app/abis \
  -e DATABASE_URL="postgresql://user:pass@timescaledb:5432/kyomei" \
  -e REDIS_URL="redis://redis:6379" \
  -p 8080:8080 \
  kyomei-indexer
```

### Docker Compose

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_USER: kyomei
      POSTGRES_PASSWORD: kyomei
      POSTGRES_DB: kyomei
    ports:
      - "5432:5432"
    volumes:
      - timescaledb_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  indexer:
    image: kyomei-indexer
    depends_on:
      - timescaledb
      - redis
    environment:
      DATABASE_URL: "postgresql://kyomei:kyomei@timescaledb:5432/kyomei"
      REDIS_URL: "redis://redis:6379"
    volumes:
      - ./config:/app/config
      - ./abis:/app/abis
    ports:
      - "8080:8080"

volumes:
  timescaledb_data:
```

---

## Event Filters

Filter decoded events before storage to reduce database size for high-volume contracts. Filters are configured per-contract and evaluate decoded event parameters.

### Configuration

```yaml
contracts:
  - name: "USDC"
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    abi_path: "./abis/ERC20.json"
    filters:
      - event: "Transfer"
        conditions:
          - field: "value"
            op: "gte"
            value: "1000000"  # Only transfers >= 1 USDC (6 decimals)
      - event: "Approval"
        conditions:
          - field: "value"
            op: "gt"
            value: "0"        # Skip zero-value approvals
```

### Filter Conditions

Each filter specifies an `event` name and a list of `conditions`. All conditions must match for the event to be stored (AND logic). If no filter matches an event name, the event is stored unconditionally.

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | Event name to filter (e.g., `"Transfer"`). |
| `conditions` | list | List of conditions (all must match). |

Each condition:

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Decoded parameter name (e.g., `"value"`, `"from"`). |
| `op` | string | Comparison operator. |
| `value` | string | Value to compare against. |

### Supported Operators

| Operator | Description | Comparison Type |
|----------|-------------|-----------------|
| `eq` | Equal | Numeric if both parseable, otherwise string |
| `neq` | Not equal | Numeric if both parseable, otherwise string |
| `gt` | Greater than | Numeric if both parseable, otherwise string |
| `gte` | Greater than or equal | Numeric if both parseable, otherwise string |
| `lt` | Less than | Numeric if both parseable, otherwise string |
| `lte` | Less than or equal | Numeric if both parseable, otherwise string |
| `contains` | String contains | Always string |

Numeric comparison uses 128-bit integers, suitable for most Solidity `uint` types. Values that cannot be parsed as numbers fall back to string comparison.

---

## View Function Indexing

Capture contract state that doesn't emit events by periodically calling view (read-only) functions and storing the results. Useful for tracking `totalSupply`, `balanceOf`, reserve amounts, or any other contract state.

### Configuration

```yaml
contracts:
  - name: "USDC"
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    abi_path: "./abis/ERC20.json"
    views:
      - function: "totalSupply"
        interval_blocks: 100       # Call every 100 blocks
      - function: "balanceOf"
        interval_blocks: 1000
        params: ["0xdead000000000000000000000000000000000000"]
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `function` | string | *required* | Solidity function name (e.g., `"totalSupply"`). |
| `interval_blocks` | integer | *required* | Call the function every N blocks. |
| `params` | list | `[]` | Static parameters as hex-encoded strings (each zero-padded to 32 bytes). |

### How It Works

1. During live sync, after processing each block's events, the indexer checks if `block_number % interval_blocks == 0` for each configured view function.
2. If triggered, it sends an `eth_call` JSON-RPC request at that block number.
3. The raw hex result is stored in a per-function table in the sync schema.

### Storage

Each view function gets its own table:

```sql
{sync_schema}.view_{contract_name}_{function_name} (
    chain_id        INTEGER NOT NULL,
    block_number    BIGINT NOT NULL,
    block_timestamp BIGINT NOT NULL,
    address         TEXT NOT NULL,
    result          TEXT NOT NULL,       -- Raw hex result from eth_call
    PRIMARY KEY (chain_id, block_number, address)
)
```

### Requirements

- The contract must have a fixed `address` (factory contracts without a fixed address are skipped).
- An RPC URL must be available. For HyperSync sources, set `fallback_rpc` to provide an RPC endpoint for `eth_call`.

---

## Webhook Notifications

POST decoded events to HTTP endpoints for event-driven architectures. Webhooks fire asynchronously (fire-and-forget) so they don't block the sync pipeline.

### Configuration

```yaml
webhooks:
  - url: "https://api.example.com/events"
    events: ["Transfer", "Swap"]     # Optional filter, empty = all events
    retry_attempts: 3
    timeout_secs: 10
    headers:
      Authorization: "Bearer ${WEBHOOK_TOKEN}"
      Content-Type: "application/json"

  - url: "https://backup.example.com/events"
    # No events filter — receives all decoded events
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | *required* | HTTP endpoint to POST events to. |
| `events` | list | `[]` | Event name filter. Empty = all events. |
| `retry_attempts` | integer | `3` | Max retry attempts on failure. |
| `timeout_secs` | integer | `10` | Request timeout in seconds. |
| `headers` | map | `{}` | Custom HTTP headers (e.g., authentication). |

### Payload Format

Events are sent as a JSON array:

```json
[
  {
    "chain_id": 1,
    "block_number": 18000000,
    "block_timestamp": 1700000000,
    "tx_hash": "0xabc...",
    "log_index": 12,
    "address": "0xa0b8...",
    "contract_name": "ERC20",
    "event_name": "Transfer",
    "params": {
      "from": "0xaaaa...",
      "to": "0xbbbb...",
      "value": "1000000"
    }
  }
]
```

### Retry Behavior

- **5xx errors**: Retried with exponential backoff (500ms, 1s, 2s, ...).
- **4xx errors**: Not retried (permanent client error).
- **Network errors/timeouts**: Retried with exponential backoff.
- Delivery is fire-and-forget via `tokio::spawn` — failures are logged but don't block indexing.

---

## CSV Export

Write decoded events to per-event-type CSV files for data pipelines, BigQuery imports, or offline analysis.

### Configuration

```yaml
export:
  csv:
    enabled: true
    output_dir: "./data/csv"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable CSV export. |
| `output_dir` | string | `"./data/csv"` | Output directory for CSV files. Created automatically if it doesn't exist. |

### Output Format

One CSV file per event type, named `{table_name}.csv` (e.g., `event_erc20_transfer.csv`).

Headers are written on the first write. Subsequent writes append data rows. Files include all standard columns (`chain_id`, `block_number`, `tx_index`, `log_index`, `block_hash`, `block_timestamp`, `tx_hash`, `address`) plus decoded parameter columns.

CSV writes happen asynchronously via `spawn_blocking` to avoid blocking the async runtime.

---

## Call Trace Indexing

Index function calls (not just events) via `debug_traceBlockByNumber` or `trace_block`. Captures calldata, return values, gas usage, and call tree position for every internal and external call.

### Configuration

```yaml
traces:
  enabled: true
  method: "debug"          # "debug" or "parity"
  tracer: "callTracer"     # tracer config (debug method only)
  contracts:
    - name: "UniswapV2Router"
      address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
      abi_path: "./abis/UniswapV2Router.json"  # optional, enables function decoding
      start_block: 10000835
    - name: "UniswapV2Pair"
      factory_ref: "UniswapV2Pair"  # use dynamic addresses from factory discovery
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable call trace indexing. |
| `method` | string | `"debug"` | RPC method: `"debug"` uses `debug_traceBlockByNumber`, `"parity"` uses `trace_block`. |
| `tracer` | string | `"callTracer"` | Tracer configuration for debug method. |
| `contracts[].name` | string | required | Human-readable contract name. |
| `contracts[].address` | string | optional | Fixed contract address to filter traces by. |
| `contracts[].factory_ref` | string | optional | Reference to a top-level `contracts[]` entry for dynamic addresses (factory-discovered). |
| `contracts[].abi_path` | string | optional | Path to ABI JSON file. Enables function name decoding via 4-byte selector matching. |
| `contracts[].start_block` | integer | optional | Override start block for this contract's traces. |

### Trace Methods

**Debug (`debug_traceBlockByNumber`):**
- Uses Geth's `callTracer` to produce a nested call tree per transaction
- Kyomei flattens the tree into records with `trace_address` paths (e.g., `"0.1.3"`)
- Requires an archive node with debug API enabled
- Supported by Geth, Erigon, Reth, and most RPC providers (Alchemy, QuickNode, Infura)

**Parity (`trace_block`):**
- Uses OpenEthereum/Parity's trace API which returns a flat array
- Supported by Erigon, Nethermind, and Alchemy
- No nested flattening needed — traces arrive pre-flattened

### Database Schema

Traces are stored in `{data_schema}.raw_traces`:

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | INTEGER | Chain identifier |
| `block_number` | BIGINT | Block number |
| `tx_index` | INTEGER | Transaction index within block |
| `trace_address` | TEXT | Position in call tree (e.g., `"0.1.3"`) |
| `tx_hash` | TEXT | Transaction hash |
| `block_hash` | TEXT | Block hash |
| `block_timestamp` | BIGINT | Unix timestamp |
| `call_type` | TEXT | `call`, `delegatecall`, `staticcall`, `create`, `create2` |
| `from_address` | TEXT | Caller address |
| `to_address` | TEXT | Callee address |
| `input` | TEXT | Calldata (hex) |
| `output` | TEXT | Return data (hex) |
| `value` | NUMERIC | ETH value transferred (wei) |
| `gas` | BIGINT | Gas provided |
| `gas_used` | BIGINT | Gas consumed |
| `error` | TEXT | Error message if call reverted |
| `function_name` | TEXT | Decoded function name (if ABI provided) |
| `function_sig` | TEXT | 4-byte function selector |

Primary key: `(chain_id, block_number, tx_index, trace_address)`. Stored as a TimescaleDB hypertable partitioned by `block_number`.

Indexes: `(chain_id, to_address, block_number)` for contract lookups, `(chain_id, function_sig, block_number)` for function-specific queries.

### Function Decoding

When `abi_path` is provided for a traced contract, Kyomei automatically:
1. Parses function definitions from the ABI (ignoring events)
2. Computes 4-byte selectors (keccak256 of function signature)
3. Matches each trace's input calldata against known selectors
4. Populates `function_name` and `function_sig` on matching traces

This enables queries like:
```sql
SELECT * FROM uniswap_v2.raw_traces
WHERE function_name = 'swapExactTokensForTokens'
ORDER BY block_number DESC
LIMIT 100;
```

### User-Schema View

When traces are enabled, a view is created at `{user_schema}.raw_traces` filtered to the configured `chain_id`.

### Sync Behavior

- **Historic sync**: Processes blocks sequentially in batches of 10 (trace API calls are expensive — one RPC call per block)
- **Live sync**: Traces are fetched per-block alongside event indexing
- **Reorg handling**: Traces in rolled-back blocks are automatically deleted

---

## Account/Transaction Tracking

Track transactions and native ETH transfers for specific addresses. Generates four event types:

| Event Type | Description |
|------------|-------------|
| `transaction:from` | Address sent a transaction (as `tx.from`) |
| `transaction:to` | Address received a transaction (as `tx.to`) |
| `transfer:from` | Address sent native ETH via internal call (requires traces) |
| `transfer:to` | Address received native ETH via internal call (requires traces) |

### Configuration

```yaml
accounts:
  enabled: true
  addresses:
    - name: "Treasury"
      address: "0xdead000000000000000000000000000000000001"
      start_block: 10000835
      events: ["transaction:from", "transaction:to", "transfer:from", "transfer:to"]
    - name: "HotWallet"
      address: "0x1234567890abcdef1234567890abcdef12345678"
      events: ["transaction:from", "transaction:to"]
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable account tracking. |
| `addresses[].name` | string | required | Human-readable label for the address. |
| `addresses[].address` | string | required | Ethereum address to track. |
| `addresses[].start_block` | integer | optional | Override start block for this address. |
| `addresses[].events` | string[] | all four types | Which event types to track. |

### Transfer Events and Traces

`transfer:from` and `transfer:to` events capture internal ETH transfers (e.g., a contract forwarding ETH via `call`). These require call trace indexing to also be enabled, because internal transfers are extracted from trace data. If you configure transfer events without enabling traces, Kyomei logs a warning at startup.

`transaction:from` and `transaction:to` only require standard block data and work without traces.

### Database Schema

Account events are stored in `{data_schema}.raw_account_events`:

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | INTEGER | Chain identifier |
| `block_number` | BIGINT | Block number |
| `tx_index` | INTEGER | Transaction index within block |
| `event_type` | TEXT | One of `transaction:from`, `transaction:to`, `transfer:from`, `transfer:to` |
| `address` | TEXT | The tracked address |
| `tx_hash` | TEXT | Transaction hash |
| `block_hash` | TEXT | Block hash |
| `block_timestamp` | BIGINT | Unix timestamp |
| `counterparty` | TEXT | The other address in the transaction/transfer |
| `value` | NUMERIC | ETH value (wei) |
| `input` | TEXT | Transaction calldata (for transaction events) |
| `trace_address` | TEXT | Call tree position (for transfer events) |

Primary key: `(chain_id, block_number, tx_index, event_type, address)`. Stored as a TimescaleDB hypertable.

Indexes: `(chain_id, address, block_number)` for address lookups, `(chain_id, address, event_type, block_number)` for filtered queries.

### User-Schema View

When accounts are enabled, a view is created at `{user_schema}.account_events` filtered to the configured `chain_id`.

### Example Queries

```sql
-- All outgoing transactions from Treasury
SELECT * FROM uniswap_v2.account_events
WHERE address = '0xdead000000000000000000000000000000000001'
  AND event_type = 'transaction:from'
ORDER BY block_number DESC;

-- Total ETH received via internal transfers
SELECT SUM(value) FROM uniswap_v2.account_events
WHERE address = '0xdead000000000000000000000000000000000001'
  AND event_type = 'transfer:to';
```

---

## Continuous Aggregates

Leverage TimescaleDB's continuous aggregates to create auto-refreshing materialized views with time-bucketed rollups. TimescaleDB handles refresh automatically — no application code needed. Every decoded event table automatically gets `event_count` and `tx_count` aggregates, and you can define **custom aggregate expressions** over any decoded parameter column.

### Configuration

```yaml
aggregations:
  enabled: true
  intervals: ["1h", "1d"]
  custom:
    - table: "event_uniswap_v2_pair_swap"
      metrics:
        - name: "total_amount0_in"
          expr: "SUM(p_amount0_in::numeric)"
        - name: "unique_senders"
          expr: "COUNT(DISTINCT p_sender)"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable continuous aggregates. |
| `intervals` | list | `["1h", "1d"]` | Time bucket intervals to create. |
| `custom` | list | `[]` | Custom aggregate definitions per table. |

### Supported Intervals

| Interval | Suffix | Refresh Schedule |
|----------|--------|-----------------|
| `15m` | `_15m` | Every 15 minutes |
| `30m` | `_30m` | Every 30 minutes |
| `1h` | `_hourly` | Every hour |
| `4h` | `_4h` | Every 4 hours |
| `1d` | `_daily` | Every day |
| `1w` | `_weekly` | Every week |

### What Gets Created

For each decoded event table and each configured interval, the indexer creates:

1. **A continuous aggregate** in the `sync_schema`:
   ```sql
   {sync_schema}.{event_table}_{suffix}
   ```
   Default columns: `chain_id`, `address`, `bucket` (time), `event_count`, `tx_count`, plus any custom metric columns.

2. **A refresh policy** with appropriate start/end offsets.

3. **A user schema view** pointing to the aggregate for easy querying.

### Understanding Decoded Parameter Columns

When the indexer decodes events from an ABI, it creates columns in the decoded event table with a `p_` prefix. Each Solidity parameter type maps to a PostgreSQL column:

| Solidity Type | Column Name | PG Type | Example Value |
|---------------|-------------|---------|---------------|
| `address` | `p_{name}` | `TEXT` | `0xdead...beef` |
| `uint256` | `p_{name}` | `TEXT` | `1000000000000000000` (stored as string, cast to `numeric` for math) |
| `int256` | `p_{name}` | `TEXT` | `-500` |
| `bool` | `p_{name}` | `TEXT` | `true` / `false` |
| `bytes32` | `p_{name}` | `TEXT` | `0xabcdef...` |
| `string` | `p_{name}` | `TEXT` | `Hello World` |

All decoded values are stored as `TEXT`. To perform numeric aggregation, cast them: `p_amount::numeric`, `p_value::numeric`.

### Custom Aggregate Expression Reference

Custom metrics use SQL aggregate expressions that run inside the continuous aggregate's `SELECT` clause, grouped by `chain_id, address, bucket`. Here is a comprehensive guide to what's possible:

#### Basic Numeric Aggregation

The most common use case — aggregate numeric event parameters (amounts, prices, gas, etc.):

```yaml
custom:
  - table: "event_erc20_transfer"
    metrics:
      # Total tokens transferred per time bucket
      - name: "total_transferred"
        expr: "SUM(p_value::numeric)"
      # Average transfer size
      - name: "avg_transfer"
        expr: "AVG(p_value::numeric)"
      # Largest single transfer
      - name: "max_transfer"
        expr: "MAX(p_value::numeric)"
      # Smallest non-zero transfer
      - name: "min_transfer"
        expr: "MIN(NULLIF(p_value::numeric, 0))"
```

#### Counting Unique Addresses

Track unique participants — senders, receivers, liquidity providers:

```yaml
custom:
  - table: "event_erc20_transfer"
    metrics:
      - name: "unique_senders"
        expr: "COUNT(DISTINCT p_from)"
      - name: "unique_receivers"
        expr: "COUNT(DISTINCT p_to)"
      # Total unique addresses (either side)
      # Note: this counts senders only; for both sides, use two separate metrics
```

#### DEX Swap Volume Analytics

For Uniswap V2-style swap events with `amount0In`, `amount1In`, `amount0Out`, `amount1Out`:

```yaml
custom:
  - table: "event_uniswap_v2_pair_swap"
    metrics:
      # Total volume per token side
      - name: "total_amount0_in"
        expr: "SUM(p_amount0_in::numeric)"
      - name: "total_amount0_out"
        expr: "SUM(p_amount0_out::numeric)"
      - name: "total_amount1_in"
        expr: "SUM(p_amount1_in::numeric)"
      - name: "total_amount1_out"
        expr: "SUM(p_amount1_out::numeric)"
      # Number of unique traders
      - name: "unique_traders"
        expr: "COUNT(DISTINCT p_sender)"
      # Total swap count (same as event_count, but explicit)
      - name: "swap_count"
        expr: "COUNT(*)"
      # Max single-swap volume (useful for whale detection)
      - name: "max_swap_amount0"
        expr: "MAX(p_amount0_in::numeric + p_amount0_out::numeric)"
```

#### Liquidity Events (Mint/Burn)

Track liquidity additions and removals:

```yaml
custom:
  - table: "event_uniswap_v2_pair_mint"
    metrics:
      - name: "total_amount0_added"
        expr: "SUM(p_amount0::numeric)"
      - name: "total_amount1_added"
        expr: "SUM(p_amount1::numeric)"
      - name: "unique_providers"
        expr: "COUNT(DISTINCT p_sender)"

  - table: "event_uniswap_v2_pair_burn"
    metrics:
      - name: "total_amount0_removed"
        expr: "SUM(p_amount0::numeric)"
      - name: "total_amount1_removed"
        expr: "SUM(p_amount1::numeric)"
      - name: "unique_removers"
        expr: "COUNT(DISTINCT p_sender)"
```

#### Conditional Aggregation with FILTER

Use `FILTER (WHERE ...)` to aggregate only rows matching a condition. This is powerful for splitting metrics by criteria:

```yaml
custom:
  - table: "event_uniswap_v2_pair_swap"
    metrics:
      # Count only "buy" swaps (amount0In > 0)
      - name: "buy_count"
        expr: "COUNT(*) FILTER (WHERE p_amount0_in::numeric > 0)"
      # Count only "sell" swaps (amount1In > 0)
      - name: "sell_count"
        expr: "COUNT(*) FILTER (WHERE p_amount1_in::numeric > 0)"
      # Volume from buys only
      - name: "buy_volume"
        expr: "SUM(p_amount0_in::numeric) FILTER (WHERE p_amount0_in::numeric > 0)"
      # Volume from sells only
      - name: "sell_volume"
        expr: "SUM(p_amount1_in::numeric) FILTER (WHERE p_amount1_in::numeric > 0)"
```

#### Boolean and Categorical Aggregation

For events with boolean or string parameters:

```yaml
custom:
  - table: "event_governance_vote_cast"
    metrics:
      # Count votes for/against
      - name: "votes_for"
        expr: "COUNT(*) FILTER (WHERE p_support = 'true')"
      - name: "votes_against"
        expr: "COUNT(*) FILTER (WHERE p_support = 'false')"
      # Total voting power
      - name: "total_weight"
        expr: "SUM(p_weight::numeric)"
      # Unique voters
      - name: "unique_voters"
        expr: "COUNT(DISTINCT p_voter)"
```

#### Derived Calculations

Combine multiple columns in a single expression:

```yaml
custom:
  - table: "event_uniswap_v2_pair_swap"
    metrics:
      # Net flow (in minus out) per side
      - name: "net_amount0_flow"
        expr: "SUM(p_amount0_in::numeric) - SUM(p_amount0_out::numeric)"
      # Ratio of buy to sell volume (guard against divide-by-zero)
      - name: "total_gross_volume_0"
        expr: "SUM(p_amount0_in::numeric + p_amount0_out::numeric)"
```

#### Working with Token Decimals

On-chain amounts are raw integers (e.g., 1 USDC = `1000000`, 1 ETH = `10^18`). Scale in your expression:

```yaml
custom:
  - table: "event_erc20_transfer"
    metrics:
      # USDC (6 decimals): divide by 10^6 for human-readable
      - name: "total_usdc_transferred"
        expr: "SUM(p_value::numeric / 1e6)"
      # ETH/WETH (18 decimals)
      - name: "total_eth_transferred"
        expr: "SUM(p_value::numeric / 1e18)"
```

> **Note:** Decimal scaling is specific to each token contract. If your aggregate covers multiple tokens (e.g., a factory with many ERC-20s), the scaling must be uniform or the metric won't make sense across tokens.

#### Practical Patterns for Multi-Pool Analytics

When indexing a factory contract (e.g., UniswapV2Factory), the continuous aggregate groups by `address` (each pool). You can query across all pools or filter:

```sql
-- Top 10 pools by hourly swap count
SELECT address, bucket, event_count, total_amount0_in
FROM my_project.event_uniswap_v2_pair_swap_hourly
WHERE bucket > now() - interval '24 hours'
ORDER BY event_count DESC
LIMIT 10;

-- Daily volume across ALL pools
SELECT bucket, SUM(total_amount0_in) AS total_volume, SUM(swap_count) AS total_swaps
FROM my_project.event_uniswap_v2_pair_swap_daily
GROUP BY bucket
ORDER BY bucket DESC;

-- Compare two pools side by side
SELECT bucket,
  SUM(total_amount0_in) FILTER (WHERE address = '0xpool1...') AS pool1_volume,
  SUM(total_amount0_in) FILTER (WHERE address = '0xpool2...') AS pool2_volume
FROM my_project.event_uniswap_v2_pair_swap_hourly
WHERE bucket > now() - interval '7 days'
GROUP BY bucket
ORDER BY bucket;
```

### Expression Rules and Limitations

1. **Must be valid PostgreSQL aggregate functions.** The expression runs inside `GROUP BY chain_id, address, bucket`. Functions like `SUM()`, `AVG()`, `MIN()`, `MAX()`, `COUNT()`, `COUNT(DISTINCT ...)` are safe.

2. **Column references must use the `p_` prefix.** All decoded event parameters are stored with a `p_` prefix (e.g., Solidity `amount0In` → SQL column `p_amount0_in`). The base event columns (`chain_id`, `block_number`, `tx_hash`, `address`, `block_timestamp`, etc.) are also available.

3. **Cast text to numeric for math.** All decoded values are `TEXT`. Use `::numeric` for arithmetic: `SUM(p_value::numeric)`.

4. **Use `NULLIF` to handle zero values.** `MIN(NULLIF(p_amount::numeric, 0))` avoids treating zero as the minimum.

5. **`FILTER (WHERE ...)` for conditional aggregation.** Supported in PostgreSQL 9.4+ and works in continuous aggregates.

6. **Avoid window functions.** Window functions (`ROW_NUMBER()`, `LAG()`, etc.) are not supported inside continuous aggregates. Use them in queries over the aggregate views instead.

7. **Avoid subqueries.** Continuous aggregate definitions don't support subqueries. Keep expressions as simple aggregate function calls over columns.

8. **Table names follow the pattern:** `event_{contract_snake_case}_{event_snake_case}`. For example, contract `UniswapV2Pair` event `Swap` → table `event_uniswap_v2_pair_swap`.

### Example Queries Over Aggregated Data

```sql
-- Hourly transfer counts for USDC (basic, no custom metrics)
SELECT bucket, event_count, tx_count
FROM my_project.event_usdc_transfer_hourly
WHERE bucket > now() - interval '7 days'
ORDER BY bucket DESC;

-- Daily volume trend with custom metrics
SELECT
  bucket,
  total_amount0_in / 1e18 AS volume_eth,
  unique_traders,
  swap_count
FROM my_project.event_uniswap_v2_pair_swap_daily
WHERE bucket > now() - interval '30 days'
ORDER BY bucket;

-- Buy/sell ratio per hour (using conditional aggregates)
SELECT
  bucket,
  buy_count,
  sell_count,
  CASE WHEN sell_count > 0
    THEN buy_count::float / sell_count
    ELSE NULL
  END AS buy_sell_ratio
FROM my_project.event_uniswap_v2_pair_swap_hourly
WHERE bucket > now() - interval '7 days'
ORDER BY bucket;

-- Weekly unique participants across all pools
SELECT
  bucket,
  SUM(unique_traders) AS total_unique_traders,
  SUM(swap_count) AS total_swaps
FROM my_project.event_uniswap_v2_pair_swap_weekly
GROUP BY bucket
ORDER BY bucket;
```

### Requirements

Requires TimescaleDB extension. If TimescaleDB is not installed, aggregate creation will fail gracefully with a warning. Custom metric expressions are validated at SQL execution time — if a column doesn't exist or the expression is invalid, the aggregate creation will log a warning and skip that table.

---

## Multi-Chain Mode

Run a single indexer process for multiple EVM chains. All chains share the same database and Redis connection, with per-chain schemas, sources, and contracts.

### Configuration

Multi-chain mode uses a different YAML format with a top-level `chains` key. The indexer auto-detects the format — if `chains` is present, it runs in multi-chain mode.

```yaml
database:
  connection_string: "${DATABASE_URL}"

redis:
  url: "${REDIS_URL}"

api:
  host: "0.0.0.0"
  port: 8080

logging:
  level: "info"
  format: "json"

chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_sync"
      user_schema: "eth"
    source:
      type: rpc
      url: "${ETH_RPC}"
    sync:
      start_block: 18000000
      parallel_workers: 4
    contracts:
      - name: "USDC"
        address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        abi_path: "./abis/ERC20.json"
    aggregations:
      enabled: true
      intervals: ["1h", "1d"]

  - chain:
      id: 137
      name: "polygon"
    schema:
      sync_schema: "poly_sync"
      user_schema: "poly"
    source:
      type: hypersync
      fallback_rpc: "https://polygon-rpc.com"
    sync:
      start_block: 50000000
    contracts:
      - name: "USDC"
        address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        abi_path: "./abis/ERC20.json"
```

### Shared vs Per-Chain Configuration

| Shared (top-level) | Per-Chain (under `chains[]`) |
|--------------------|-----------------------------|
| `database` | `chain` (id, name) |
| `redis` | `schema` (sync_schema, user_schema) |
| `api` | `source` (type, url, etc.) |
| `logging` | `sync` (start_block, workers, etc.) |
| | `contracts` |
| | `reorg` |
| | `aggregations` |
| | `export` |
| | `webhooks` |

### How It Works

1. **Startup**: The indexer loads all chain configs, runs migrations and creates views for each chain.
2. **Sync**: Each chain gets its own `ChainSyncer` running in a separate `tokio::spawn` task. Chains sync independently and in parallel.
3. **Shared resources**: All chains share the database pool and Redis connection. Each chain writes to its own `sync_schema`.
4. **Shutdown**: A single SIGINT/SIGTERM signal gracefully stops all chain syncers.

### API Changes in Multi-Chain Mode

#### GET /sync

Returns an **array** of sync statuses (one per chain):

```json
[
  {
    "chain_id": 1,
    "chain_name": "ethereum",
    "synced": true,
    "current_block": 18000000,
    "chain_tip": 18000000,
    "percentage": 100.0,
    "mode": "live",
    "source": "rpc",
    "historic_workers": 0
  },
  {
    "chain_id": 137,
    "chain_name": "polygon",
    "synced": false,
    "current_block": 55000000,
    "chain_tip": 60000000,
    "percentage": 50.0,
    "mode": "historical",
    "source": "hypersync",
    "historic_workers": 4
  }
]
```

Filter by chain: `GET /sync?chain_id=1` returns only that chain's status.

#### GET /readiness

Returns **200 OK** only when **all** chains are in live sync mode. If some chains are live but others are still syncing, returns **503** with `"mode": "partial"`.

### Backwards Compatibility

Single-chain YAML configs (without `chains` key) continue to work exactly as before. The indexer auto-detects the format.

---

## Configuration Examples

### Example 1: Simple ERC-20 Token Tracking

Index Transfer and Approval events from a single token contract.

```yaml
database:
  connection_string: "${DATABASE_URL}"

redis:
  url: "${REDIS_URL}"

schema:
  sync_schema: "token_tracker_sync"
  user_schema: "token_tracker"

chain:
  id: 1
  name: "ethereum"

source:
  type: "rpc"
  url: "${RPC_URL}"

sync:
  start_block: 18000000
  parallel_workers: 2
  blocks_per_request: 5000

contracts:
  - name: "USDC"
    address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    abi_path: "./abis/ERC20.json"

logging:
  level: "info"
  format: "pretty"
```

### Example 2: Uniswap V2 Factory + Pairs (HyperSync)

Fast historical indexing of Uniswap V2 with automatic pair discovery.

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 30

redis:
  url: "${REDIS_URL}"
  batch_notification_interval: 5000

schema:
  sync_schema: "uniswap_v2_sync"
  user_schema: "uniswap_v2"

chain:
  id: 1
  name: "ethereum"

source:
  type: "hypersync"
  api_token: "${HYPERSYNC_API_TOKEN}"

sync:
  start_block: 10000835
  parallel_workers: 8
  blocks_per_request: 50000
  blocks_per_worker: 500000
  checkpoint_interval: 1000

contracts:
  - name: "UniswapV2Factory"
    address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    abi_path: "./abis/UniswapV2Factory.json"

  - name: "UniswapV2Pair"
    factory:
      address: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
      event: "PairCreated"
      parameter: "pair"
    abi_path: "./abis/UniswapV2Pair.json"

reorg:
  max_reorg_depth: 1000

api:
  port: 9090

logging:
  level: "info"
  format: "json"
```

### Example 3: Polygon DEX with eRPC Proxy

Using eRPC for enhanced reliability on Polygon with aggressive parallelism.

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 40

redis:
  url: "${REDIS_URL}"

schema:
  sync_schema: "quickswap_sync"
  user_schema: "quickswap"

chain:
  id: 137
  name: "polygon"

source:
  type: "erpc"
  url: "http://erpc:4000"
  project_id: "polygon-indexer"
  fallback_rpc: "https://polygon.llamarpc.com"

sync:
  start_block: 25000000
  parallel_workers: 16
  blocks_per_request: 20000
  blocks_per_worker: 1000000
  checkpoint_interval: 2000
  retry:
    max_retries: 3
    initial_backoff_ms: 500
    max_backoff_ms: 10000

contracts:
  - name: "QuickSwapFactory"
    address: "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32"
    abi_path: "./abis/UniswapV2Factory.json"

  - name: "QuickSwapPair"
    factory:
      address: "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32"
      event: "PairCreated"
      parameter: "pair"
    abi_path: "./abis/UniswapV2Pair.json"

logging:
  level: "info"
  format: "json"
```

### Example 4: Multi-Contract Arbitrum Indexer (Conservative)

Conservative settings for a paid RPC with rate limits.

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 10

redis:
  url: "${REDIS_URL}"

schema:
  sync_schema: "arb_defi_sync"
  user_schema: "arb_defi"

chain:
  id: 42161
  name: "arbitrum"

source:
  type: "rpc"
  url: "${ARB_RPC_URL}"
  fallback_rpc: "https://arb1.arbitrum.io/rpc"

sync:
  start_block: 100000000
  parallel_workers: 2
  blocks_per_request: 2000
  blocks_per_worker: 100000
  max_concurrent_requests: 3
  checkpoint_interval: 200
  retry:
    max_retries: 10
    initial_backoff_ms: 2000
    max_backoff_ms: 60000
    validate_logs_bloom: true
    bloom_validation_retries: 5

contracts:
  - name: "GMXVault"
    address: "0x489ee077994B6658eAfA855C308275EAd8097C4A"
    abi_path: "./abis/GMXVault.json"

  - name: "CamelotFactory"
    address: "0x6EcCab422D763aC031210895C81787E87B43A652"
    abi_path: "./abis/CamelotFactory.json"

  - name: "CamelotPair"
    factory:
      address: "0x6EcCab422D763aC031210895C81787E87B43A652"
      event: "PairCreated"
      parameter: "pair"
    abi_path: "./abis/CamelotPair.json"

reorg:
  max_reorg_depth: 500

logging:
  level: "warn"
  format: "json"
```

### Example 5: L2 High-Throughput (Base)

Maximum throughput for a low-cost L2 chain.

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 50

redis:
  url: "${REDIS_URL}"
  batch_notification_interval: 10000

schema:
  sync_schema: "base_dex_sync"
  user_schema: "base_dex"

chain:
  id: 8453
  name: "base"

source:
  type: "hypersync"
  fallback_rpc: "${BASE_RPC_URL}"

sync:
  start_block: 1000000
  parallel_workers: 32
  blocks_per_request: 100000
  blocks_per_worker: 2000000
  max_concurrent_requests: 64
  checkpoint_interval: 5000
  retry:
    max_retries: 3
    initial_backoff_ms: 500
    max_backoff_ms: 5000

contracts:
  - name: "AerodromeFactory"
    address: "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"
    abi_path: "./abis/AerodromeFactory.json"

  - name: "AerodromePool"
    factory:
      address: "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"
      event: "PoolCreated"
      parameter: "pool"
    abi_path: "./abis/AerodromePool.json"

logging:
  level: "info"
  format: "json"
```

### Example 6: Multi-Chain USDC Tracker with Filters and Webhooks

Index USDC Transfer events across Ethereum and Polygon, filtering small transfers and sending large ones to a webhook.

```yaml
database:
  connection_string: "${DATABASE_URL}"
  pool_size: 30

redis:
  url: "${REDIS_URL}"

api:
  host: "0.0.0.0"
  port: 8080

logging:
  level: "info"
  format: "json"

chains:
  - chain:
      id: 1
      name: "ethereum"
    schema:
      sync_schema: "eth_usdc_sync"
      user_schema: "eth_usdc"
    source:
      type: rpc
      url: "${ETH_RPC}"
    sync:
      start_block: 18000000
      parallel_workers: 4
    contracts:
      - name: "USDC"
        address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        abi_path: "./abis/ERC20.json"
        filters:
          - event: "Transfer"
            conditions:
              - field: "value"
                op: "gte"
                value: "1000000000"   # >= 1000 USDC
        views:
          - function: "totalSupply"
            interval_blocks: 100
    webhooks:
      - url: "https://api.example.com/eth-transfers"
        events: ["Transfer"]
        headers:
          Authorization: "Bearer ${WEBHOOK_TOKEN}"
    aggregations:
      enabled: true
      intervals: ["1h", "1d"]

  - chain:
      id: 137
      name: "polygon"
    schema:
      sync_schema: "poly_usdc_sync"
      user_schema: "poly_usdc"
    source:
      type: hypersync
      fallback_rpc: "https://polygon-rpc.com"
    sync:
      start_block: 50000000
      parallel_workers: 8
    contracts:
      - name: "USDC"
        address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        abi_path: "./abis/ERC20.json"
        filters:
          - event: "Transfer"
            conditions:
              - field: "value"
                op: "gte"
                value: "1000000000"
    export:
      csv:
        enabled: true
        output_dir: "./data/polygon-csv"
```

---

## Crash Recovery and Decoded Table Rebuild

### Normal restart behavior

On restart, the indexer reads worker checkpoints from the `sync_workers` table in the `sync_schema` and resumes from where each worker left off. Raw events use ON CONFLICT DO NOTHING, so any overlap between the checkpoint and the last actual block processed is safely handled without duplicates.

Progress percentage on resume is calculated from the **original config `start_block`**, not the resume point. If you stopped at 20% progress, the indexer resumes showing ~20% and counts up from there.

### Auto-rebuild for new sync_schema

When the indexer starts and detects that the `sync_schema` has **no workers** but the `data_schema` has **existing raw_events**, it automatically rebuilds decoded tables from the raw data. This happens transparently on startup — no manual action required.

This enables zero-downtime deployments: deploy with a new `sync_schema` name and the indexer bootstraps itself from existing raw data without re-fetching from RPC.

### The decoded table gap

During **historic sync**, raw event inserts and decoded event inserts are separate operations (not wrapped in a transaction, since COPY protocol doesn't support transactions well at high throughput). If the process crashes between a raw event insert and the corresponding decoded insert, the raw events will exist but decoded rows will be missing. On resume, the worker continues from its checkpoint — it won't go back to re-decode the already-stored raw events.

During **live sync**, this is not an issue because raw + decoded + checkpoint are wrapped in a single atomic transaction.

### Rebuilding decoded tables

The `--rebuild-decoded` flag reads all raw events from the database and re-decodes them into the per-event tables in the current `sync_schema`:

```bash
kyomei-indexer --config config.yaml --rebuild-decoded
```

This is useful when:
- **After a crash** that left decoded tables with gaps
- **After an ABI change** — if you update a contract's ABI, run rebuild to re-decode all events with the new ABI
- **After adding a new contract** — if you add a contract whose events already exist in `raw_events` (e.g., same event signature from a different contract)

The rebuild:
- Reads raw_events from `data_schema` in batches (using `blocks_per_request` from config)
- Re-decodes each event into `sync_schema` tables using the current ABI
- Uses ON CONFLICT DO NOTHING — already-decoded rows are skipped
- Exits after completion (does not start the syncer)
- Logs progress every 10%

---

## Zero-Downtime Deployments

The 3-schema architecture (`data_schema`, `sync_schema`, `user_schema`) enables zero-downtime deployments, similar to Ponder's approach.

### How it works

```
┌─────────────────────────────────────────────────┐
│  data_schema (kyomei_data) — SHARED             │
│  ┌─────────────┐  ┌──────────────────┐          │
│  │ raw_events  │  │ factory_children │          │
│  └─────────────┘  └──────────────────┘          │
└───────────┬─────────────────┬───────────────────┘
            │                 │
    ┌───────┴───────┐ ┌──────┴────────┐
    │ sync_schema_v1│ │ sync_schema_v2│
    │ (old deploy)  │ │ (new deploy)  │
    │ decoded tables│ │ decoded tables│
    │ sync_workers  │ │ sync_workers  │
    └───────┬───────┘ └──────┬────────┘
            │                │
            └──────┬─────────┘
                   │
    ┌──────────────┴──────────────┐
    │  user_schema (views)        │
    │  Points to active sync      │
    │  schema's decoded tables    │
    └─────────────────────────────┘
```

### Deployment steps (Kubernetes)

1. **Current state**: Indexer v1 running with `sync_schema: "myapp_sync_v1"`, fully synced in live mode.

2. **Deploy v2**: Start a new indexer pod with `sync_schema: "myapp_sync_v2"` (same `data_schema` and `user_schema`).

3. **Auto-bootstrap**: v2 detects the empty `sync_schema` and auto-rebuilds decoded tables from existing `raw_events` — no RPC requests needed for historical data.

4. **Concurrent sync**: Both indexers run simultaneously. They share `data_schema` safely (ON CONFLICT DO NOTHING on `raw_events` and `factory_children`). Each writes decoded events to its own `sync_schema`.

5. **Wait for readiness**: Monitor v2's `/readiness` endpoint. Once it returns `200 OK` (live sync mode), it's caught up.

6. **Switch views**: v2 recreates views in `user_schema` pointing to `myapp_sync_v2` tables. This is an atomic `CREATE OR REPLACE VIEW` operation.

7. **Drain v1**: Stop the old indexer pod. Optionally drop `myapp_sync_v1` schema.

### Example Kubernetes config

```yaml
# v1 deployment (current)
schema:
  sync_schema: "myapp_sync_v1"
  user_schema: "myapp"

# v2 deployment (new)
schema:
  sync_schema: "myapp_sync_v2"
  user_schema: "myapp"
```

Both deployments share the same `data_schema: "kyomei_data"` (default) and `user_schema: "myapp"`. The only difference is the `sync_schema` name.

### Readiness probe config

```yaml
readinessProbe:
  httpGet:
    path: /readiness
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10

livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15
```

---

## Performance Internals

This section describes the internal optimizations that make Kyomei fast. These are automatic — no configuration required.

### ABI Decoding Pipeline

The decoder maintains a pre-computed signature index (`SignatureEntry`) that caches:
- **Table name**: The PostgreSQL table name for each event (computed once at ABI registration).
- **Parameter columns**: The ordered list of decoded column names (computed once).
- **Parsed event**: The full ABI event definition with parameter types.

This avoids repeated string formatting and allocation during the hot decode loop.

### Zero-Allocation Hex Decoding

`decode_value_raw` decodes EVM log words (topics and data) directly from hex without prepending `0x`:
- **u128 fast path**: Values up to 128 bits (covers most uint types like `uint64`, `uint128`, balances under ~3.4×10³⁸) are parsed via native `u128::from_str_radix` — no big-integer library needed.
- **U256 fallback**: Values exceeding 128 bits use the alloy `U256` type.
- **Address extraction**: Extracts the last 40 hex characters directly, avoiding full word parsing.

### Move-Based Event Processing

Log entries from the block source are **consumed** (moved) rather than cloned when building raw event records:
- `into_raw_event()` transfers ownership of `String` fields (block_hash, tx_hash, topics, data) instead of allocating copies.
- Block merge during concurrent address batching uses `BTreeMap::Entry` with `extend()` and `insert()` — no `block.logs.clone()`.

### Concurrent Address Sub-Batching

When a single worker monitors more addresses than `addresses_per_batch`, the addresses are split into sub-batches that are fetched **concurrently** via `futures::future::try_join_all`. All sub-batches query the same block range, so no events are missed. Results are merged by block number using move semantics.

### HyperSync Address Borrowing

The HyperSync query builder borrows address slices (`&[String]`) from the log filter instead of cloning them, reducing allocation pressure during query construction.

### Chunked Factory COPY

Factory children inserts are chunked into 5,000-row batches to prevent oversized COPY payloads from causing memory pressure or timeout issues with large factory deployments (e.g., Uniswap V2 with 100K+ pairs).

### Resume-Aware Progress

When the indexer resumes from a checkpoint, progress percentage is calculated from the **original config `start_block`** (not the resume block). This means:
- First run starting at block 10M targeting 20M: shows 0% → 100%
- Restart after reaching 12M: shows 20% (not 0%) and continues to 100%

---

## Tuning Guide

### RPC Provider Rate Limits

If you're hitting rate limits:
- Lower `parallel_workers` (2-4)
- Lower `max_concurrent_requests` (3-5)
- Increase `initial_backoff_ms` (2000-5000)
- The adaptive concurrency will automatically back off, but conservative settings reduce wasted requests

### Maximizing Historic Sync Speed

For unlimited/high-throughput providers:
- Use HyperSync if available for your chain
- Increase `parallel_workers` (8-32)
- Increase `blocks_per_request` (50000-100000)
- Increase `blocks_per_worker` (500000-2000000)
- Increase `pool_size` to match parallelism (30-50)
- Increase `checkpoint_interval` (2000-5000) to reduce DB writes

### Memory Usage

Memory is primarily consumed by:
- `event_buffer_size` — max events held in memory per worker
- `pool_size` — PostgreSQL connection buffers
- Number of parallel workers

For memory-constrained environments, lower `event_buffer_size` and `parallel_workers`.

### Database Performance

- TimescaleDB hypertables provide automatic time-partitioning
- Compression is enabled on decoded event tables
- COPY protocol is used for batch inserts (much faster than individual INSERTs)
- ON CONFLICT DO NOTHING ensures idempotent writes for crash safety
