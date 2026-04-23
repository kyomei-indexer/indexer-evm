# Documentation

Focused, per-feature docs for `indexer-evm`. Each page covers one capability and — where a flow is involved — ships a Mermaid diagram.

## Start here

- [Architecture](./architecture.md) — components, data flow, where each module lives.
- [Configuration](./configuration.md) — the YAML surface, env-var expansion, single-chain vs multi-chain.
- [Schema model](./schema-model.md) — the 3-schema design that makes zero-downtime cutovers possible.

## Ingestion

- [Block sources](./block-sources.md) — RPC, eRPC, HyperSync, fallback RPC, adaptive range, concurrency.
- [Sync engine](./sync-engine.md) — workers, checkpoints, historic backfill, live follow.
- [ABI decoding](./abi-decoding.md) — event + function decoding, per-event table generation.
- [Reorg handling](./reorg-handling.md) — detection, finality, atomic rewrites.
- [Factory contracts](./factory-contracts.md) — dynamic child-contract discovery.
- [Filters](./filters.md) — per-event predicates to drop noise at ingest.

## Optional feature modules

- [Call traces](./traces.md) — `debug_traceBlockByNumber` / `trace_block` indexing.
- [Accounts](./accounts.md) — per-address transaction and internal transfer tracking.
- [Views](./views.md) — periodic view-function reads.
- [Continuous aggregates](./aggregations.md) — TimescaleDB rollups.
- [Fan-out](./fan-out.md) — Redis Streams, HTTP webhooks, CSV export.

## Operations

- [Multi-chain](./multi-chain.md) — one process, N chains, shared DB.
- [Observability](./observability.md) — `/health`, `/readiness`, `/sync`, Prometheus `/metrics`, logs.

---

> Every page links back to the source files it describes. If a doc and the code disagree, the code wins — open an issue or fix the doc.
