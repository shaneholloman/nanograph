---
audience: dev
status: draft
updated: 2026-03-06
---

# Embeddable API + Embedding Refactor

Canonical plan for the embeddable Rust API and the remaining large-embedding work.

## Summary

The query-side refactor is mostly done:
- shared query lookup and typed JSON param conversion exist in core
- `Database::run()` / `run_query()` exist in core
- `RunResult` / `QueryResult` / `MutationResult` exist in core
- TS, FFI, and CLI now use the shared execution path
- `runArrow()` and Arrow IPC are already landed

What is still open is the work that actually finishes the refactor:
- `open_in_memory()`
- streaming ingest for large embedding graphs, including streaming `@embed` materialization
- optional JSON vector serialization optimization

Current Phase 0 status:
- schemas with a user `id` property now load, persist, reopen, append, and merge correctly
- internal Lance columns are stored under reserved physical names and mapped back on read
- duplicate logical field names currently disable the CDC-derived append / merge fast path and fall back to full dataset rewrites for correctness

Current Phase 1 status:
- `QueryResult` now exposes `num_rows()`, `concat_batches()`, `to_rust_json()`, and `deserialize::<T>()`
- `QueryResult` stays batch-oriented: `schema()` / `batches()` are the zero-copy accessors, and `concat_batches()` is the explicit materialization step when a caller wants one batch
- Rust-native JSON / deserialize preserve wide integers instead of routing through JS-safe SDK JSON coercion
- `params!` and `ToParam` now exist in core for Rust embedders; callers can pass explicit `Literal::Date(...)` / `Literal::DateTime(...)` when those query types cannot be inferred from native Rust scalars

Current Phase 2 status:
- `Database` is now a cheap shared handle backed by `Arc`, `RwLock<Arc<GraphStorage>>`, and an internal single-writer mutex
- read and mutation APIs now operate on `&self`, while storage swaps, manifest commits, and CDC persistence serialize through one writer path
- TS / FFI wrappers no longer serialize all reads behind an external database mutex; they clone a shared `Database` handle and release wrapper locks before execution
- prepared reads intentionally freeze an in-memory snapshot so they can observe pre-mutation state, while one-shot reads still use the live snapshot and Lance pushdown

## Decisions That Still Matter

### One execution path, multiple output formats

This is the core architectural decision and it should not be reopened.

- Query parsing, typechecking, lowering, and execution live in core.
- SDKs are thin adapters over that path.
- Output format is a serialization choice, not a separate execution pipeline.

Current formats:
- SDK JSON via `to_sdk_json()`
- Arrow IPC via `to_arrow_ipc()`
- raw `RecordBatch` access for Rust callers

### Rust-native decode must stay separate from SDK JSON

SDK JSON is JS-safe. That means it can string-coerce wide integers for compatibility.

So the Rust embeddable API must keep a separate path for:
- `to_rust_json()`
- `deserialize::<T>()`

Those methods must not route through `to_sdk_json()`.

### `QueryResult` should stay batch-oriented

`QueryResult` stores `Vec<RecordBatch>`, so a `columns()` helper is a bad fit:
- returning per-batch columns would be awkward and easy to misuse
- returning concatenated columns would hide an allocation and turn a shape-changing operation into a cheap-looking accessor

So the intended surface is:
- `schema()` and `batches()` for zero-copy inspection
- `num_rows()` for aggregate row count
- `concat_batches()` when a caller explicitly wants one materialized batch

### Prepared reads and live reads have different goals

- one-shot reads should keep using the live shared snapshot so they can use persisted Lance pushdown
- prepared reads must keep snapshot isolation across later mutations, so they intentionally freeze an in-memory snapshot at prepare time
- the shared-handle refactor means ordinary query execution now clones an `Arc<GraphStorage>` instead of cloning the full storage

## Progress Track

### Completed

- [x] Core execution facade in `nanograph`
- [x] Shared typed JSON param conversion in core
- [x] `RunResult`, `QueryResult`, `MutationResult`
- [x] Rust embeddable ergonomics: `num_rows()`, `concat_batches()`, `to_rust_json()`, `deserialize::<T>()`, `params!`, `ToParam`
- [x] Adapter migration in TS, FFI, and CLI
- [x] Arrow IPC transport and TS `runArrow()`
- [x] JS-side Arrow decode helper
- [x] Shareable `Database` handle (`Clone + Send + Sync`, internal single-writer mutation path, cheap live snapshots)

### Open

- [x] Phase 3: add `open_in_memory()` on top of the shareable handle design
- [x] Phase 4: expose streaming ingest in TS / FFI / CLI after the landed core reader path
- [x] Phase 5: evaluate JSON vector fast path after streaming ingest

### Deferred

- [ ] Event callbacks / reactivity
- [ ] Custom rules / extension API
- [ ] Pure in-memory storage path that bypasses Lance entirely

## Next Implementation Order

### 0. Completed: duplicate-name correctness

This landed:
- duplicate-safe user property lookup in loader, constraint, and merge paths
- append / merge temporary node IDs now start above the existing node ID range so seeded existing keys cannot alias incoming rows
- Lance persistence stores internal node / edge columns under reserved physical names and maps them back to logical names on read
- regression coverage for overwrite, append, merge, and unique constraints with a user property named `id`

Current limitation:
- if a logical schema still contains duplicate field names after the reserved internal columns are projected back in, the CDC-derived append / merge fast path is disabled and persistence falls back to full dataset rewrites

### 1. Completed: embeddable Rust surface

This landed:
- `QueryResult::num_rows()`
- `QueryResult::concat_batches()` for callers that intentionally want a single-batch view
- `QueryResult::to_rust_json()`
- `QueryResult::deserialize::<T>()`
- `params!`
- `ToParam`

API note:
- `schema()` / `batches()` remain the zero-copy primitives
- there is still no ambiguous `columns()` helper across multi-batch results
- Rust-native decode still does not route through SDK JSON

### 2. Completed: docs and backlog sync

These docs now reflect the actual repo state:
- core facade is landed
- adapter migration is landed
- Arrow IPC is landed
- TS/FFI do not use `run_json()` as the hot read path today; they use `prepare_read_query()` / `execute()` to avoid holding adapter locks during read execution

### 3. Completed: shareability refactor

This landed:
- `Database` is `Clone + Send + Sync`
- storage is owned behind `RwLock<Arc<GraphStorage>>`
- mutations serialize through one internal writer guard before swapping storage and committing manifest / CDC state
- read APIs now work on `&self`
- TS / FFI wrappers clone the shared `Database` handle and release wrapper locks before doing read work
- prepared reads freeze an in-memory snapshot so they remain stable across later mutations
- one-shot reads still use the live snapshot and can keep Lance pushdown

Current caveat:
- prepared reads trade persisted-dataset pushdown for snapshot isolation by freezing the in-memory view at prepare time

### 4. Completed: `open_in_memory()`

This landed as the tempdir-backed variant:
- `Database::open_in_memory(schema_source)`
- TS `Database.openInMemory(schemaSource)` plus `db.isInMemory()`
- cloned handles keep the temp backing directory alive
- the temp backing directory is removed when the last shared handle drops
- `path()` still returns the real backing directory path for the lifetime of the handle

Still deferred:
- no pure in-memory backend that bypasses Lance or filesystem metadata
- no special cross-process / reopen-by-path lifetime coordination for ephemeral databases beyond the owning shared handle

### 5. Completed: streaming ingest for large embedding graphs

This landed:
- reader-based loading in core
- `Database::load_file(...)`
- CLI `load` now uses the file-based path instead of buffering the payload into a string first
- FFI `nanograph_db_load_file(...)`
- TS `loadFile(...)`
- deterministic node / edge spooling with batched node flushes and forward-reference edge resolution
- streaming `@embed` materialization and cache lookup so embedding schemas no longer fall back to a whole-buffer rewrite during load

Explicitly defer more aggressive embedding pipeline work unless profiling proves the basic streaming transform is still the dominant bottleneck.

### 6. Completed: JSON vector fast path evaluation

This landed as a narrow optimization plus a measurement harness:
- synthetic vector-heavy transport benchmark for `to_sdk_json()` vs Arrow IPC
- fast path for `FixedSizeList<Float32>` JSON serialization to avoid recursive per-element dispatch
- `runArrow()` remains the preferred path for large returned vectors

Current takeaway:
- JSON is still materially slower for vector-heavy result sets even after the fast path, so the main recommendation does not change: use `runArrow()` when callers can consume columnar output.

## Acceptance Criteria

- the loader no longer fails or corrupts rows when schemas contain a user `id` property
- overwrite, append, and merge remain correct for schemas with `id: String @key`
- duplicate logical field names do not corrupt persisted data; they fall back to full dataset rewrites when the CDC fast path cannot represent them safely
- Rust embedders can call `db.run(...)` and consume results without touching SDK JSON helpers
- Rust embedders can build params with `params!` / `ToParam`, and wide integers survive `to_rust_json()` / `deserialize::<T>()`
- read sharing no longer clones the full storage for each query
- cloned handles cannot race concurrent mutations into lost storage, manifest, or CDC updates
- `open_in_memory()` works without creating user-visible persistent state
- streaming ingest materially lowers peak memory for large embedding graphs, including schemas that use `@embed`
- existing TS, FFI, CLI, and core tests remain green

## Notes

- `runArrow()` is already the preferred query-time path for large returned vectors.
- The main unresolved large-embedding issue is now ingest memory pressure, not query-pipeline duplication.
- Duplicate logical field names are now supported for correctness, but they currently opt out of CDC-derived append / merge delta writes.
- This file replaces the old split between `embeddable-api.md` and `embedding-api-report.md`.
