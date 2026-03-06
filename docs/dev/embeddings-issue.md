# Embeddings Issue: Comprehensive, Clean Fix Plan

## Summary

The current TypeScript SDK path handles large embedding payloads correctly, but not efficiently:

- `db.load(data: string, mode)` requires full JSONL payloads in memory.
- `@embed` materialization does full-string parse/rewrite before normal load.
- `db.run(...)` serializes vectors as JSON arrays of JS numbers, which is expensive for large `Vector(dim)` columns.

This document proposes a clean end-state that keeps backward compatibility and fixes both load-time and read-time pressure.

## Goals

1. Keep existing API behavior stable (`load`, `run`) so no user breakage.
2. Add memory-safe ingestion for very large graphs and embeddings.
3. Add high-performance result path that avoids vector JSON inflation.
4. Preserve query projection behavior so embeddings are only read when requested.
5. Ship with measurable performance/regression gates.

## Non-Goals

1. No query language redesign.
2. No forced migration for existing SDK users.
3. No change to vector math semantics (`nearest`, `rrf`, etc.).

## Root Causes

1. **Ingestion is string-based and whole-buffer**
   - TS N-API currently accepts `data_source: String`.
   - Loader parses all lines into intermediate collections before insertion.
2. **Embedding materialization is whole-buffer**
   - `@embed` path parses full input, mutates JSON lines, renders a full output string, then re-parses in loader.
3. **Vector result transport is JSON-first**
   - `FixedSizeList<Float32>` vectors become nested JSON numbers.
   - This creates per-element conversion and V8 heap pressure.

## Proposed End-State

Implement a **dual-path architecture**:

1. **Streaming load path** for big payloads (`loadFile`/stream APIs).
2. **Columnar result path** for vector-heavy queries (`runArrow`).
3. Keep JSON APIs for compatibility, with targeted serializer improvements.

---

## A. Ingestion Fix (Primary)

### A1. New TS SDK APIs (additive, non-breaking)

Add to `Database`:

- `loadFile(dataPath: string, mode: "overwrite" | "append" | "merge"): Promise<void>`
- `loadReadable(stream: NodeJS.ReadableStream, mode: ...): Promise<void>` (optional phase 2)

Keep existing:

- `load(dataSource: string, mode: ...): Promise<void>`

### A2. Rust core: streaming loader interfaces

Add reader-based entry points in storage layer:

- `load_with_mode_reader<R: BufRead>(&mut self, reader: R, mode: LoadMode) -> Result<()>`
- `build_next_storage_for_load_reader(...)`

This avoids requiring one giant input string.

### A3. Streaming JSONL parse strategy

Current logic groups all node/edge rows in memory. Replace with bounded-memory pipeline:

1. Read line-by-line (`BufRead::lines`).
2. Parse JSON per line.
3. Route node rows into per-type append buffers (record batches flushed by `ROW_BATCH_SIZE`).
4. Route edge rows into a temporary spool file if endpoints may appear later.
5. Build/maintain `@key -> id` map as nodes are inserted.
6. Resolve and ingest spooled edges after node pass.

Result: memory scales with batch/window size, not file size.

### A4. `@embed` materialization in streaming mode

Replace current full-input `materialize_embeddings_for_load(...)->String` with transform iterator:

- Read one JSONL row at a time.
- If row needs embedding:
  - check cache first
  - batch API calls by `(dim, batch_size)` using bounded queue
  - inject vector into row and forward downstream
- No full-file render/parse cycle.

### A5. Memory control knobs

Add explicit loader knobs (env or options):

- `NANOGRAPH_LOAD_ROW_BATCH_SIZE` (default: e.g. 2048)
- `NANOGRAPH_EDGE_SPOOL_DIR` (default: db temp dir)
- existing embed knobs remain (`NANOGRAPH_EMBED_BATCH_SIZE`, chunk settings, cache limit)

---

## B. Query Result Fix (High Impact for Embedding Returns)

### B1. Add Arrow IPC query API

Add additive API:

- `runArrow(querySource, queryName, params?) -> Promise<Buffer>`

Behavior:

- Execute query normally to `RecordBatch`es.
- Encode as Arrow IPC stream (`arrow_ipc::writer::StreamWriter`).
- Return `Buffer` to Node.

Benefits:

- Preserves `Float32` vectors end-to-end.
- Avoids JSON number array expansion.
- Enables efficient typed-array access in JS.

### B2. Keep `run(...)` JSON API

`run(...)` remains unchanged for compatibility and ergonomics.

### B3. TS helpers (optional)

Provide tiny helper utilities:

- `decodeArrow(buffer)` -> table/record batches
- `rowsFromArrow(buffer)` for users who still want objects

This keeps the fast path available without forcing an immediate dependency change on all users.

---

## C. JSON Path Improvements (Low Risk)

### C1. Fast path for vector JSON serialization

Optimize `DataType::FixedSizeList` conversion:

- Detect `Float32` child array.
- Convert in a tight loop with preallocated `Vec<serde_json::Value>`.
- Avoid recursive per-element dispatch for vector elements.

This helps `run(...)` users even without adopting `runArrow`.

### C2. De-duplicate serializer logic

There are multiple `array_value_to_json` implementations in the codebase.
Unify shared vector conversion logic to prevent divergence and repeated hot-path bugs.

---

## D. Projection and Query Hygiene

Projection pruning is already in planner/node-scan and should remain.

Action items:

1. Document strongly: avoid returning full node structs (`$n`) when embeddings are large.
2. Add lint/check warning (optional) when a projected struct includes large `Vector(dim)` fields unintentionally.

---

## File-Level Change Plan

### TS SDK / N-API

1. `crates/nanograph-ts/index.d.ts`
   - add `loadFile(...)`
   - add `runArrow(...)`
2. `crates/nanograph-ts/src/lib.rs`
   - expose new napi methods
   - maintain existing `load`/`run`
3. `docs/dev/typescript-sdk.md`
   - document new performance APIs and migration guidance

### Core loader

1. `crates/nanograph/src/store/database.rs`
   - add reader/file load entry points
2. `crates/nanograph/src/store/loader.rs`
   - add reader-based build path
3. `crates/nanograph/src/store/loader/jsonl.rs` (or new `jsonl_stream.rs`)
   - implement bounded streaming ingest
4. `crates/nanograph/src/store/loader/embeddings.rs`
   - streaming materialization transformer

### Query transport

1. `crates/nanograph/src/json_output.rs`
   - fast vector JSON path
2. new Arrow IPC helper module (e.g. `crates/nanograph/src/arrow_output.rs`)
   - `record_batches_to_arrow_ipc(...)`

---

## Backward Compatibility

1. No existing method is removed or behavior-changed.
2. New methods are additive and opt-in.
3. Existing JSON responses remain identical.

---

## Test Plan

### 1) Correctness

1. Load parity tests:
   - `load(string)` vs `loadFile(path)` produce identical DB state.
2. Embedding parity tests:
   - streaming `@embed` output matches current non-streaming behavior.
3. Query parity tests:
   - `run` JSON and `runArrow` decode produce equivalent values.

### 2) Performance regressions

1. Synthetic large graph fixtures:
   - e.g. `100k` nodes, `Vector(1536)` and `Vector(3072)`.
2. Metrics:
   - peak RSS during load
   - load throughput rows/sec
   - query serialization latency
   - process GC pressure (Node side)

### 3) Targets (pass criteria)

1. `loadFile` peak RSS reduced by at least `50%` vs `load(string)` on large payloads.
2. `runArrow` latency at least `2x` better than `run` for vector-heavy result sets.
3. No correctness diffs in query results.

---

## Rollout Plan

1. Phase 1 (low risk, quick win)
   - JSON vector serializer fast path.
   - Docs on projection hygiene.
2. Phase 2 (primary fix)
   - `loadFile` + streaming loader + streaming `@embed`.
3. Phase 3 (read path optimization)
   - `runArrow` API and examples.
4. Phase 4
   - benchmark CI job + perf budgets.

---

## Recommended Default Usage After Fix

For large embedding datasets:

1. Ingest with `loadFile(...)` (not giant string payloads).
2. Query with explicit projected fields (avoid full struct projection).
3. Use `runArrow(...)` for vector-heavy responses.
4. Keep `run(...)` for small/simple response sets.

---

## Why this is the cleanest fix

It addresses the issue at the right abstraction boundaries:

1. **Ingestion boundary**: remove whole-buffer requirements.
2. **Transport boundary**: stop forcing vectors through JSON.
3. **Execution boundary**: keep projection pruning and query semantics stable.

This produces durable performance gains without forcing a breaking SDK migration.
