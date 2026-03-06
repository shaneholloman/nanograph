# Backlog

## Embeddable API

See [docs/dev/embeddable-api.md](embeddable-api.md) for full design.

Current status:
- Core execution facade, adapter migration, Arrow IPC, and the Rust embeddable ergonomics (`num_rows`, `concat_batches`, `to_rust_json`, `deserialize`, `params!`, `ToParam`) are landed.
- Duplicate logical field names are correct for load / persist / reopen / append / merge, but they currently opt out of the CDC-derived append / merge fast path and fall back to full dataset rewrites.
- The shareable `Database` refactor is landed: `Database` is `Clone + Send + Sync`, mutations serialize through one internal writer path, and TS / FFI no longer hold external read mutexes.
- Prepared reads freeze an in-memory snapshot for isolation across later mutations; one-shot reads still use the live snapshot and Lance pushdown.
- Streaming ingest is landed across core and adapters: reader-based loading, `Database::load_file(...)`, TS `loadFile(...)`, FFI `nanograph_db_load_file(...)`, CLI file-based load, edge spooling for forward references, and streaming `@embed` materialization now avoid the extra whole-buffer rewrite.
- Tempdir-backed `Database::open_in_memory(schema_source)` is landed in core and now exposed in TS as `Database.openInMemory(...)`.
- JSON vector fast path evaluation is landed: there is now a vector-heavy transport perf harness plus a targeted `FixedSizeList<Float32>` serializer fast path, but Arrow IPC is still the preferred path for large returned vectors.

### Phase 2: Shareability
- [x] Refactor `Database` internals to Arc + RwLock, implement `Clone + Send + Sync`
- [x] Move read APIs toward `&self` with cheap prepared-read snapshots
- [x] Serialize mutations through one internal writer path
- [x] Simplify FFI/TS to drop external Mutex wrappers for reads

### Phase 3: In-memory open
- [x] `Database::open_in_memory(schema_source)` (tempdir-backed)

### Phase 4: Streaming ingest
- [x] Reader-based loading in core
- [x] `Database::load_file(...)`
- [x] TS `loadFile(...)`
- [x] FFI `nanograph_db_load_file(...)`
- [x] CLI file-based load path
- [x] Bounded-memory batching, edge spooling, and streaming `@embed` materialization

### Phase 5: JSON vector fast path
- [x] Evaluate post-streaming-ingest JSON vector serialization optimization

### Deferred
- [ ] `on_change()` callback registration (broadcast channel)
- [ ] Custom UDF registration via DataFusion (separate design doc)

## CLI Ergonomics

- [ ] Structured JSON errors — when `--json` is set, emit `{"error": "...", "kind": "..."}` instead of stderr text. Critical for agent consumption.
- [ ] `--dry-run` on `load` and mutation `run` — show type-checked plan, affected types, estimated row counts without writing. (`migrate --dry-run` already exists.)
- [ ] Consistent `--json` vs `--format` — pick one mechanism. Currently `--json` is global and `--format` is per-command; some commands check both. Unify so `--json` globally switches all output (including errors) to structured JSON.
- [ ] Schema introspection — `nanograph describe --type Person` to dump a single type's properties, annotations, and Arrow schema. Current `describe` shows manifest-level info only.
- [ ] Inline query — `--query-inline 'insert Person { name: $name }'` or `--query -` for stdin, so one-liners don't need a `.gq` file.
- [ ] Stdin for load — `--data -` reads JSONL from stdin for pipeline composition (`cat data.jsonl | nanograph load my.nano --data - --mode append`).

## CLI (existing)

- [x] `nanograph describe` — print schema definition for a type or all types
- [x] `--format json` output (array of objects, in addition to existing `table|csv|jsonl`)
- [x] `--json` on all commands (`check`, `migrate`, `load`, `init`, `delete`) — machine-readable output for agents
- [x] `nanograph export --format jsonl` — dump full graph to stdout for git-friendly snapshots
- [x] `nanograph version --db` — print current Lance version per dataset type
- [ ] Progress bars on `nanograph load` for large datasets — stream row counts via indicatif during JSONL parse and Lance write phases
- [ ] Locked node types — mark types as read-only in schema (`@locked`?) to prevent mutation/delete via CLI or query mutations
- [ ] Encryption at rest — encrypt Lance datasets and JSONL logs on disk, key management via env var or keyring

## Query

- [x] Vector search — `Vector(dim)` type, `@embed(prop)`, `nearest()` ordering, text predicates (`search`, `fuzzy`, `match_text`)
- [x] Hybrid search — `bm25()` + `nearest()` + `rrf()` fusion, graph-scoped retrieval

## Storage

- [x] Content-addressed node IDs — derive from `@key` hash instead of auto-increment u64
- [ ] Multimedia content — store binary blobs (images, PDFs, audio) alongside graph nodes, auto-embed via multimodal models

## Schema / Types

- [x] Enum types (e.g. `status: enum(open, closed, blocked)`)
- [x] Array/list properties (e.g. `tags: [String]`)
- [x] Date/DateTime types (chrono — date literals, filtering, ordering)

## Performance

- [ ] Benchmark suite — criterion harnesses for NodeScan 10K, 2-hop expansion 10K, JSONL load 100K; track regressions in CI

## UX

- [ ] Agent nudges — proactive suggestions for agent how to make most of nanograph
- [ ] ASCII graph visualization — render subgraph topology in terminal output for `nanograph describe` or query results

## SDK

- [x] TypeScript SDK — `nanograph-db` npm package, napi-rs bindings for Node.js
- [x] Swift SDK — `nanograph-ffi` C ABI + Swift Package wrapper
- [ ] Python SDK — PyO3 bindings for Python applications
- [ ] Go SDK — CGo bindings for Go applications
