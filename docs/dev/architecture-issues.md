# Architecture Issues (Current)

Status: updated against source code  
Date: 2026-03-21

This note tracks the architecture bottlenecks that are still relevant to the current refactor plan and the concrete ideas worth carrying forward.

Out of scope for this document:
- branching
- concurrent write support
- extension system

## What Changed Since The Initial Review

- Many CLI commands and simple read queries no longer need `Database::open()`. `DatabaseMetadata` now supports metadata-only and Lance-first execution paths.
- `Database::open()` still restores every manifest-pinned dataset into `GraphStorage`, but it no longer eagerly builds traversal adjacency at open.
- Traversal adjacency is now resolved lazily through a separate `EdgeIndexCache` owned by `DatabaseShared`, keyed by edge type and dataset version.
- Durable history is now a single `_wal.jsonl`. Tx-catalog and CDC reads are projected views over WAL, not separate primary logs.
- The old N5 description is no longer current: nanograph still has a thin single-writer graph coordinator, but the concrete dual-log `CDC -> TX catalog -> manifest` architecture has been replaced by `WAL -> manifest`, with tx and CDC reads derived from WAL.
- The old full-graph snapshot diff is no longer the active CDC/change-tracking path. Sparse mutations now build Arrow-native `MutationDelta`, and delete/embed paths emit explicit change rows instead of diffing two whole snapshots.

## N1: Full Dataset Row Residency At Open

Status: reduced, still active.

`Database::open()` still iterates every dataset in the manifest and restores all node and edge batches into in-memory `GraphStorage`. This means the database still needs to fit comfortably in RAM for the graph-aware path.

What changed:
- reopen no longer eagerly builds `csr` / `csc` for every edge type
- traversal adjacency is loaded lazily on first use through `EdgeIndexCache`
- some CLI commands and simple read queries now bypass `Database::open()` entirely

What remains expensive:
- all node `RecordBatch` data is loaded at open
- all edge `RecordBatch` data is loaded at open
- node `id_to_row` maps remain resident
- edge `src_ids` / `dst_ids` / `edge_ids` remain resident

So the issue is no longer “full graph plus eager adjacency at open.” It is now “full row-data residency at open for the graph-aware path.”

Good ideas worth carrying forward:
- Late materialization for Class C traversal. `ExpandExec` should propagate node ids or thin row handles instead of full structs where possible, and final projection should materialize only the fields actually needed.
- Destination-side filter pushdown before expansion. If a filter references only the destination variable, precompute the qualifying destination ids and reject neighbors before building full output rows.
- Lazy node-type loading for the remaining Class C path. The next step after lazy adjacency is reducing node residency, not reintroducing more in-memory graph state.
- Statistics infrastructure focused on graph execution. Row counts and degree stats would help decide traversal direction and future lazy-loading heuristics.

Validated at:
- `crates/nanograph/src/store/database.rs` — `Database::open()`
- `crates/nanograph/src/store/lance_io.rs` — `read_lance_batches()`
- `crates/nanograph/src/store/database.rs` — `EdgeIndexCache`
- `crates/nanograph/src/plan/physical.rs` — `ExpandExec::resolve_edge_indices()`
- `crates/nanograph/src/store/metadata.rs` — `DatabaseMetadata`
- `crates/nanograph-cli/src/main.rs` — Class A / Class B / Class C routing

## N2: Graph-Aware Mutations Still Scale With Total Graph

Status: reduced, still active.

The old statement “every mutation rebuilds the whole graph” is no longer true. Dataset-scoped sparse mutations now avoid full open and operate from metadata plus the touched datasets only.

What is improved:
- sparse `run` mutations for simple node/edge insert/update/delete use the metadata-only path
- `load --mode append` and `load --mode merge` use sparse restoration paths

What still uses full-snapshot mutation semantics:
- graph-aware node deletes with edge cascade
- graph-aware edge deletes through the legacy path
- overwrite load
- embed and other snapshot-oriented mutations

Those paths still construct a new `GraphStorage`, repopulate types, and swap in a new snapshot atomically. For those operations, write latency and peak memory still scale with total graph size rather than changed rows.

Good ideas worth carrying forward:
- Move graph-aware mutations onto the same delta-first model already used by sparse mutations. The remaining write path should produce `MutationDelta` directly instead of rebuilding storage first and describing changes later.
- Break the `Database` ↔ mutation-execution callback cycle, but do it with a rich mutation-delta abstraction rather than a thin `inserts/updates/deletes` envelope.
- Minimize graph-aware rebuild scope. When a mutation touches one node type and its incident edge types, the long-term target should be “restore only what is needed,” not “copy every type into a new `GraphStorage`.”
- Keep WAL as the authoritative durable history. Any mutation refactor should feed WAL and dataset persistence from the same delta source.

Validated at:
- `crates/nanograph/src/store/database/cdc.rs` — `delete_nodes_locked()` / `delete_edges_locked()`
- `crates/nanograph/src/store/database/persist.rs` — `apply_mutation_plan_locked()`
- `crates/nanograph/src/store/database/persist.rs` — `run_mutation_query_sparse()`
- `crates/nanograph-cli/src/main.rs` — mutation classification and sparse routing

## N3: Change Tracking Is No Longer O(Total Graph), But The Write Path Is Still Mixed

Status: mostly solved, with follow-up work remaining.

The old full-graph snapshot diff described in the original review is gone from the active write path. nanograph no longer computes changes by serializing two complete graph states and diffing them.

Current state:
- sparse mutations build Arrow-native `MutationDelta`
- pending change rows are derived from that delta
- delete paths emit delete events directly from the affected rows
- embed emits update events directly from cached `before` / `after` row data
- WAL is the single durable history log

What remains mixed:
- not every mutation path uses one uniform delta-first abstraction end to end
- some graph-aware and load-oriented paths still pass `CdcLogEntry` collections through parts of the persist layer instead of carrying a single `MutationDelta` all the way through

So the main bottleneck is fixed, but the architecture is not yet fully unified around one internal mutation model.

Good ideas worth carrying forward:
- Standardize on Arrow-native `MutationDelta` everywhere. This should become the only internal representation for changed rows.
- Keep CDC as a derived view, not a primary mechanism. WAL plus delta should remain the source of truth.
- Make graph-aware mutation planning declarative. The useful idea from the broader architecture discussion is not “reintroduce old CDC modules,” but “return a durable delta plan and let persistence own how it is written.”

Validated at:
- `crates/nanograph/src/store/database/persist.rs` — `MutationDelta`
- `crates/nanograph/src/store/database/persist.rs` — `run_mutation_query_sparse()`
- `crates/nanograph/src/store/database/persist.rs` — `build_pending_cdc_entries_from_delta()`
- `crates/nanograph/src/store/database/cdc.rs` — explicit delete-event generation
- `crates/nanograph/src/store/txlog.rs` — `_wal.jsonl` and projected CDC / tx views

## N8: Text Search Predicates Are Still Brute-Force

Status: unchanged.

`search()`, `fuzzy()`, `match_text()`, and `bm25()` are still evaluated row-by-row in the planner/executor layer. They do not use a native FTS or n-gram index today.

What changed:
- simple dataset-scoped reads can now route through the Lance-first path

What did not change:
- the Lance-first classifier intentionally excludes `search`, `fuzzy`, `match_text()`, and `bm25()` for now
- these predicates still evaluate in-memory / per-row
- only `nearest()` has a Lance-native ANN fast path
- the codebase still only builds scalar and vector indexes, not FTS indexes

This is still acceptable for small graphs and narrow workloads, but it remains a scaling limit for text-heavy queries.

Good ideas worth carrying forward:
- Add explicit Lance-native text index integration instead of keeping text predicates as planner-local string functions forever.
- Add TopN for non-ANN `order + limit` queries so text-heavy result ranking does not always pay full-sort cost.
- Add lightweight statistics so the planner can make better choices about filter placement and future text-query lowering.
- Consider selection-vector style filtering only after the larger Class C residency and mutation issues are further reduced.

Validated at:
- `crates/nanograph/src/plan/planner.rs` — `evaluate_*` search helpers
- `crates/nanograph-cli/src/main.rs` — Lance-first query eligibility
- `crates/nanograph/src/store/indexing.rs` — scalar and vector indexes only

## Current Priority Order

1. Finish moving graph-aware mutations onto delta-first internals.
2. Reduce or replace full row-data residency for the remaining Class C path.
3. Add traversal-oriented optimizations that directly reduce Class C memory and work:
   late materialization, destination-filter pushdown, and targeted graph statistics.
4. Add proper indexed text search if text-heavy workloads become important.

## Deprioritized Ideas

These may still be reasonable eventually, but they are not the current roadmap:
- large-scale package/module reshuffles for their own sake
- a generic `StorageAccessor` that re-bakes adjacency ownership back into storage
- planner/executor layer cleanup that does not materially improve the remaining bottlenecks
- parallel execution and spilling before the Class C shape is simplified
