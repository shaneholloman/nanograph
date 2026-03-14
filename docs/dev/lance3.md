# Lance 2.x -> 3.x Migration

Spike status: **completed on `codex/lance3-spike`**

## Outcome

The Lance 3 upgrade is in much better shape than the original assessment assumed.

What happened in the spike:

- workspace dependencies were bumped from Lance 2.x to 3.0.0
- the lockfile converged to a **single DataFusion line** at `52.3.0`
- `cargo check --workspace --all-targets` passed
- focused runtime-heavy suites passed:
  - `cargo test -p nanograph --test engine_integration`
  - `cargo test -p nanograph --test schema_migration`
  - `cargo test -p nanograph-cli`
  - `npm test` in `crates/nanograph-ts`
  - `swift test` in `crates/nanograph-ffi/swift`

The important result is:

- **the migration is viable**
- **the DataFusion unification benefit is real**
- **the expected direct API breakage was smaller than feared**

## Why the upgrade is worth it

### 1. Single DataFusion line

Before the spike:

- `nanograph` depended directly on DataFusion 52.x
- Lance 2.x still pulled DataFusion 51.x
- the lockfile therefore carried two DataFusion families

After the bump:

- DataFusion converged to `52.3.0`

That is the clearest structural win from Lance 3:

- less dependency duplication
- smaller compile graph
- cleaner maintenance story
- fewer version-split surfaces in the runtime

### 2. Better base for core-operation optimization

Even though this spike did not benchmark performance yet, Lance 3 is still attractive for:

- point lookup paths
- scan-heavy reads
- vector/scalar index work
- aggregate-style operations
- warm-open/index prewarm follow-up work

This is now a **separate performance task**, not part of the migration itself.

## What actually changed

### Dependency/toolchain

- `lance`, `lance-index`, `lance-linalg` -> `3.0.0`
- MSRV updated to `1.91`
- explicit `rust-toolchain.toml` added
- CI/release workflows updated to install `1.91.0`

## What did not require code changes in the spike

The original migration note overestimated the likely code churn.

In practice:

- `Dataset::delete()` did not force a source change in our current usage
  - we ignore the success value and still compile cleanly
- the merge insert source path also compiled as-is
  - the existing `RecordBatchIterator` wrapper still satisfied Lance 3

So the expected first-breakpoint files did **not** require direct edits during this spike.

## Risk areas that still matter

Even though the focused tests passed, these remain the places to watch for regressions in future work:

- `crates/nanograph/src/store/indexing.rs`
  - scalar/vector index creation and rebuild
- `crates/nanograph/src/store/database/maintenance.rs`
  - compaction and cleanup semantics
- `crates/nanograph/src/plan/node_scan.rs`
  - scan path behavior
- `crates/nanograph/src/plan/planner.rs`
  - nearest/index-aware planning behavior

In other words:

- the migration looks safe
- but indexing and maintenance are still the highest-value regression zones

## Recommendation

Treat the Lance 3 migration as:

- **accepted from a compatibility standpoint**
- **worth landing**
- **followed by a separate performance/benchmarking pass**

The biggest practical gain already achieved is the single DataFusion line.
The next decision is no longer "can we migrate?" but "do we see meaningful runtime wins after the dependency simplification?"
