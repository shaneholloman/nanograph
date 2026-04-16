---
title: Storage Migration
slug: lance-migration
---

# Storage Migration to NamespaceLineage

nanograph `1.2.x` defaults new graphs to `NamespaceLineage`.

If you still have a legacy database, the supported upgrade path is:

```bash
nanograph storage migrate --db <db>.nano --target lineage-native
```

This is a one-shot storage migration. nanograph copies the currently committed graph state into a fresh `NamespaceLineage` database, swaps the database root on success, and keeps the old database as a backup.

## What changed in 1.2

New graphs no longer treat the old filesystem manifest as the source of truth.

- Committed graph state now lives in Lance-backed internal tables, centered around `__graph_snapshot`.
- Transaction windows live in `__graph_tx`.
- Delete tombstones live in `__graph_deletes`.
- Inserts and updates are reconstructed from Lance lineage instead of replaying `_wal.jsonl`.
- Managed imported media is stored in `__blob_store` and exposed as `lanceblob://sha256/...` URIs.

In simple terms: the old `graph.manifest.json` / `_wal.jsonl` mental model is legacy-only now. New graphs commit and replay from Lance-native state.

## When you need to migrate

Run migration when either of these is true:

- `nanograph` tells you the database is legacy storage and prints `nanograph storage migrate --db ... --target lineage-native`
- you want the new default CDC rail, Lance 4 namespace storage, and managed blob storage

You do not need migration for databases that already use `NamespaceLineage`.

## Recommended target

Use `lineage-native` unless you are doing a very specific intermediate compatibility step.

- `lineage-native` is the current default for new graphs and the recommended destination.
- `lance-v4` is an older intermediate namespace rail. It remains available, but it is not the recommended steady state for new work.

## What migration preserves

Migration preserves the currently visible graph state:

- nodes
- edges
- property values
- schema files
- indexes rebuilt for the migrated tables
- remote media URIs such as `http(s)://`, `s3://`, and other provider-pass-through references

## Consequences of migration

Migration is intentionally not history-preserving.

- Old CDC history does not carry forward. The migrated graph starts a fresh CDC epoch.
- The migrated `graph_version` restarts from `1`.
- Old Lance dataset version history is not preserved.
- Old WAL-era sequencing semantics do not carry forward.
- Scripts or tools that read or patch `graph.manifest.json` or `_wal.jsonl` must stop doing that for migrated graphs.
- Managed imported media is rewritten into `__blob_store`; values become `lanceblob://sha256/...` instead of old filesystem-backed managed paths.
- Unmigrated legacy databases no longer open for normal read/write in the 1.2 line. The CLI fails fast and prints the exact `storage migrate` command to run.

That is the main operational tradeoff: you keep the current graph contents, but you do not keep the old storage-era history.

## Backup behavior

The migrator moves the original database aside before creating the new one.

- Active database after success: `<db>.nano`
- Automatic backup path: `<db>.nano.v3-backup`

The backup naming is the same whether you migrate from legacy v3 storage or from the intermediate `lance-v4` rail.

Migration refuses to run if the backup path already exists.

## Migration procedure

### 1. Pre-flight checks

```bash
nanograph doctor --db <db>.nano --verbose
nanograph describe --db <db>.nano --json > /tmp/pre-migration-describe.json
```

Record the current row counts and keep a note of any managed media types in the graph.

### 2. Run the migration

```bash
nanograph storage migrate --db <db>.nano --target lineage-native
```

On success, the command reports:

- `db_path`
- `backup_path`
- `graph_version`
- `node_tables`
- `edge_tables`
- `media_uris_rewritten`

`media_uris_rewritten` counts imported managed media values that were moved into `__blob_store`.

### 3. Verify the migrated database

```bash
nanograph version --db <db>.nano
nanograph doctor --db <db>.nano --verbose
nanograph describe --db <db>.nano --json > /tmp/post-migration-describe.json
nanograph changes --db <db>.nano --format json
```

Validate:

- row counts per node and edge type match pre-migration
- the database opens and is writable
- `changes` returns lineage-native rows
- expected managed media values now use `lanceblob://sha256/...`

Run your normal smoke queries too:

```bash
nanograph run --db <db>.nano --query queries.gq --name <query> --param key=value
```

### 4. Keep the backup until you trust the new graph

Do not delete `<db>.nano.v3-backup` immediately. Keep it until:

- smoke queries pass
- search and embeddings behave correctly
- media-backed records resolve correctly
- downstream tools no longer depend on the old WAL/manifest layout

## Rollback

If post-migration verification fails, restore the backup:

```bash
mv <db>.nano <db>.nano.failed
mv <db>.nano.v3-backup <db>.nano
```

If the migration command itself fails, nanograph tries to restore the original database automatically.

## Operator notes

These are the behavior changes that matter most after migration:

- `changes` is lineage-native for new graphs. Ordering is deterministic, but there is no old `seq_in_tx` contract.
- `cleanup --retain-tx-versions N` is the main CDC retention control. nanograph derives the necessary Lance history automatically.
- `compact` and `doctor` operate against the committed Lance snapshot and retained lineage windows.
- Managed imported media is now database-managed. `NANOGRAPH_MEDIA_ROOT` is legacy input for migration, not the active steady-state storage model.

## See also

- [Best Practices](best-practices.md)
- [CLI Reference](cli-reference.md)
- [Blobs](blobs.md)
- [Embeddings](embeddings.md)
