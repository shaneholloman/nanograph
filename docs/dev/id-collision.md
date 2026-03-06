# Bug: Internal `id` Column Collides with User-Defined `id` Property

**GitHub Issue:** #2
**Status:** Open
**Severity:** High — makes any node type with a property named `id` non-functional

## Root Cause

The internal node ID column is named `"id"` (UInt64) and inserted at index 0 of the Arrow schema (`schema_ir.rs:239`). If a user defines a property also named `id` (e.g., `id: String @key`), the schema has two fields with the same name.

The JSONL loader at `jsonl.rs:80` correctly uses **index-based** field lookup (`fields()[i + 1]`) for null validation. But at `jsonl.rs:98`, it uses **name-based** lookup (`field_with_name(prop)`), which returns the first match — the internal UInt64 field. String values then fail `as_u64()`, produce nulls, and the non-nullable check errors with:

```
storage error: field has N null value(s) from type mismatch (expected UInt64)
```

## Affected Code

| File | Line | Issue |
|------|------|-------|
| `catalog/schema_ir.rs` | 239 | Internal ID field created as `Field::new("id", UInt64, false)` |
| `catalog/schema_ir.rs` | 262 | User property with same name appended to same schema |
| `store/loader/jsonl.rs` | 98 | `field_with_name(prop)` returns wrong field (first match) |
| `store/loader/jsonl.rs` | 80 | Same file already uses correct index-based lookup |

## Fix

Replace name-based lookup with index-based lookup at `jsonl.rs:98`:

```rust
// Before (buggy):
let field = node_type.arrow_schema.field_with_name(prop).unwrap();

// After (correct):
let field = &node_type.arrow_schema.fields()[i + 1];
```

Also audit the Lance persistence path (`store/database.rs`) for the same pattern — any `field_with_name()` call on a schema that includes the internal `id` column is suspect.

## Longer-Term

Consider renaming the internal ID column to `_id` to avoid future collisions with user-defined properties. This would require a storage migration for existing databases.
