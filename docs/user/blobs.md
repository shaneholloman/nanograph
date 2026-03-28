---
title: Blobs and Multimodal Embeddings
slug: blobs
---

# Blobs and Multimodal Embeddings

NanoGraph does not store raw blob bytes inside `.nano/`.

Instead, media is modeled as normal nodes with external URIs. The database stores:

- the media URI
- the mime type
- optional derived metadata
- optional embeddings

This keeps `.nano/` small and Git-friendly while still allowing text-to-image retrieval and graph traversal from media nodes.

## Storage model

Use media nodes, not `Blob` properties.

Example:

```graphql
node Product {
    slug: String @key
    name: String
}

node PhotoAsset {
    slug: String @key
    uri: String @media_uri(mime)
    mime: String
    width: I32?
    height: I32?
    embedding: Vector(768)? @embed(uri) @index
}

edge HasPhoto: Product -> PhotoAsset
```

In this model:

- `uri` points to the external media asset
- `mime` stores the media type
- `embedding` stores the multimodal vector
- edges connect domain entities to media assets

NanoGraph never writes media bytes into `nodes/*` or `edges/*` Lance datasets.

## How NanoGraph stores media

`@media_uri(mime_prop)` marks a `String` property as an external media URI and names the sibling mime field.

Example:

```graphql
uri: String @media_uri(mime)
mime: String
```

Rules:

- `@media_uri(...)` is valid only on `String` or `String?`
- the annotation argument must name a sibling `String` or `String?` property
- plain strings are rejected during load for `@media_uri` fields
- the value must come from one of NanoGraph's media input formats

Supported load formats:

- `@file:path/to/image.jpg`
- `@base64:...`
- `@uri:file:///absolute/path/image.jpg`
- `@uri:https://example.com/image.jpg`
- `@uri:s3://bucket/path/image.jpg`

Behavior:

- `@file:` reads a file and imports it into NanoGraph's media root
- `@base64:` decodes bytes and imports them into the media root
- `@uri:` stores the URI directly without copying

When NanoGraph imports bytes from `@file:` or `@base64:`, it writes them outside the database under the media root and stores the final `file://...` URI in the node.

By default the media root is:

```text
<db-parent>/media/
```

You can override it with:

```bash
NANOGRAPH_MEDIA_ROOT=/absolute/path/to/media
```

Imported files are content-addressed and grouped by node type, for example:

```text
media/
  photoasset/
    4c8f...e2a1.jpg
```

Relative `@file:` paths are only supported when loading from a file on disk, because NanoGraph resolves them relative to the source JSONL file.

## Multimodal embeddings

`@embed(source_prop)` works on media URI properties too.

Example:

```graphql
node PhotoAsset {
    slug: String @key
    uri: String @media_uri(mime)
    mime: String
    embedding: Vector(768)? @embed(uri) @index
}
```

When `embedding` is null or missing:

- `nanograph load` will generate the vector automatically
- `nanograph embed` can backfill it later

For media URI sources, NanoGraph treats `@embed(uri)` as multimodal embedding, not text embedding.

## Provider setup

Today the practical multimodal provider is Gemini.

Project config:

```toml
[embedding]
provider = "gemini"
model = "gemini-embedding-2-preview"
```

Local secrets:

```bash
GEMINI_API_KEY=...
```

Typical backfill command:

```bash
nanograph embed --db app.nano --only-null
```

## Current Gemini limits in NanoGraph

NanoGraph currently uses Gemini multimodal embeddings for images with these constraints:

- supported media types: `image/png`, `image/jpeg`
- direct embedding sources: `file://`, `http://`, `https://`
- max inline image payload: 20 MB
- max batch size: 6 images per request

Important implications:

- `@file:` and `@base64:` work well because NanoGraph imports them into local `file://` assets
- stored `file://...` media nodes work
- stored `https://...` media nodes work
- plain `s3://...` URIs can be stored, but they are not directly embeddable today

If you want embeddings for S3-backed media today, use one of these patterns:

- load from a local file with `@file:...`
- use a presigned `https://...` URL
- import the asset into the media root first, then embed from the resulting `file://...` URI

OpenAI embeddings remain text-only in NanoGraph.

## Loading media

Example JSONL:

```jsonl
{"type":"PhotoAsset","data":{"slug":"space","uri":"@file:photos/space.jpg"}}
{"type":"PhotoAsset","data":{"slug":"beach","uri":"@uri:https://example.com/beach.jpg","mime":"image/jpeg"}}
{"type":"Product","data":{"slug":"rocket","name":"Rocket Poster"}}
{"type":"HasPhoto","from":"rocket","to":"space"}
```

Overwrite load:

```bash
nanograph load --db app.nano --data data.jsonl --mode overwrite
```

After load:

- the `PhotoAsset.uri` value is a durable external URI
- `PhotoAsset.mime` is filled automatically if possible
- `PhotoAsset.embedding` is generated if configured and null

## Querying media

Because media is modeled as nodes, you query it like any other node type.

Find the closest images for a text query:

```graphql
query image_search($q: String) {
    match { $img: PhotoAsset }
    return {
        $img.slug as slug,
        $img.uri as uri,
        $img.mime as mime,
        nearest($img.embedding, $q) as score
    }
    order { nearest($img.embedding, $q) }
    limit 5
}
```

Traverse from matched images to related graph entities:

```graphql
query products_from_image_search($q: String) {
    match {
        $product: Product
        $product hasPhoto $img
    }
    return {
        $product.slug as product,
        $product.name as name,
        $img.slug as image,
        $img.uri as uri
    }
    order { nearest($img.embedding, $q) }
    limit 5
}
```

This is the intended multimodal workflow in NanoGraph:

1. store media as nodes with external URIs
2. embed those media nodes
3. issue a text query
4. rank media nodes with `nearest(...)`
5. traverse from those media nodes into the rest of the graph

## Operational notes

- NanoGraph stores references and derived vectors, not blob payloads, in `.nano/`
- imported media files live under the media root, not under `nodes/` or `edges/`
- if you keep the media root inside your repo, add it to `.gitignore`
- `nanograph export` preserves URIs by default; it does not silently copy external assets

## Migrating from OpenAI to Gemini multimodal embeddings

OpenAI embeddings in NanoGraph are text-only. Gemini supports text and images with a single model. If you started with OpenAI and want to move to Gemini for unified text + image embeddings, follow these steps.

### 1. Update the embedding config

Change `nanograph.toml`:

```toml
[embedding]
provider = "gemini"
model = "gemini-embedding-2-preview"
```

Swap the API key in `.env.nano`:

```bash
# OPENAI_API_KEY=sk-...
GEMINI_API_KEY=...
```

### 2. Resize vector dimensions if needed

OpenAI `text-embedding-3-small` has a native dimension of 1536. Gemini `gemini-embedding-2-preview` has a native dimension of 3072. Both models support dimensionality reduction — NanoGraph passes your schema's `Vector(dim)` value to the API, so the model truncates its output to match.

Common choices:

| Model | Native dim | Typical schema dim |
|-------|-----------|-------------------|
| `text-embedding-3-small` (OpenAI) | 1536 | `Vector(1536)` |
| `gemini-embedding-2-preview` (Gemini) | 3072 | `Vector(768)` or `Vector(3072)` |

`Vector(768)` is a practical default for Gemini — good quality at half the storage of OpenAI 1536. Use `Vector(3072)` if you need maximum fidelity.

If you want to change the dimension (e.g. from 1536 to 768), edit the schema:

```graphql
node Article {
    slug: String @key
    body: String
    embedding: Vector(768)? @embed(body) @index   # was Vector(1536)?
}

node PhotoAsset {
    slug: String @key
    uri: String @media_uri(mime)
    mime: String
    embedding: Vector(768)? @embed(uri) @index
}
```

Then migrate:

```bash
nanograph migrate --dry-run    # preview the vector dimension change
nanograph migrate
```

If you keep the same dimension, no schema migration is needed — skip to re-embedding.

### 3. Re-embed everything

Old vectors were generated by a different model. They are not comparable to the new model's vectors, so re-embed all rows — not just nulls:

```bash
nanograph embed                            # recompute all embeddings
nanograph embed --reindex                  # recompute and rebuild vector indexes
```

This is the one time a full re-embed is necessary. After this, `--only-null` works normally for incremental backfills.

### 4. Verify

```bash
nanograph doctor
nanograph run <your-search-alias> "test query"
```

### What you get after migration

With Gemini as the single provider, `@embed(source_prop)` does the right thing for both content types:

| Source property | Behavior |
|----------------|----------|
| Plain `String` | Text embedding via Gemini |
| `String @media_uri(mime)` pointing to an image | Image embedding via Gemini |

Text nodes and image nodes share the same embedding space, so `nearest(...)` queries rank them together. A text query can surface images and vice versa — no separate search pipelines needed.

### Checklist

1. Set `provider = "gemini"` and `GEMINI_API_KEY` in `.env.nano`
2. If changing dimensions: edit `Vector(dim)` in schema → `nanograph migrate`
3. `nanograph embed --reindex` (full re-embed, not `--only-null`)
4. `nanograph doctor` to confirm health
5. Test a search query to verify results

## See also

- [schema.md](schema.md)
- [search.md](search.md)
- [config.md](config.md)
