# NanoGraph Swift Wrapper (Local)

This package wraps the `nanograph-ffi` C ABI and links against the Rust library built in this repo.

## Prerequisites

Build the Rust FFI library first:

```bash
cd /Users/andrew/code/nanograph
cargo build -p nanograph-ffi
```

Expected artifacts:

- `target/debug/libnanograph_ffi.a`
- `target/debug/libnanograph_ffi.dylib`

## Build the Swift package

```bash
cd /Users/andrew/code/nanograph/crates/nanograph-ffi/swift
swift build
swift test
```

Debug/test builds link `../../../target/debug`; Swift release builds link `../../../target/release`.

## API shape

`Database` provides:

- `create(dbPath:schemaSource:)`
- `open(dbPath:)`
- `openInMemory(schemaSource:)`
- `load(dataSource:mode:)`
- `loadFile(dataPath:mode:)`
- `run(querySource:queryName:params:)`
- `runArrow(querySource:queryName:params:)`
- `check(querySource:)`
- `describe()`
- `compact(options:)`
- `cleanup(options:)`
- `doctor()`
- `isInMemory()`
- `close()`

All read APIs decode C-returned JSON into Foundation values (`Any`) by default.
Typed decode overloads are available for `run/check/describe`. `runArrow(...)` returns Arrow IPC as `Data`, and the package also exports:

- `decodeArrow(_ data: Data) -> Any`
- `decodeArrow(_:from:) -> T`
