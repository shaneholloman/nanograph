# Release Checklist

## Pre-release

- [ ] CI workflow green on `main` / PRs (`cargo check`, TS SDK tests, TS consumer smoke test, Swift SDK tests)
- [ ] All tests pass: `cargo test`
- [ ] CLI e2e pass: `bash tests/cli/run-cli-e2e.sh`
- [ ] Clippy clean: `cargo clippy`
- [ ] Bump version in all Cargo.toml files (nanograph, nanograph-cli, nanograph-ffi, nanograph-ts)
- [ ] Bump version in `crates/nanograph-ts/package.json`
- [ ] Update cross-references (`nanograph = { path = "../nanograph", version = "X.Y.Z" }`) in nanograph-cli, nanograph-ffi, nanograph-ts
- [ ] Commit: `release: X.Y.Z — <summary>`

## Publish

### 1. Tag and push (triggers GitHub Actions release workflow)

```bash
git tag vX.Y.Z
git push origin main --tags
```

This automatically:
- Builds macOS ARM binary on `macos-14` runner
- Builds Swift XCFramework artifacts for macOS (`NanoGraphFFI.xcframework.zip` + checksum)
- Creates GitHub Release with `nanograph-vX.Y.Z-aarch64-apple-darwin.tar.gz` + `.sha256`
- Dispatches formula update to `nanograph/homebrew-tap`

### 2. crates.io (order matters — nanograph first)

```bash
cargo publish -p nanograph
cargo publish -p nanograph-cli
cargo publish -p nanograph-ffi
cargo publish -p nanograph-ts
```

### 3. npm

```bash
cd crates/nanograph-ts
npm publish --otp=<code>
```

## Post-release verification

- [ ] GitHub Release exists: `gh release view vX.Y.Z`
- [ ] Binary downloads: `gh release download vX.Y.Z --pattern '*.tar.gz'`
- [ ] Homebrew formula updated: `gh api repos/nanograph/homebrew-tap/contents/Formula/nanograph.rb --jq '.content' | base64 -d | head -5`
- [ ] Brew install works: `brew install nanograph/tap/nanograph` (or `brew upgrade nanograph`)
- [ ] crates.io: `cargo search nanograph` shows new version
- [ ] npm: `npm view nanograph-db version` shows new version

## Assets

| Asset | Location |
|-------|----------|
| GitHub Release | `github.com/aaltshuler/nanograph/releases` |
| macOS ARM binary | `nanograph-vX.Y.Z-aarch64-apple-darwin.tar.gz` on release |
| Swift XCFramework | `NanoGraphFFI.xcframework.zip` on release |
| Homebrew tap | `github.com/nanograph/homebrew-tap` |
| crates.io (core) | `crates.io/crates/nanograph` |
| crates.io (CLI) | `crates.io/crates/nanograph-cli` |
| crates.io (FFI) | `crates.io/crates/nanograph-ffi` |
| crates.io (TS) | `crates.io/crates/nanograph-ts` |
| npm | `npmjs.com/package/nanograph-db` |

## Infrastructure

| Component | Repo / Config |
|-----------|---------------|
| Release workflow | `.github/workflows/release.yml` |
| Homebrew tap | `nanograph/homebrew-tap` (GitHub org) |
| Tap update workflow | `homebrew-tap/.github/workflows/update-formula.yml` |
| `HOMEBREW_TAP_TOKEN` | Secret on `aaltshuler/nanograph` — fine-grained PAT with Contents write to `nanograph/homebrew-tap` |
