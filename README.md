<h1 align="center">ermite-kv</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue?style=flat-square" alt="License: MIT"></a>
  <a href="https://github.com/bnema/ermite-kv/commits/main"><img src="https://badgen.net/github/last-commit/bnema/ermite-kv/main?icon=github" alt="Last commit"></a>
  <a href="https://github.com/bnema/ermite-kv/stargazers"><img src="https://badgen.net/github/stars/bnema/ermite-kv?icon=github" alt="GitHub stars"></a>
</p>

<p align="center">An embedded key-value store for Go.</p>

> Read-only mirror. Development happens at git.bnema.dev; issues and pull requests are closed here.

---

## Features

- Data in memory, persisted to an append-only log with snapshots
- One writer groups commits into a single `fsync`
- Lock-free reads from immutable snapshots
- Large values (1 KiB and up, `Options.ColdValueBytes`) served from the memory-mapped snapshot file once they are in a snapshot (at open, or after the next compaction); keys, smaller values and values written since the last compaction are on the heap
- Conditional writes (`RequireAbsent`, `RequireVersion`)
- Standard library only, plus [zerowrap](https://git.bnema.dev/zerowrap) for logs

Status: early development, no stable API. Linux only.

## Usage

```go
import kv "bnema.dev/ermite-kv"

db, err := kv.Open(ctx, "/var/lib/app/kv", kv.Options{})
if err != nil {
	return err
}
defer db.Close(ctx)

var b kv.Batch
b.RequireAbsent([]byte("user/alice"))
b.Put([]byte("user/alice"), []byte(`{"name":"Alice"}`))
seq, err := db.Apply(ctx, &b) // durable and visible when it returns
if errors.Is(err, kv.ErrConflict) {
	// the key already exists
}

for key, item := range db.Snapshot(ctx).Prefix(ctx, []byte("user/")) {
	fmt.Printf("%s = %s (version %d)\n", key, item.Value, item.Version)
}
```

Durability, visibility and value lifetime guarantees are in the package documentation (`go doc bnema.dev/ermite-kv`).

## Operation

- Only the database writes its directory: never modify, truncate or replace its files while it is open.
- A disk I/O error while reading a memory-mapped value raises `SIGBUS` and crashes the process.
- Mapped values live in the page cache: file-backed, reclaimable memory, not heap.
- A value stays valid while its `Snapshot` is reachable. Without the `Snapshot`, it stays valid at least until the end of the first compaction that starts after the read, and while the database is reachable once closed or failed. Copy values you keep longer (`bytes.Clone`, `string(v)`).

## Test

```sh
go test -race ./...
ERMITE_KV_CRASH_ITERATIONS=200 go test -run TestCrash .   # SIGKILL crash test
ERMITE_KV_LOG_LEVEL=debug go test -run TestPowerLoss .    # with logs
```

## License

MIT. See [LICENSE](LICENSE).
