# ermite-kv

> Read-only mirror. Development happens at git.bnema.dev; issues and pull requests are closed here.

An embedded key-value store for Go. Data lives in memory and is persisted to an append-only log with snapshots. One writer goroutine groups commits into a single `fsync`, and readers use immutable snapshots without locks. Standard library only, plus [zerowrap](https://github.com/bnema/zerowrap) for logs.

Status: early development, no stable API. Linux only.

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

The package documentation describes the durability and visibility guarantees.

## Test

```sh
go test -race ./...
ERMITE_KV_CRASH_ITERATIONS=200 go test -run TestCrash .   # SIGKILL crash test
ERMITE_KV_LOG_LEVEL=debug go test -run TestPowerLoss .    # with logs
```

## License

MIT. See [LICENSE](LICENSE).
