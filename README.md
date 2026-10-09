# ermite-kv

An embedded key-value store for Go. Data lives in memory and is persisted to an append-only log. One writer goroutine groups commits into a single `fsync`, and readers use immutable snapshots without locks. Standard library only.

Status: early development, no stable API.

```go
import kv "bnema.dev/ermite-kv"
```

## Test

```sh
go test ./...
```

## License

MIT. See [LICENSE](LICENSE).
