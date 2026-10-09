// SPDX-License-Identifier: MIT

// Package kv is an embedded key-value store. The whole dataset lives in
// memory as immutable snapshots and is persisted to an append-only log.
// A single writer goroutine applies batches and groups their fsync calls;
// readers load the current snapshot without locks.
package kv
