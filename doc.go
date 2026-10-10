// SPDX-License-Identifier: MIT

// Package kv is an embedded key-value store. The whole dataset lives in
// memory as immutable snapshots and is persisted to an append-only log.
// A single writer goroutine applies batches and groups their fsync calls;
// readers load the current snapshot without locks.
//
// # Guarantees
//
//   - Durability: DB.Apply returns only after the batch is written and
//     synced (fdatasync), unless Options.NoSync is set. After a crash or a
//     power loss, Open recovers every acknowledged batch, and the state is
//     exactly a prefix of the applied batches.
//   - Atomicity: a Batch is applied entirely or not at all. Its
//     preconditions (RequireAbsent, RequireVersion) are checked against the
//     latest state before any op; a failed precondition returns ErrConflict.
//   - Visibility: a successful Apply is visible to the next DB.Snapshot or
//     DB.Get (read-your-writes). Nothing is visible before it is durable.
//   - Snapshots are immutable and safe for concurrent use. Keys yielded by
//     iterators and Item.Value are owned by the snapshot and must not be
//     modified.
//   - ErrFailed means a write or sync failed. The outcome of the batches in
//     that commit group is unknown (they may be replayed by the next Open);
//     every later Apply fails, while reads keep serving the last snapshot.
//     Close the DB and open it again to recover.
//   - ErrCorrupt means Open found damage that is not a torn tail of the last
//     log. Open never silently drops acknowledged data.
//
// One process at a time may open a directory (ErrLocked otherwise). Linux
// only. Logs go through the zerowrap logger carried by the ctx given to Open.
package kv
