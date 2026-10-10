// SPDX-License-Identifier: MIT

package kv

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"
)

// maybeCompact starts a compaction once the current log holds more than
// Options.CompactAfter bytes after its header, unless one is already running.
// It runs on the writer goroutine after the group has been answered and
// reports false if the database failed.
func (db *DB) maybeCompact() bool {
	if db.opts.CompactAfter < 0 || db.compacting.Load() ||
		db.logSize-logHeaderSize <= db.opts.CompactAfter || db.logSize < db.retryAt {
		return true
	}
	return db.switchLog()
}

// switchLog ends the current log at the published seq S and starts the
// compactor on the snapshot at S: SyncAll the current log, create log-S, make
// it the current log, then compact in the background. It never waits for the
// compactor, so the writer is not blocked by the snapshot size.
//
// Failure handling:
//   - SyncAll of the current log fails: the database fails. With NoSync this
//     is the first sync of the log's data, and after a failed fsync the state
//     of the file is unknown, so the writer must not go on appending.
//   - Creating log-S fails: a warning is logged and appends continue on the
//     current log (nothing is lost, only compaction is postponed). The next
//     attempt waits for another CompactAfter bytes, so that a persistent
//     failure does not cost a sync per commit. If createFile renamed log-S
//     before failing, it is removed: left behind, it would become the last
//     log while records go on in the old one, and the next Open would find a
//     last log that ends below the recovered seq (ErrCorrupt). If that removal
//     fails too, the database fails, because the directory may no longer open.
//
// It reports false if the database failed.
func (db *DB) switchLog() bool {
	start := time.Now()
	snap := db.snap.Load() // the writer is the only publisher: this is the seq of the log
	seq := snap.Seq()
	if err := db.file.SyncAll(); err != nil {
		_ = db.fail(fmt.Errorf("sync log before switch: %w", err))
		return false
	}
	name := logName(seq)
	header := appendLogHeader(nil, seq)
	f, err := createFile(db.fx, db.dir, name, func(w io.Writer) error {
		_, err := w.Write(header)
		return err
	})
	if err != nil {
		db.log.Warn().Err(err).Uint64("seq", seq).Msg("log switch failed")
		if rerr := db.removeStrayLog(filepath.Join(db.dir, name)); rerr != nil {
			_ = db.fail(fmt.Errorf("remove incomplete log %s: %w", name, rerr))
			return false
		}
		db.retryAt = db.logSize + db.opts.CompactAfter
		return true
	}

	from := db.logBase
	old := db.file
	db.file, db.logBase, db.logSize, db.retryAt = f, seq, logHeaderSize, 0
	if err := old.Close(); err != nil {
		db.log.Warn().Err(err).Uint64("base", from).Msg("closing the previous log")
	}
	db.log.Debug().Uint64("from", from).Uint64("seq", seq).Dur("duration", time.Since(start)).Msg("log switch")

	db.compacting.Store(true)
	db.compactors.Go(func() {
		defer db.compacting.Store(false)
		db.compact(snap)
	})
	return true
}

// removeStrayLog deletes a log file that createFile may have left in place
// when it failed, and makes the removal durable.
func (db *DB) removeStrayLog(path string) error {
	err := db.fx.Remove(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return db.fx.SyncDir(db.dir)
}

// compact writes snapshot-<seq> from snap and deletes the snapshots and logs
// older than it. A failure is logged and never fails the database: the next
// trigger starts again from a newer snapshot.
func (db *DB) compact(snap *Snapshot) {
	start := time.Now()
	seq := snap.Seq()
	size, err := db.writeSnapshotFile(snap)
	if err == nil {
		err = db.removeObsolete(seq)
	}
	if err != nil {
		db.log.Warn().Err(err).Uint64("seq", seq).Msg("compaction failed")
		return
	}
	db.log.Info().Uint64("seq", seq).Int("keys", snap.Len()).Int64("bytes", size).
		Dur("duration", time.Since(start)).Msg("compaction done")
}

// writeSnapshotFile durably creates snapshot-<snap.Seq()> and returns its size.
func (db *DB) writeSnapshotFile(snap *Snapshot) (int64, error) {
	seq := snap.Seq()
	entries := func(yield func(key, value []byte, version uint64) bool) {
		for k, it := range snap.t.ascend(nil, nil) {
			if !yield(k, it.Value, it.Version) {
				return
			}
		}
	}
	f, err := createFile(db.fx, db.dir, snapshotName(seq), func(w io.Writer) error {
		return writeSnapshot(w, seq, uint64(snap.Len()), entries)
	})
	if err != nil {
		return 0, fmt.Errorf("write snapshot: %w", err)
	}
	size, _ := f.Size() // informational
	_ = f.Close()       // the file is synced and renamed; nothing is pending
	return size, nil
}

// removeObsolete deletes the snapshots and logs with a seq below seq, then
// syncs the directory. It tries every file and returns the first error. The
// files are removed only after snapshot-<seq> is durable, so a crash at any
// point leaves a recoverable directory.
func (db *DB) removeObsolete(seq uint64) error {
	names, err := db.fx.ReadDir(db.dir)
	if err != nil {
		return fmt.Errorf("list obsolete files: %w", err)
	}
	var first error
	for _, name := range names {
		n, ok := parseSeqName(name, logPrefix)
		if !ok {
			n, ok = parseSeqName(name, snapshotPrefix)
		}
		if !ok || n >= seq {
			continue
		}
		if err := db.fx.Remove(filepath.Join(db.dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) && first == nil {
			first = fmt.Errorf("remove %s: %w", name, err)
		}
	}
	if err := db.fx.SyncDir(db.dir); err != nil && first == nil {
		first = fmt.Errorf("sync directory: %w", err)
	}
	return first
}
