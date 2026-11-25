// SPDX-License-Identifier: MIT

package kv

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"bnema.dev/ermite-kv/internal/fsys"
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
	size, w, err := db.writeSnapshotFile(snap)
	if w != nil {
		db.sendCold(w)
	}
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

// writeSnapshotFile durably creates snapshot-<snap.Seq()> and returns its
// size and, if some values can be served from the new file, the cold work for
// the writer.
func (db *DB) writeSnapshotFile(snap *Snapshot) (int64, *coldWork, error) {
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
		return 0, nil, fmt.Errorf("write snapshot: %w", err)
	}
	size, _ := f.Size() // informational
	w := db.coldWork(f, filepath.Join(db.dir, snapshotName(seq)), seq)
	_ = f.Close() // the file is synced and renamed; nothing is pending
	return size, w, nil
}

// coldWork is the set of values of a new snapshot file that the writer can
// serve from its mapping instead of the heap.
type coldWork struct {
	m     *mapping
	cands []coldCand
}

// coldCand is one large value of the snapshot. key and value alias m.data:
// the writer keeps the tree's existing heap key (setValue) and only uses key
// to find the entry.
type coldCand struct {
	key     []byte
	version uint64
	value   []byte
}

// coldWork maps the synced snapshot file f, stored at path, and collects its
// values of at least Options.ColdValueBytes. It returns nil when the option
// is disabled, when there is no such value, or on failure, which only logs a
// warning: the compaction itself has succeeded.
func (db *DB) coldWork(f fsys.File, path string, seq uint64) *coldWork {
	if db.opts.ColdValueBytes < 0 {
		return nil
	}
	data, unmap, err := f.Map()
	if err != nil {
		db.log.Warn().Err(err).Str("file", path).Msg("cannot map new snapshot")
		return nil
	}
	view, err := openSnapshot(data)
	if err == nil && view.seq != seq {
		err = fmt.Errorf("%w: snapshot seq %d, want %d", ErrCorrupt, view.seq, seq)
	}
	if err != nil {
		_ = unmap()
		db.log.Warn().Err(err).Str("file", path).Msg("new snapshot does not validate once mapped")
		return nil
	}
	var cands []coldCand
	view.each(func(key, value []byte, version uint64) {
		if len(value) >= db.opts.ColdValueBytes {
			cands = append(cands, coldCand{key: key, version: version, value: value})
		}
	})
	if len(cands) == 0 {
		_ = unmap()
		return nil
	}
	return &coldWork{m: newMapping(data, unmap, path), cands: cands}
}

// sendCold hands w to the writer through the one-slot db.coldCh. A job still
// waiting in the slot is dropped first: w comes from a newer snapshot and
// supersedes it. Only the compactor sends and at most one compactor runs, so
// the slot is free after the drain and the send never blocks, even once the
// writer has exited.
func (db *DB) sendCold(w *coldWork) {
	select {
	case <-db.coldCh:
	default:
	}
	select {
	case db.coldCh <- w:
	default: // unreachable while a single compactor sends
	}
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
