// SPDX-License-Identifier: MIT

package kv

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"bnema.dev/ermite-kv/internal/fsys"
)

const (
	lockName       = "LOCK"
	snapshotPrefix = "snapshot-"
	logPrefix      = "log-"
	tmpSuffix      = ".tmp"
)

func snapshotName(seq uint64) string { return fmt.Sprintf("%s%016x", snapshotPrefix, seq) }
func logName(base uint64) string     { return fmt.Sprintf("%s%016x", logPrefix, base) }

// parseSeqName parses "<prefix><seq:016x>"; ok is false for any other name.
func parseSeqName(name, prefix string) (seq uint64, ok bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	seq, err := strconv.ParseUint(name[len(prefix):], 16, 64)
	if err != nil || fmt.Sprintf("%s%016x", prefix, seq) != name {
		return 0, false
	}
	return seq, true
}

// recovered is the state handed to the DB by recoverDir.
type recovered struct {
	snapSeq uint64 // seq of the loaded snapshot, 0 if none
	lastSeq uint64 // seq of the last applied record (snapSeq if none)

	// log is the last log file, opened read-write with appending writes. It
	// is at logBase and holds logSize bytes (header included) after recovery.
	// The caller owns it.
	log     fsys.File
	logBase uint64
	logSize int64

	// lock holds the directory lock; closing it releases the lock. The
	// caller owns it.
	lock io.Closer
}

// recoverDir opens the database directory and rebuilds its state: recovery
// rule 1 (directory, lock, *.tmp files) and rules 2–7. It does not depend on
// the in-memory tree.
//
// load is called once per snapshot entry in ascending key order, only after
// the whole snapshot file has been validated. apply is called for each log
// record newer than the snapshot, in seq order. Both receive freshly
// allocated slices that the callee may retain but must not modify; recovery
// never reuses them (snapshot entries share one read-only buffer, log ops are
// copied per record). Either callback may be nil. A nil logger discards logs.
//
// On error nothing stays open or locked. ErrLocked and ErrCorrupt are
// returned wrapped.
func recoverDir(fx fsys.FS, dir string, logger *slog.Logger,
	load func(key, value []byte, version uint64),
	apply func(seq uint64, ops []op),
) (rec recovered, err error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	dir = filepath.Clean(dir)
	if err := fx.MkdirAll(dir, 0o700); err != nil {
		return rec, err
	}
	if err := fx.SyncDir(dir); err != nil {
		return rec, err
	}
	if parent := filepath.Dir(dir); parent != dir {
		if err := fx.SyncDir(parent); err != nil {
			return rec, err
		}
	}
	lock, err := fx.Lock(filepath.Join(dir, lockName))
	if errors.Is(err, fsys.ErrLocked) {
		return rec, fmt.Errorf("%w: %s", ErrLocked, dir)
	}
	if err != nil {
		return rec, err
	}
	rec.lock = lock
	defer func() {
		if err != nil {
			if rec.log != nil {
				_ = rec.log.Close()
			}
			_ = lock.Close()
			rec = recovered{}
		}
	}()

	names, err := fx.ReadDir(dir)
	if err != nil {
		return rec, err
	}
	var snaps, logs []uint64
	for _, name := range names {
		if strings.HasSuffix(name, tmpSuffix) {
			if err := fx.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return rec, err
			}
		} else if seq, ok := parseSeqName(name, snapshotPrefix); ok {
			snaps = append(snaps, seq)
		} else if base, ok := parseSeqName(name, logPrefix); ok {
			logs = append(logs, base)
		}
	}
	if err := fx.SyncDir(dir); err != nil {
		return rec, err
	}

	// Rule 2: load the highest-seq snapshot that validates.
	slices.Sort(snaps)
	for i := len(snaps) - 1; i >= 0; i-- {
		path := filepath.Join(dir, snapshotName(snaps[i]))
		data, err := readFile(fx, path)
		if err != nil {
			return rec, err
		}
		view, err := openSnapshot(data)
		if err == nil && view.seq != snaps[i] {
			err = fmt.Errorf("%w: snapshot seq %d does not match its name", ErrCorrupt, view.seq)
		}
		if err != nil {
			logger.Warn("kv: ignoring invalid snapshot", "file", path, "err", err)
			continue
		}
		if load != nil {
			view.each(load)
		}
		rec.snapSeq = view.seq
		break
	}
	rec.lastSeq = rec.snapSeq

	// Rules 3–6: replay the logs in base order.
	slices.Sort(logs)
	for i, base := range logs {
		last := i == len(logs)-1
		path := filepath.Join(dir, logName(base))
		if !last {
			data, err := readFile(fx, path)
			if err != nil {
				return rec, err
			}
			if _, err := replayLog(nil, data, path, base, &rec, logger, apply); err != nil {
				return rec, err
			}
			continue
		}
		f, err := fx.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return rec, err
		}
		data, err := readAll(f)
		if err == nil {
			rec.logSize, err = replayLog(f, data, path, base, &rec, logger, apply)
		}
		if err != nil {
			_ = f.Close()
			return rec, err
		}
		rec.log, rec.logBase = f, base
	}

	// Rule 7: no log file at all.
	if rec.log == nil {
		name := logName(rec.snapSeq)
		header := appendLogHeader(nil, rec.snapSeq)
		f, err := createFile(fx, dir, name, func(w io.Writer) error {
			_, err := w.Write(header)
			return err
		})
		if err != nil {
			return rec, err
		}
		rec.log, rec.logBase, rec.logSize = f, rec.snapSeq, logHeaderSize
	}
	return rec, nil
}

// replayLog applies the records of the log file contents data (rules 3–6)
// and updates rec.lastSeq. f is nil for a log that is not the last one; for
// the last log it is used to truncate a torn tail. It returns the size of the
// log after recovery.
func replayLog(f fsys.File, data []byte, path string, base uint64, rec *recovered,
	logger *slog.Logger, apply func(seq uint64, ops []op),
) (int64, error) {
	last := f != nil
	corrupt := func(off int, format string, args ...any) error {
		return fmt.Errorf("%w: %s at offset %d: %s", ErrCorrupt, path, off, fmt.Sprintf(format, args...))
	}
	hdrBase, err := decodeLogHeader(data)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	if hdrBase != base {
		return 0, corrupt(0, "header base %d does not match the file name", hdrBase)
	}
	if base > rec.lastSeq {
		return 0, corrupt(0, "base %d is above the last recovered seq %d", base, rec.lastSeq)
	}

	var ops []op
	expected := base + 1 // seq of the next record of this file
	off := logHeaderSize
	for off < len(data) {
		seq, decoded, n, err := decodeRecord(data[off:], ops)
		if err != nil {
			if !last {
				return 0, corrupt(off, "%v", err)
			}
			// Rule 6: a readable next record means acknowledged data
			// lies beyond the damage.
			if total, ok := peekRecord(data[off:]); ok && off+total < len(data) {
				nseq, _, _, nerr := decodeRecord(data[off+total:], nil)
				if nerr == nil && nseq == expected+1 {
					return 0, corrupt(off, "%v, followed by a valid record", err)
				}
			}
			if err := f.Truncate(int64(off)); err != nil {
				return 0, err
			}
			if err := f.SyncAll(); err != nil {
				return 0, err
			}
			logger.Warn("kv: dropped torn log tail", "file", path, "offset", off, "bytes", len(data)-off)
			data = data[:off]
			break
		}
		ops = decoded[:0]
		if seq != expected {
			return 0, corrupt(off, "record seq %d, want %d", seq, expected)
		}
		if seq > rec.snapSeq {
			if seq != rec.lastSeq+1 {
				return 0, corrupt(off, "record seq %d, want %d", seq, rec.lastSeq+1)
			}
			if apply != nil {
				apply(seq, ownOps(decoded))
			}
			rec.lastSeq = seq
		}
		expected++
		off += n
	}
	if last && expected-1 < rec.lastSeq {
		// Cannot happen unless records acknowledged before the snapshot
		// are missing: appending would create a seq gap.
		return 0, corrupt(len(data), "log ends at seq %d, want %d", expected-1, rec.lastSeq)
	}
	return int64(len(data)), nil
}

// readFile returns the whole content of the file at path.
func readFile(fx fsys.FS, path string) ([]byte, error) {
	f, err := fx.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	data, err := readAll(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return data, err
}

// readAll returns the whole content of f.
func readAll(f fsys.File) ([]byte, error) {
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	n, err := f.ReadAt(data, 0)
	if n == len(data) {
		return data, nil // io.EOF is allowed when the read ends at EOF
	}
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return nil, err
}

// createFile durably creates dir/name with the content produced by write:
// name.tmp is written and fsynced, renamed to name, then dir is synced. The
// returned file is open for appending (and reading) at its end. On error the
// temporary file is removed on a best-effort basis.
func createFile(fx fsys.FS, dir, name string, write func(w io.Writer) error) (fsys.File, error) {
	tmp := filepath.Join(dir, name+tmpSuffix)
	f, err := fx.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (fsys.File, error) {
		_ = f.Close()
		_ = fx.Remove(tmp)
		return nil, err
	}
	if err := write(f); err != nil {
		return fail(err)
	}
	if err := f.SyncAll(); err != nil {
		return fail(err)
	}
	if err := fx.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return fail(err)
	}
	if err := fx.SyncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
