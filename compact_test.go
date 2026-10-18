// SPDX-License-Identifier: MIT

package kv

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bnema/zerowrap"

	"bnema.dev/ermite-kv/internal/fsys"
	"bnema.dev/ermite-kv/internal/fsys/faultfs"
)

var errCrash = errors.New("test: power was cut")

// hookFS wraps an FS and intercepts the operations that change files:
// hook (if set) may block or fail them, and a power loss can be cut at a
// given step. With serial set, operations run one at a time under mu, so a
// cut is atomic with respect to every other operation and nothing touches the
// file system afterwards: once cut, every operation fails with errCrash, like
// a process that no longer exists.
type hookFS struct {
	fsys.FS
	ff     *faultfs.FS                 // underlying FS, needed by cut
	serial bool                        // run operations under mu
	hook   func(op, path string) error // optional; set before use

	mu      sync.Mutex // guards the fields below when serial
	armed   bool       // record the operations in trace
	trace   []string
	crashAt int // cut the power before the crashAt-th traced operation (0: never)
	plo     faultfs.PowerLossOptions
	dead    bool
}

func (h *hookFS) run(op, path string, fn func() error) error {
	if h.serial {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.dead {
			return errCrash
		}
		if h.armed {
			h.trace = append(h.trace, op+" "+filepath.Base(path))
			if len(h.trace) == h.crashAt {
				h.cutLocked(h.plo)
				return errCrash
			}
		}
	}
	if h.hook != nil {
		if err := h.hook(op, path); err != nil {
			return err
		}
	}
	return fn()
}

func (h *hookFS) arm() {
	h.mu.Lock()
	h.armed = true
	h.mu.Unlock()
}

func (h *hookFS) isDead() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dead
}

func (h *hookFS) steps() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.trace)
}

// cut cuts the power: the unsynced state is dropped as plo says and every
// later operation fails.
func (h *hookFS) cut(plo faultfs.PowerLossOptions) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.dead {
		h.cutLocked(plo)
	}
}

func (h *hookFS) cutLocked(plo faultfs.PowerLossOptions) {
	h.dead = true
	h.ff.PowerLoss(plo)
}

func (h *hookFS) OpenFile(name string, flag int, perm os.FileMode) (fsys.File, error) {
	var f fsys.File
	err := h.run("open", name, func() (err error) {
		f, err = h.FS.OpenFile(name, flag, perm)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &hookFile{File: f, fs: h, path: name}, nil
}

func (h *hookFS) Rename(oldpath, newpath string) error {
	return h.run("rename", newpath, func() error { return h.FS.Rename(oldpath, newpath) })
}

func (h *hookFS) Remove(name string) error {
	return h.run("remove", name, func() error { return h.FS.Remove(name) })
}

func (h *hookFS) SyncDir(dir string) error {
	return h.run("syncdir", dir, func() error { return h.FS.SyncDir(dir) })
}

type hookFile struct {
	fsys.File
	fs   *hookFS
	path string
}

func (f *hookFile) Write(p []byte) (n int, err error) {
	err = f.fs.run("write", f.path, func() (err error) {
		n, err = f.File.Write(p)
		return err
	})
	return n, err
}

func (f *hookFile) Sync() error    { return f.fs.run("sync", f.path, f.File.Sync) }
func (f *hookFile) SyncAll() error { return f.fs.run("syncall", f.path, f.File.SyncAll) }

func (f *hookFile) Truncate(size int64) error {
	return f.fs.run("truncate", f.path, func() error { return f.File.Truncate(size) })
}

// The sequential workload: batch i (which gets seq i) puts one key.
const seqPad = "................................"

func seqKey(i int) []byte   { return fmt.Appendf(nil, "k%06d", i) }
func seqValue(i int) []byte { return fmt.Appendf(nil, "v%06d%s", i, seqPad) }

// recSize is the encoded size of the log record of one sequential batch; it
// does not depend on i. Options.CompactAfter = n*recSize makes the (n+1)-th
// record of a log trigger a compaction.
var recSize = int64(len(appendRecord(nil, 1, []op{{kind: opPut, key: seqKey(1), value: seqValue(1)}})))

// compactOpts compacts when the 6th record of a log is written.
func compactOpts() Options { return Options{CompactAfter: 5 * recSize} }

// applySeq applies batch i and checks that it gets seq i.
func applySeq(db *DB, i int) error {
	var b Batch
	b.Put(seqKey(i), seqValue(i))
	seq, err := db.Apply(bg, &b)
	if err == nil && seq != uint64(i) {
		err = fmt.Errorf("batch %d got seq %d", i, seq)
	}
	return err
}

func mustApply(t testing.TB, db *DB, i int) {
	t.Helper()
	if err := applySeq(db, i); err != nil {
		t.Fatalf("apply %d: %v", i, err)
	}
}

// barrier returns once the writer has finished everything it did for the
// batches applied so far, including a log switch: an empty batch is only
// taken after the previous loop iteration, which ends with the compaction
// trigger.
func barrier(t testing.TB, db *DB) {
	t.Helper()
	if _, err := db.Apply(bg, new(Batch)); err != nil {
		t.Fatalf("barrier: %v", err)
	}
}

// checkSequential checks that s holds exactly the keys of batches 1..N for
// some N ≥ lastAcked, with the right values and versions, and returns N.
func checkSequential(s *Snapshot, lastAcked int) (int, error) {
	n := s.Len()
	if s.Seq() != uint64(n) {
		return n, fmt.Errorf("seq %d but %d keys", s.Seq(), n)
	}
	if n < lastAcked {
		return n, fmt.Errorf("acknowledged batch %d lost: recovered %d batches", lastAcked, n)
	}
	for i := 1; i <= n; i++ {
		it, ok := s.Get(bg, seqKey(i))
		if !ok || string(it.Value) != string(seqValue(i)) || it.Version != uint64(i) {
			return n, fmt.Errorf("key %d: %q at version %d, present %v", i, it.Value, it.Version, ok)
		}
	}
	return n, nil
}

func mustCheck(t testing.TB, db *DB, lastAcked int) int {
	t.Helper()
	n, err := checkSequential(db.Snapshot(bg), lastAcked)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// listFiles returns the seqs of the snapshot and log files in testDir.
func listFiles(t testing.TB, fx fsys.FS) (snaps, logs []uint64) {
	t.Helper()
	names, err := fx.ReadDir(testDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if s, ok := parseSeqName(name, snapshotPrefix); ok {
			snaps = append(snaps, s)
		} else if b, ok := parseSeqName(name, logPrefix); ok {
			logs = append(logs, b)
		}
	}
	return snaps, logs
}

// settled reports whether the directory holds exactly snapshot-S and log-S.
func settled(t testing.TB, fx fsys.FS, s uint64) bool {
	snaps, logs := listFiles(t, fx)
	return slices.Equal(snaps, []uint64{s}) && slices.Equal(logs, []uint64{s})
}

// settle returns once every goroutine of the bubble is durably blocked: the
// writer and any compactor have finished all the work they can do. It must be
// called inside a synctest bubble.
func settle() { synctest.Wait() }

// applyUntil applies batches i, i+1, … until cond holds, settling after each
// one so that a compaction the batch triggered has finished. It returns the
// next unused i.
func applyUntil(t testing.TB, db *DB, i int, what string, cond func() bool) int {
	t.Helper()
	settle()
	for ; !cond(); i++ {
		if i > 1000 {
			t.Fatalf("%s did not happen", what)
		}
		mustApply(t, db, i)
		settle()
	}
	return i
}

// failedDB reports whether the database is in the failed state.
func failedDB(db *DB) bool {
	select {
	case <-db.failed:
		return true
	default:
		return false
	}
}

// debugCtx returns a context whose logger writes JSON at debug level to the
// returned buffer.
func debugCtx() (context.Context, *syncBuffer) {
	buf := new(syncBuffer)
	return zerowrap.WithCtx(bg, zerowrap.New(zerowrap.Config{Level: "debug", Format: "json", Output: buf})), buf
}

func logged(buf *syncBuffer, msg string) int {
	return strings.Count(buf.String(), `"message":"`+msg+`"`)
}

func TestCompactTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := faultfs.New()
		ctx, logs := debugCtx()
		db := mustOpenCtx(t, ctx, fx, compactOpts())

		// Exactly CompactAfter bytes after the header: not above the threshold.
		for i := 1; i <= 5; i++ {
			mustApply(t, db, i)
		}
		barrier(t, db)
		if snaps, files := listFiles(t, fx); len(snaps) != 0 || !slices.Equal(files, []uint64{0}) {
			t.Fatalf("below the threshold: snapshots %v, logs %v", snaps, files)
		}

		mustApply(t, db, 6)
		settle() // compaction: the background work has finished
		if logged(logs, "compaction done") != 1 {
			t.Fatalf("compaction did not happen: %s", logs.String())
		}
		if !settled(t, fx, 6) {
			snaps, files := listFiles(t, fx)
			t.Fatalf("after compaction: snapshots %v, logs %v; want snapshot-6 and log-6 only", snaps, files)
		}
		if n := logged(logs, "log switch"); n != 1 {
			t.Fatalf("%d log switch messages, want 1", n)
		}
		for _, key := range []string{`"seq":6`, `"keys":6`, `"bytes":`, `"duration":`} {
			if !strings.Contains(logs.String(), key) {
				t.Errorf("compaction message lacks %s: %s", key, logs.String())
			}
		}

		// The new log takes the following batches; the old files stay gone.
		mustApply(t, db, 7)
		if err := db.Close(bg); err != nil {
			t.Fatal(err)
		}
		db = mustOpen(t, fx, Options{})
		if n := mustCheck(t, db, 7); n != 7 {
			t.Fatalf("recovered %d batches, want 7", n)
		}
	})
}

func TestCompactDisabled(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{CompactAfter: -1})
	for i := 1; i <= 50; i++ {
		mustApply(t, db, i)
	}
	barrier(t, db)
	if snaps, files := listFiles(t, fx); len(snaps) != 0 || !slices.Equal(files, []uint64{0}) {
		t.Fatalf("snapshots %v, logs %v", snaps, files)
	}
}

func TestCompactSingleFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := faultfs.New()
		started, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		h := &hookFS{FS: fx, hook: func(op, path string) error {
			if op == "open" && strings.Contains(path, snapshotPrefix) {
				once.Do(func() { close(started); <-release })
			}
			return nil
		}}
		ctx, logs := debugCtx()
		db := mustOpenCtx(t, ctx, h, compactOpts())
		for i := 1; i <= 6; i++ {
			mustApply(t, db, i)
		}
		settle() // the compactor is parked in the hook
		select {
		case <-started:
		default:
			t.Fatal("the compactor did not start")
		}

		// The compactor is stuck. The writer is not: it goes on far past the
		// threshold without starting a second compaction.
		for i := 7; i <= 60; i++ {
			mustApply(t, db, i)
		}
		barrier(t, db)
		if snaps, files := listFiles(t, fx); len(snaps) != 0 || !slices.Equal(files, []uint64{0, 6}) {
			t.Fatalf("while compacting: snapshots %v, logs %v", snaps, files)
		}
		if n := logged(logs, "log switch"); n != 1 {
			t.Fatalf("%d log switches while a compaction was running, want 1", n)
		}

		close(release)
		settle() // first compaction: the background work has finished
		if logged(logs, "compaction done") != 1 {
			t.Fatalf("first compaction did not happen: %s", logs.String())
		}
		// The log is still over the threshold: the next commit compacts again.
		i := 61
		i = applyUntil(t, db, i, "second compaction", func() bool { return logged(logs, "compaction done") >= 2 })
		if n := logged(logs, "log switch"); n != 2 {
			t.Fatalf("%d log switches, want 2", n)
		}
		if err := db.Close(bg); err != nil {
			t.Fatal(err)
		}
		snaps, files := listFiles(t, fx)
		if len(snaps) != 1 || !slices.Equal(files, snaps) || snaps[0] <= 6 {
			t.Fatalf("after the second compaction: snapshots %v, logs %v", snaps, files)
		}
		db = mustOpen(t, fx, Options{})
		mustCheck(t, db, i-1)
	})
}

func TestCompactRemovesOldFiles(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, compactOpts())
	const total = 200
	for i := 1; i <= total; i++ {
		mustApply(t, db, i)
	}
	if err := db.Close(bg); err != nil { // waits for the last compaction
		t.Fatal(err)
	}
	snaps, files := listFiles(t, fx)
	if len(snaps) != 1 || !slices.Equal(files, snaps) || snaps[0] == 0 {
		t.Fatalf("snapshots %v, logs %v: want only the newest pair", snaps, files)
	}
	names, _ := fx.ReadDir(testDir)
	for _, n := range names {
		if strings.HasSuffix(n, tmpSuffix) {
			t.Errorf("leftover %s", n)
		}
	}
	db = mustOpen(t, fx, compactOpts())
	if n := mustCheck(t, db, total); n != total {
		t.Fatalf("recovered %d batches, want %d", n, total)
	}
	// A reopened database keeps compacting.
	for i := total + 1; i <= 2*total; i++ {
		mustApply(t, db, i)
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, fx, Options{})
	mustCheck(t, db, 2*total)
}

// TestCompactOS runs compaction on the real file system.
func TestCompactOS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	ctx, logs := debugCtx()
	db, err := Open(ctx, dir, compactOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		mustApply(t, db, i)
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	if logged(logs, "compaction done") == 0 {
		t.Fatal("no compaction on the OS file system")
	}
	db, err = Open(bg, dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(bg) }()
	mustCheck(t, db, 100)
}

// compactorFaults fail one step of the compactor, which runs after the log
// switch. The injection happens before batch 6 is applied, so the numbering
// counts from the group write: Write 1 is the group, 2 the header of log-6, 3
// the snapshot; the syncs count Sync and SyncAll (1 the group, 2 the log
// before the switch, 3 log-6, 4 the snapshot); SyncDir 1 is log-6, 2 the
// snapshot, 3 the cleanup; Rename 1 is log-6, 2 the snapshot; OpenFile 1 is
// log-6, 2 the snapshot; Remove 1 is log-0.
var compactorFaults = []struct {
	name   string
	inject func(fx *faultfs.FS)
}{
	{"open snapshot", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpOpenFile, 2, nil) }},
	{"write snapshot", func(fx *faultfs.FS) { fx.FailWrite(3, nil) }},
	{"short write snapshot", func(fx *faultfs.FS) { fx.ShortWrite(3, 10) }},
	{"sync snapshot", func(fx *faultfs.FS) { fx.FailSync(4, nil) }},
	{"rename snapshot", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpRename, 2, nil) }},
	{"syncdir snapshot", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpSyncDir, 2, nil) }},
	{"remove old log", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpRemove, 1, nil) }},
	{"syncdir cleanup", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpSyncDir, 3, nil) }},
}

// TestCompactorFailure checks that a failing compactor is logged, leaves the
// database usable and a recoverable directory, and that the next trigger
// compacts.
func TestCompactorFailure(t *testing.T) {
	for _, f := range compactorFaults {
		t.Run(f.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fx := faultfs.New()
				ctx, logs := debugCtx()
				db := mustOpenCtx(t, ctx, fx, compactOpts())
				for i := 1; i <= 5; i++ {
					mustApply(t, db, i)
				}
				f.inject(fx)
				mustApply(t, db, 6)
				settle() // compaction failure: the background work has finished
				if logged(logs, "compaction failed") != 1 {
					t.Fatalf("compaction failure did not happen: %s", logs.String())
				}
				if !strings.Contains(logs.String(), `"level":"warn"`) {
					t.Errorf("failure not logged as a warning: %s", logs.String())
				}
				mustApply(t, db, 7) // the database is not failed
				if err := db.Close(bg); err != nil {
					t.Fatalf("Close: %v", err)
				}

				// Reopen the partial state: it recovers and compacts again.
				ctx, logs = debugCtx()
				db = mustOpenCtx(t, ctx, fx, compactOpts())
				mustCheck(t, db, 7)
				i := 8
				i = applyUntil(t, db, i, "retried compaction", func() bool { return logged(logs, "compaction done") >= 1 })
				if err := db.Close(bg); err != nil {
					t.Fatal(err)
				}
				snaps, files := listFiles(t, fx)
				if len(snaps) != 1 || !slices.Equal(files, snaps) || snaps[0] <= 6 {
					t.Fatalf("after the retry: snapshots %v, logs %v", snaps, files)
				}
				db = mustOpen(t, fx, Options{})
				mustCheck(t, db, i-1)
			})
		})
	}
}

// TestCompactorRetryWithoutReopen checks the retry in the same process.
func TestCompactorRetryWithoutReopen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := faultfs.New()
		ctx, logs := debugCtx()
		db := mustOpenCtx(t, ctx, fx, compactOpts())
		for i := 1; i <= 5; i++ {
			mustApply(t, db, i)
		}
		fx.FailOp(faultfs.OpRename, 2, nil) // the snapshot
		mustApply(t, db, 6)
		settle() // compaction failure: the background work has finished
		if logged(logs, "compaction failed") != 1 {
			t.Fatalf("compaction failure did not happen: %s", logs.String())
		}
		applyUntil(t, db, 7, "retried compaction", func() bool { return logged(logs, "compaction done") >= 1 })
		if err := db.Close(bg); err != nil {
			t.Fatal(err)
		}
		snaps, files := listFiles(t, fx)
		if len(snaps) != 1 || !slices.Equal(files, snaps) || snaps[0] <= 6 {
			t.Fatalf("snapshots %v, logs %v", snaps, files)
		}
	})
}

// switchFaults fail the creation of log-6 in the writer. The numbering is
// the one of compactorFaults.
var switchFaults = []struct {
	name   string
	inject func(fx *faultfs.FS)
}{
	{"open", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpOpenFile, 1, nil) }},
	{"write", func(fx *faultfs.FS) { fx.FailWrite(2, nil) }},
	{"sync", func(fx *faultfs.FS) { fx.FailSync(3, nil) }},
	{"rename", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpRename, 1, nil) }},
	{"syncdir", func(fx *faultfs.FS) { fx.FailOp(faultfs.OpSyncDir, 1, nil) }},
}

// TestLogSwitchFailure checks that failing to create the new log only
// postpones the compaction: the database goes on appending to the current
// log and no log-6 is left behind.
func TestLogSwitchFailure(t *testing.T) {
	for _, f := range switchFaults {
		t.Run(f.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fx := faultfs.New()
				ctx, logs := debugCtx()
				db := mustOpenCtx(t, ctx, fx, compactOpts())
				for i := 1; i <= 5; i++ {
					mustApply(t, db, i)
				}
				f.inject(fx)
				mustApply(t, db, 6)
				barrier(t, db)
				if logged(logs, "log switch failed") != 1 {
					t.Fatalf("switch failure not logged: %s", logs.String())
				}
				snaps, files := listFiles(t, fx)
				if len(snaps) != 0 || !slices.Equal(files, []uint64{0}) {
					t.Fatalf("after the failure: snapshots %v, logs %v", snaps, files)
				}
				names, _ := fx.ReadDir(testDir)
				for _, n := range names {
					if strings.HasSuffix(n, tmpSuffix) {
						t.Errorf("leftover %s", n)
					}
				}

				// No retry on every commit: a failure costs another CompactAfter bytes.
				mustApply(t, db, 7)
				barrier(t, db)
				if n := logged(logs, "log switch failed"); n != 1 {
					t.Fatalf("%d switch attempts for 2 commits, want 1", n)
				}
				i := 8
				i = applyUntil(t, db, i, "compaction after the failed switch", func() bool { return logged(logs, "compaction done") >= 1 })
				if err := db.Close(bg); err != nil {
					t.Fatal(err)
				}
				db = mustOpen(t, fx, Options{})
				mustCheck(t, db, i-1)
			})
		})
	}
}

// TestLogSwitchFailsDatabase covers the failures that must fail the database:
// the sync of the current log, and a stray log-S that cannot be removed.
func TestLogSwitchFailsDatabase(t *testing.T) {
	cases := []struct {
		name   string
		inject func(fx *faultfs.FS)
	}{
		{"syncall of the current log", func(fx *faultfs.FS) { fx.FailSync(2, nil) }},
		{"syncdir then remove", func(fx *faultfs.FS) {
			fx.FailOp(faultfs.OpSyncDir, 1, nil)
			fx.FailOp(faultfs.OpRemove, 1, nil)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fx := faultfs.New()
				ctx, logs := debugCtx()
				db := mustOpenCtx(t, ctx, fx, compactOpts())
				for i := 1; i <= 5; i++ {
					mustApply(t, db, i)
				}
				c.inject(fx)
				mustApply(t, db, 6) // acknowledged: the failure comes after the reply
				settle()            // the writer has seen the failure
				if !failedDB(db) {
					t.Fatal("the database did not fail")
				}
				var b Batch
				b.Put([]byte("later"), nil)
				if _, err := db.Apply(bg, &b); !errors.Is(err, ErrFailed) {
					t.Fatalf("Apply after the failure: %v", err)
				}
				if !strings.Contains(logs.String(), "database failed") {
					t.Errorf("failure not logged: %s", logs.String())
				}
				if err := db.Close(bg); !errors.Is(err, ErrFailed) {
					t.Fatalf("Close = %v, want the failure", err)
				}
				db = mustOpen(t, fx, Options{})
				mustCheck(t, db, 6)
			})
		})
	}
}

// TestLogSwitchSyncFailureDropsUnsyncedData is the fsyncgate case with
// NoSync: the SyncAll before the switch is the first sync of the log. When it
// fails and the kernel drops the pages, the batches acknowledged without a
// sync are gone; the database must fail rather than continue, and what is
// left must open as a prefix.
func TestLogSwitchSyncFailureDropsUnsyncedData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := faultfs.New()
		fx.DropOnSyncFailure(true)
		opts := compactOpts()
		opts.NoSync = true
		db := mustOpen(t, fx, opts)
		for i := 1; i <= 5; i++ {
			mustApply(t, db, i)
		}
		fx.FailSync(1, nil)
		mustApply(t, db, 6)
		settle() // the writer has seen the failure
		if !failedDB(db) {
			t.Fatal("the database did not fail")
		}
		if err := db.Close(bg); !errors.Is(err, ErrFailed) {
			t.Fatalf("Close = %v, want the failure", err)
		}
		db = mustOpen(t, fx, Options{})
		if n := mustCheck(t, db, 0); n != 0 {
			t.Fatalf("recovered %d batches, want 0 (all were dropped)", n)
		}
	})
}

// TestCloseWaitsForCompaction checks that Close does not cancel a running
// compaction, waits for it, and leaves a directory that opens.
func TestCloseWaitsForCompaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := faultfs.New()
		started, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		h := &hookFS{FS: fx, hook: func(op, path string) error {
			if op == "open" && strings.Contains(path, snapshotPrefix) {
				once.Do(func() { close(started); <-release })
			}
			return nil
		}}
		ctx, logs := debugCtx()
		db := mustOpenCtx(t, ctx, h, compactOpts())
		for i := 1; i <= 6; i++ {
			mustApply(t, db, i)
		}
		settle() // the compactor is parked in the hook
		select {
		case <-started:
		default:
			t.Fatal("the compactor did not start")
		}
		closed := make(chan error, 1)
		go func() { closed <- db.Close(bg) }()
		settle() // Close is blocked waiting for the compactor, which is parked in the hook
		select {
		case err := <-closed:
			t.Fatalf("Close returned during the compaction: %v", err)
		default:
		}
		if _, err := db.Apply(bg, new(Batch)); !errors.Is(err, ErrClosed) {
			t.Fatalf("Apply while closing: %v", err)
		}
		close(release)
		settle() // the compaction finishes and Close returns
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("Close: %v", err)
			}
		default:
			t.Fatal("Close did not return after the compaction was released")
		}
		if logged(logs, "compaction done") != 1 || !settled(t, fx, 6) {
			t.Fatalf("the compaction did not complete: %s", logs.String())
		}
		db = mustOpen(t, fx, Options{})
		mustCheck(t, db, 6)
	})
}

// TestCloseDuringCompaction (AC9) closes the database while writers run and
// compactions are frequent: Close returns, and the next Open finds every
// acknowledged batch.
func TestCloseDuringCompaction(t *testing.T) {
	var switches atomic.Int64
	for i := range short(12, 4) {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			fx := faultfs.New()
			fx.SetSyncDelay(50 * time.Microsecond)
			ctx, logs := debugCtx()
			db := mustOpenCtx(t, ctx, fx, Options{CompactAfter: 2 * recSize})
			const writers = 16
			acked := make([]int, writers) // highest acknowledged n+1
			var wg sync.WaitGroup
			for g := range writers {
				wg.Go(func() {
					for n := 0; ; n++ {
						var b Batch
						b.Put(fmt.Appendf(nil, "g%02d", g), seqValue(n))
						b.Put(fmt.Appendf(nil, "p%02d/%06d", g, n), nil)
						_, err := db.Apply(bg, &b)
						if errors.Is(err, ErrClosed) {
							return
						}
						if err != nil {
							t.Errorf("writer %d: %v", g, err)
							return
						}
						acked[g] = n + 1
					}
				})
			}
			time.Sleep(time.Duration(5+3*i) * time.Millisecond)
			closed := make(chan error, 1)
			go func() { closed <- db.Close(bg) }()
			select {
			case err := <-closed:
				if err != nil {
					t.Errorf("Close: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("Close did not return")
			}
			wg.Wait()
			switches.Add(int64(logged(logs, "log switch")))

			fx.SetSyncDelay(0)
			s := mustOpen(t, fx, Options{}).Snapshot(bg)
			for g, n := range acked {
				it, ok := s.Get(bg, fmt.Appendf(nil, "g%02d", g))
				if n == 0 {
					continue
				}
				// The batch after the last acknowledged one may have been
				// committed by the final group.
				if !ok || string(it.Value) != string(seqValue(n-1)) && string(it.Value) != string(seqValue(n)) {
					t.Errorf("writer %d: acked %d batches, recovered %q (%v)", g, n, it.Value, ok)
				}
				if _, ok := s.Get(bg, fmt.Appendf(nil, "p%02d/%06d", g, n-1)); !ok {
					t.Errorf("writer %d: acked batch %d lost", g, n-1)
				}
			}
		})
	}
	if switches.Load() == 0 {
		t.Error("no log switch happened: the test did not exercise compaction")
	}
}

// compactSteps runs a workload of three rounds of six batches (each round
// ends with a compaction) on a fresh file system and cuts the power before
// the crashAt-th file operation after Open (0 and out of range: after the
// workload). It then reopens the database and reports a violation of AC2.
// The workload is sequential and waits for each compaction, so the sequence
// of operations is the same in every run.
func compactSteps(t *testing.T, plo faultfs.PowerLossOptions, crashAt int) (steps []string, err error) {
	ff := faultfs.New()
	h := &hookFS{FS: ff, ff: ff, serial: true, crashAt: crashAt, plo: plo}
	db, oerr := open(testCtx(t), h, testDir, compactOpts())
	if oerr != nil {
		return nil, fmt.Errorf("open: %w", oerr)
	}
	h.arm()
	lastAcked := 0
workload:
	for round := 1; round <= 3; round++ {
		for k := 1; k <= 6; k++ {
			i := (round-1)*6 + k
			if applySeq(db, i) != nil {
				break workload
			}
			lastAcked = i
		}
		// Wait for the compaction of this round.
		deadline := time.Now().Add(10 * time.Second)
		for !h.isDead() && (!settled(t, ff, uint64(6*round)) || db.compacting.Load()) {
			if time.Now().After(deadline) {
				return h.steps(), errors.New("timeout waiting for the compaction")
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	steps = h.steps()
	_ = db.Close(bg)
	h.cut(plo) // no-op if the power was already cut

	for range 2 {
		db2, err := open(testCtx(t), ff, testDir, Options{})
		if err != nil {
			return steps, fmt.Errorf("reopen: %w", err)
		}
		n, err := checkSequential(db2.Snapshot(bg), lastAcked)
		if err == nil {
			// The recovered log accepts new batches.
			err = applySeq(db2, n+1)
			lastAcked = n + 1
		}
		if cerr := db2.Close(bg); err == nil {
			err = cerr
		}
		if err != nil {
			return steps, err
		}
	}
	return steps, nil
}

// TestCompactPowerLossAtEachStep cuts the power before each file operation
// of three compactions in turn, in every power-loss mode, and checks AC2:
// the reopened database holds every acknowledged batch and is a prefix.
func TestCompactPowerLossAtEachStep(t *testing.T) {
	modes := []struct {
		name string
		plo  faultfs.PowerLossOptions
	}{
		{"drop", faultfs.PowerLossOptions{}},
		{"zerofill", faultfs.PowerLossOptions{ZeroFill: true}},
		{"keeppages", faultfs.PowerLossOptions{KeepPages: true, PageSize: 64}},
		{"keeppages+zerofill", faultfs.PowerLossOptions{KeepPages: true, ZeroFill: true, PageSize: 64}},
	}
	base := powerLossBase(t)
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			plo := m.plo
			plo.Rand = rand.New(rand.NewPCG(base, 7))
			steps, err := compactSteps(t, plo, 0)
			if err != nil {
				t.Fatalf("without a cut: %v", err)
			}
			t.Logf("%d file operations in the workload", len(steps))
			for step := 1; step <= len(steps); step++ {
				plo.Rand = rand.New(rand.NewPCG(base+uint64(step), 7))
				if _, err := compactSteps(t, plo, step); err != nil {
					t.Fatalf("power cut before step %d (%s), seed %d (ERMITE_KV_SEED=%d reproduces it): %v",
						step, steps[step-1], base, base, err)
				}
			}
		})
	}
}

// TestCompactPowerLoss cuts the power at a random time during a workload
// that compacts all the time, then checks AC2. With NoSync the acknowledged
// batches may be lost, but the directory must open and be a prefix; only the
// modes that cannot make Open fail with ErrCorrupt are used (see
// Options.NoSync).
func TestCompactPowerLoss(t *testing.T) {
	modes := []struct {
		name   string
		nosync bool
		plo    faultfs.PowerLossOptions
	}{
		{"drop", false, faultfs.PowerLossOptions{}},
		{"zerofill", false, faultfs.PowerLossOptions{ZeroFill: true}},
		{"keeppages", false, faultfs.PowerLossOptions{KeepPages: true}},
		{"keeppages+zerofill", false, faultfs.PowerLossOptions{KeepPages: true, ZeroFill: true}},
		{"nosync drop", true, faultfs.PowerLossOptions{}},
		{"nosync zerofill", true, faultfs.PowerLossOptions{ZeroFill: true}},
	}
	base := powerLossBase(t)
	iterations := short(100, 10)
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			for i := range iterations {
				seed := base + uint64(i)
				if err := runCompactPowerLoss(t, seed, m.plo, m.nosync); err != nil {
					t.Fatalf("seed %d (ERMITE_KV_SEED=%d reproduces it): %v", seed, seed, err)
				}
			}
		})
	}
}

func runCompactPowerLoss(t *testing.T, seed uint64, plo faultfs.PowerLossOptions, nosync bool) error {
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	ff := faultfs.New()
	h := &hookFS{FS: ff, ff: ff, serial: true}
	opts := Options{NoSync: nosync, CompactAfter: int64(2+rng.IntN(10)) * recSize}
	db, err := open(testCtx(t), h, testDir, opts)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	ff.SetSyncDelay(time.Duration(rng.IntN(100)) * time.Microsecond)
	var lastAcked atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; applySeq(db, i) == nil; i++ {
			lastAcked.Store(int64(i))
		}
	}()
	time.Sleep(time.Duration(rng.IntN(6000)) * time.Microsecond)
	plo.Rand = rand.New(rand.NewPCG(seed, 7))
	if plo.KeepPages {
		plo.PageSize = 64
	}
	h.cut(plo)
	<-done
	_ = db.Close(bg)
	ff.SetSyncDelay(0)

	floor := int(lastAcked.Load())
	if nosync {
		floor = 0
	}
	for range 2 {
		db, err = open(testCtx(t), ff, testDir, Options{})
		if err != nil {
			return fmt.Errorf("reopen: %w", err)
		}
		n, err := checkSequential(db.Snapshot(bg), floor)
		if err == nil {
			err = applySeq(db, n+1) // appends to the right log
			floor = n + 1
		}
		if cerr := db.Close(bg); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
}
