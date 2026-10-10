// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/zerowrap"

	"bnema.dev/ermite-kv/internal/fsys"
	"bnema.dev/ermite-kv/internal/fsys/faultfs"
)

var bg = context.Background()

// testCtx returns a context carrying a console logger when
// ERMITE_KV_LOG_LEVEL is set (the library itself never reads the
// environment), and context.Background() (logging disabled) otherwise.
func testCtx(t testing.TB) context.Context {
	level := os.Getenv("ERMITE_KV_LOG_LEVEL")
	if level == "" {
		return bg
	}
	return zerowrap.WithCtx(bg, zerowrap.New(zerowrap.Config{Level: level, Format: "console", Output: t.Output()}))
}

// mustOpen opens a database on fx and closes it when the test ends.
func mustOpen(t testing.TB, fx fsys.FS, opts Options) *DB {
	t.Helper()
	return mustOpenCtx(t, testCtx(t), fx, opts)
}

func mustOpenCtx(t testing.TB, ctx context.Context, fx fsys.FS, opts Options) *DB {
	t.Helper()
	db, err := open(ctx, fx, testDir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(bg) })
	return db
}

// putKV applies a single put and returns its seq.
func putKV(t testing.TB, db *DB, key, value string) uint64 {
	t.Helper()
	var b Batch
	b.Put([]byte(key), []byte(value))
	seq, err := db.Apply(bg, &b)
	if err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
	return seq
}

func mustGet(t testing.TB, s *Snapshot, key, want string) Item {
	t.Helper()
	it, ok := s.Get(bg, []byte(key))
	if !ok || string(it.Value) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q", key, it.Value, ok, want)
	}
	return it
}

func mustMissing(t testing.TB, s *Snapshot, key string) {
	t.Helper()
	if it, ok := s.Get(bg, []byte(key)); ok {
		t.Fatalf("Get(%q) = %q, want missing", key, it.Value)
	}
}

func short(full, quick int) int {
	if testing.Short() {
		return quick
	}
	return full
}

func TestReopenFaultfs(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{})
	if s := db.Snapshot(bg); s.Seq() != 0 || s.Len() != 0 {
		t.Fatalf("new db: seq %d len %d", s.Seq(), s.Len())
	}
	for i := range 50 {
		if seq := putKV(t, db, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i)); seq != uint64(i+1) {
			t.Fatalf("seq = %d, want %d", seq, i+1)
		}
	}
	var b Batch
	b.Delete([]byte("k00"))
	b.Put([]byte("k01"), []byte("again"))
	b.Put([]byte("k01"), []byte("last"))
	if seq, err := db.Apply(bg, &b); err != nil || seq != 51 {
		t.Fatalf("Apply = %d, %v", seq, err)
	}
	if err := db.Close(bg); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Close(bg); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	db = mustOpen(t, fx, Options{})
	s := db.Snapshot(bg)
	if s.Seq() != 51 || s.Len() != 49 {
		t.Fatalf("reopened: seq %d len %d, want 51 and 49", s.Seq(), s.Len())
	}
	mustMissing(t, s, "k00")
	if it := mustGet(t, s, "k01", "last"); it.Version != 51 {
		t.Fatalf("k01 version %d, want 51", it.Version)
	}
	if it := mustGet(t, s, "k02", "v2"); it.Version != 3 {
		t.Fatalf("k02 version %d, want 3", it.Version)
	}
	if seq := putKV(t, db, "next", "x"); seq != 52 {
		t.Fatalf("seq after reopen = %d, want 52", seq)
	}
}

func TestReopenOS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "db")
	db, err := Open(testCtx(t), dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		putKV(t, db, fmt.Sprintf("k%03d", i), strings.Repeat("v", i))
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("dir stat: %v, %v", fi, err)
	}

	db, err = Open(testCtx(t), dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(bg) }()
	s := db.Snapshot(bg)
	if s.Seq() != 100 || s.Len() != 100 {
		t.Fatalf("seq %d len %d, want 100", s.Seq(), s.Len())
	}
	for i := range 100 {
		mustGet(t, s, fmt.Sprintf("k%03d", i), strings.Repeat("v", i))
	}
	if got := db.Snapshot(bg); got != s {
		t.Fatal("Snapshot changed without a write")
	}
	if it, ok := db.Get(bg, []byte("k005")); !ok || it.Version != 6 {
		t.Fatalf("DB.Get = %v, %v", it, ok)
	}
}

// TestOpenFromSnapshot covers loading a snapshot file (written by the future
// compactor) and appending to the log created next to it.
func TestOpenFromSnapshot(t *testing.T) {
	var buf bytes.Buffer
	entries := func(yield func(k, v []byte, ver uint64) bool) {
		_ = yield([]byte("a"), []byte("1"), 3) && yield([]byte("b"), nil, 7) && yield([]byte("c"), []byte("3"), 9)
	}
	if err := writeSnapshot(&buf, 9, 3, entries); err != nil {
		t.Fatal(err)
	}
	fx := faultfs.New()
	fx.WriteFile(filepath.Join(testDir, snapshotName(9)), buf.Bytes())

	db := mustOpen(t, fx, Options{})
	s := db.Snapshot(bg)
	if s.Seq() != 9 || s.Len() != 3 {
		t.Fatalf("seq %d len %d", s.Seq(), s.Len())
	}
	if it := mustGet(t, s, "b", ""); it.Version != 7 {
		t.Fatalf("b = %+v", it)
	}
	if seq := putKV(t, db, "d", "4"); seq != 10 {
		t.Fatalf("seq = %d, want 10", seq)
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, fx, Options{})
	mustGet(t, db.Snapshot(bg), "d", "4")
	mustGet(t, db.Snapshot(bg), "c", "3")
	if db.Snapshot(bg).Seq() != 10 {
		t.Fatalf("seq = %d", db.Snapshot(bg).Seq())
	}
}

func TestConcurrentWriters(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{})
	const writers = 64
	per := short(50, 10)

	seqs := make([][]uint64, writers)
	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range per {
				key := fmt.Sprintf("w%02d/%03d", g, n)
				var b Batch
				b.Put([]byte(key), []byte(key))
				seq, err := db.Apply(bg, &b)
				if err != nil {
					t.Errorf("writer %d: %v", g, err)
					return
				}
				if it, ok := db.Get(bg, []byte(key)); !ok || it.Version < seq {
					t.Errorf("writer %d: no read-your-writes for %s at seq %d", g, key, seq)
					return
				}
				seqs[g] = append(seqs[g], seq)
			}
		}()
	}
	wg.Wait()

	var all []uint64
	for g, s := range seqs {
		if !slices.IsSorted(s) || len(slices.Compact(slices.Clone(s))) != len(s) {
			t.Fatalf("writer %d seqs not strictly increasing: %v", g, s)
		}
		all = append(all, s...)
	}
	slices.Sort(all)
	for i, s := range all {
		if s != uint64(i+1) {
			t.Fatalf("seq %d at rank %d: seqs are not 1..%d", s, i, len(all))
		}
	}
	if got := db.Snapshot(bg).Seq(); got != writers*uint64(per) {
		t.Fatalf("Seq = %d, want %d", got, writers*per)
	}
	if got := db.Snapshot(bg).Len(); got != writers*per {
		t.Fatalf("Len = %d, want %d", got, writers*per)
	}
}

func TestRequireAbsentRace(t *testing.T) {
	for _, window := range []time.Duration{0, 5 * time.Millisecond} {
		t.Run(window.String(), func(t *testing.T) {
			fx := faultfs.New()
			db := mustOpen(t, fx, Options{GroupWindow: window})
			const n = 64
			var wins atomic.Int32
			winner := make([]bool, n)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for g := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var b Batch
					b.RequireAbsent([]byte("lock"))
					b.Put([]byte("lock"), []byte(strconv.Itoa(g)))
					<-start
					seq, err := db.Apply(bg, &b)
					switch {
					case err == nil && seq > 0:
						wins.Add(1)
						winner[g] = true
					case errors.Is(err, ErrConflict) && seq == 0:
					default:
						t.Errorf("writer %d: seq %d err %v", g, seq, err)
					}
				}()
			}
			close(start)
			wg.Wait()
			if wins.Load() != 1 {
				t.Fatalf("%d winners, want 1", wins.Load())
			}
			w := slices.Index(winner, true)
			mustGet(t, db.Snapshot(bg), "lock", strconv.Itoa(w))
			if db.Snapshot(bg).Seq() != 1 {
				t.Fatalf("Seq = %d, want 1 (conflicts consume no seq)", db.Snapshot(bg).Seq())
			}
		})
	}
}

func TestRequireVersion(t *testing.T) {
	db := mustOpen(t, faultfs.New(), Options{})
	v1 := putKV(t, db, "k", "a")
	cas := func(key string, v uint64, val string) (uint64, error) {
		var b Batch
		b.RequireVersion([]byte(key), v)
		b.Put([]byte(key), []byte(val))
		return db.Apply(bg, &b)
	}
	v2, err := cas("k", v1, "b")
	if err != nil || v2 != v1+1 {
		t.Fatalf("cas at current version: %d, %v", v2, err)
	}
	if seq, err := cas("k", v1, "c"); !errors.Is(err, ErrConflict) || seq != 0 {
		t.Fatalf("cas at stale version: %d, %v", seq, err)
	}
	if seq, err := cas("missing", 1, "c"); !errors.Is(err, ErrConflict) || seq != 0 {
		t.Fatalf("cas on missing key: %d, %v", seq, err)
	}
	mustGet(t, db.Snapshot(bg), "k", "b")
	mustMissing(t, db.Snapshot(bg), "missing")
	if got := db.Snapshot(bg).Seq(); got != v2 {
		t.Fatalf("Seq = %d, want %d", got, v2)
	}
}

// TestPreconditionsBeforeOps checks that all preconditions are evaluated
// before any op of the same batch and that a conflict contributes nothing.
func TestPreconditionsBeforeOps(t *testing.T) {
	db := mustOpen(t, faultfs.New(), Options{})
	putKV(t, db, "exists", "1")

	var b Batch
	b.Put([]byte("new"), []byte("x"))
	b.RequireAbsent([]byte("new")) // holds: checked before the put
	if _, err := db.Apply(bg, &b); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	b.Reset()
	b.Delete([]byte("exists"))
	b.Put([]byte("other"), []byte("x"))
	b.RequireAbsent([]byte("never"))
	b.RequireAbsent([]byte("exists")) // fails
	before := db.Snapshot(bg)
	if seq, err := db.Apply(bg, &b); !errors.Is(err, ErrConflict) || seq != 0 {
		t.Fatalf("Apply = %d, %v; want ErrConflict", seq, err)
	}
	if db.Snapshot(bg) != before {
		t.Fatal("a conflicting batch published a snapshot")
	}
	mustGet(t, db.Snapshot(bg), "exists", "1")
	mustMissing(t, db.Snapshot(bg), "other")
}

// TestGroupPreconditions checks that preconditions see earlier batches of
// the same group: with a group window, the second batch must conflict.
func TestGroupPreconditions(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{GroupWindow: 200 * time.Millisecond})
	syncs := fx.Syncs()
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var b Batch
			b.RequireAbsent([]byte("k"))
			b.Put([]byte("k"), []byte{byte('a' + i)})
			_, errs[i] = db.Apply(bg, &b)
		}()
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("errors %v, want exactly one success", errs)
	}
	if fx.Syncs() != syncs+1 {
		t.Fatalf("%d syncs for the group, want 1", fx.Syncs()-syncs)
	}
}

func TestEmptyAndConflictOnlyBatches(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{})
	putKV(t, db, "a", "1")
	putKV(t, db, "b", "2")
	writes, syncs := fx.Writes(), fx.Syncs()
	snap := db.Snapshot(bg)

	var empty Batch
	if seq, err := db.Apply(bg, &empty); err != nil || seq != 2 {
		t.Fatalf("empty batch: %d, %v; want current seq 2", seq, err)
	}
	var pre Batch
	pre.RequireVersion([]byte("a"), 1)
	pre.RequireAbsent([]byte("zzz"))
	if seq, err := db.Apply(bg, &pre); err != nil || seq != 2 {
		t.Fatalf("holding precondition-only batch: %d, %v; want seq 2", seq, err)
	}
	var bad Batch
	bad.RequireAbsent([]byte("a"))
	if seq, err := db.Apply(bg, &bad); !errors.Is(err, ErrConflict) || seq != 0 {
		t.Fatalf("failing precondition-only batch: %d, %v", seq, err)
	}
	var conflict Batch
	conflict.RequireAbsent([]byte("a"))
	conflict.Put([]byte("c"), nil)
	if _, err := db.Apply(bg, &conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	var del Batch // deleting a missing key still writes a record
	del.Delete([]byte("nope"))
	if seq, err := db.Apply(bg, &del); err != nil || seq != 3 {
		t.Fatalf("delete of a missing key: %d, %v; want seq 3", seq, err)
	}

	// Only the last batch touched the log.
	if got := fx.Writes() - writes; got != 1 {
		t.Fatalf("%d writes, want 1", got)
	}
	if got := fx.Syncs() - syncs; got != 1 {
		t.Fatalf("%d syncs, want 1", got)
	}
	if db.Snapshot(bg).Len() != snap.Len() || db.Snapshot(bg).Seq() != 3 {
		t.Fatalf("snapshot after: seq %d len %d", db.Snapshot(bg).Seq(), db.Snapshot(bg).Len())
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, fx, Options{})
	if db.Snapshot(bg).Seq() != 3 || db.Snapshot(bg).Len() != 2 {
		t.Fatalf("reopened: seq %d len %d", db.Snapshot(bg).Seq(), db.Snapshot(bg).Len())
	}
}

// TestNoWriteForSkippedGroup checks that a group of only conflicts and empty
// batches neither writes nor syncs.
func TestNoWriteForSkippedGroup(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{GroupWindow: 100 * time.Millisecond})
	putKV(t, db, "a", "1")
	writes, syncs := fx.Writes(), fx.Syncs()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			var b Batch
			b.RequireAbsent([]byte("a"))
			b.Put([]byte("x"), nil)
			if _, err := db.Apply(bg, &b); !errors.Is(err, ErrConflict) {
				t.Errorf("err = %v, want ErrConflict", err)
			}
		})
	}
	wg.Wait()
	if fx.Writes() != writes || fx.Syncs() != syncs {
		t.Fatalf("writes %d→%d syncs %d→%d", writes, fx.Writes(), syncs, fx.Syncs())
	}
}

func TestGroupCommitSyncs(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{})
	fx.SetSyncDelay(time.Millisecond)
	base := fx.Syncs()
	const writers = 64
	per := short(100, 16)
	var wg sync.WaitGroup
	for g := range writers {
		wg.Go(func() {
			for n := range per {
				putKV(t, db, fmt.Sprintf("g%02d/%03d", g, n), "v")
			}
		})
	}
	wg.Wait()
	total := writers * per
	syncs := fx.Syncs() - base
	t.Logf("%d batches, %d syncs", total, syncs)
	if syncs >= total/4 {
		t.Fatalf("%d syncs for %d batches: no group commit", syncs, total)
	}
	if got := db.Snapshot(bg).Len(); got != total {
		t.Fatalf("Len = %d, want %d", got, total)
	}
}

func TestNoSync(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{NoSync: true})
	base := fx.Syncs()
	putKV(t, db, "a", "1")
	if fx.Syncs() != base {
		t.Fatal("NoSync still syncs")
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	mustGet(t, mustOpen(t, fx, Options{}).Snapshot(bg), "a", "1")
}

// TestLargeBatches covers a batch above the 4 MiB group cap (a group of one)
// and a burst of batches that does not fit in one group.
func TestLargeBatches(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{GroupWindow: 50 * time.Millisecond})
	big := strings.Repeat("B", 5<<20)
	putKV(t, db, "big", big)

	const n = 6
	chunk := strings.Repeat("c", 3<<19) // 1.5 MiB: three do not fit in 4 MiB
	seqs := make([]uint64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { seqs[i] = putKV(t, db, fmt.Sprintf("chunk%d", i), chunk) })
	}
	wg.Wait()
	slices.Sort(seqs)
	for i, s := range seqs {
		if s != uint64(i+2) {
			t.Fatalf("seqs = %v", seqs)
		}
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	s := mustOpen(t, fx, Options{}).Snapshot(bg)
	mustGet(t, s, "big", big)
	for i := range n {
		mustGet(t, s, fmt.Sprintf("chunk%d", i), chunk)
	}
}

func TestInvalidInputs(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{MaxBatchBytes: 100})

	var b Batch
	b.Put(nil, []byte("v"))
	if _, err := db.Apply(bg, &b); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("empty key: %v", err)
	}
	b.Reset()
	b.Put(bytes.Repeat([]byte("k"), maxKeyLen+1), nil)
	if _, err := db.Apply(bg, &b); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("long key: %v", err)
	}
	b.Reset()
	b.RequireVersion([]byte("k"), 0)
	if _, err := db.Apply(bg, &b); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("version 0: %v", err)
	}

	// Encoded batch = 4 (count) + 1 (kind) + 1 (klen) + 1 (key) + 1 (vlen) + v.
	b.Reset()
	b.Put([]byte("k"), make([]byte, 93))
	if seq, err := db.Apply(bg, &b); !errors.Is(err, ErrBatchTooLarge) || seq != 0 {
		t.Fatalf("101-byte batch: %d, %v", seq, err)
	}
	b.Reset()
	b.Put([]byte("k"), make([]byte, 92))
	if _, err := db.Apply(bg, &b); err != nil {
		t.Fatalf("100-byte batch: %v", err)
	}
	if db.Snapshot(bg).Seq() != 1 {
		t.Fatalf("Seq = %d: rejected batches reached the log", db.Snapshot(bg).Seq())
	}
}

func TestOptionValidation(t *testing.T) {
	bad := []Options{
		{MaxBatchBytes: maxRecord + 1},
		{MaxBatchBytes: maxRecord - 11},
		{MaxBatchBytes: -1},
		{GroupWindow: -time.Second},
	}
	for _, o := range bad {
		fx := faultfs.New()
		if db, err := open(testCtx(t), fx, testDir, o); err == nil {
			_ = db.Close(bg)
			t.Errorf("open(%+v) succeeded", o)
		}
		if _, err := fx.ReadDir(testDir); err == nil {
			t.Errorf("open(%+v) created the directory", o)
		}
	}
	if _, err := Open(testCtx(t), t.TempDir(), Options{MaxBatchBytes: 65 << 20}); err == nil {
		t.Error("Open with MaxBatchBytes > 64 MiB succeeded")
	}
	for _, o := range []Options{{MaxBatchBytes: maxRecord - 12}, {CompactAfter: -1}, {NoSync: true, CompactAfter: 1}} {
		db, err := open(testCtx(t), faultfs.New(), testDir, o)
		if err != nil {
			t.Errorf("open(%+v): %v", o, err)
			continue
		}
		_ = db.Close(bg)
	}
}

func TestLocked(t *testing.T) {
	t.Run("faultfs", func(t *testing.T) {
		fx := faultfs.New()
		db := mustOpen(t, fx, Options{})
		if _, err := open(testCtx(t), fx, testDir, Options{}); !errors.Is(err, ErrLocked) {
			t.Fatalf("second open: %v, want ErrLocked", err)
		}
		if err := db.Close(bg); err != nil {
			t.Fatal(err)
		}
		mustOpen(t, fx, Options{})
	})
	t.Run("os", func(t *testing.T) {
		dir := t.TempDir()
		db, err := Open(testCtx(t), dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Open(testCtx(t), dir, Options{}); !errors.Is(err, ErrLocked) {
			t.Fatalf("second Open: %v, want ErrLocked", err)
		}
		putKV(t, db, "still", "works")
		if err := db.Close(bg); err != nil {
			t.Fatal(err)
		}
		db, err = Open(testCtx(t), dir, Options{})
		if err != nil {
			t.Fatalf("Open after Close: %v", err)
		}
		_ = db.Close(bg)
	})
}

func TestApplyAfterClose(t *testing.T) {
	db := mustOpen(t, faultfs.New(), Options{})
	putKV(t, db, "a", "1")
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	var b Batch
	b.Put([]byte("b"), nil)
	if _, err := db.Apply(bg, &b); !errors.Is(err, ErrClosed) {
		t.Fatalf("Apply after Close: %v", err)
	}
	mustGet(t, db.Snapshot(bg), "a", "1") // reads keep working
	if err := db.Close(bg); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestContextBeforeDequeue blocks the writer in a slow sync so that a second
// batch cannot be dequeued, then cancels its context.
func TestContextBeforeDequeue(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{})

	done := make(chan error, 1)
	base := fx.Syncs()
	fx.SetSyncDelay(300 * time.Millisecond)
	go func() {
		var b Batch
		b.Put([]byte("first"), []byte("1"))
		_, err := db.Apply(bg, &b)
		done <- err
	}()
	for fx.Syncs() == base { // the writer is now inside the delayed sync
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(bg)
	second := make(chan error, 1)
	go func() {
		var b Batch
		b.Put([]byte("second"), []byte("2"))
		_, err := db.Apply(ctx, &b)
		second <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply = %v, want context.Canceled", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	fx.SetSyncDelay(0)

	// An already cancelled context never reaches the writer either.
	var b Batch
	b.Put([]byte("third"), nil)
	if _, err := db.Apply(ctx, &b); !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply with cancelled ctx = %v", err)
	}
	putKV(t, db, "fourth", "4") // the writer is still healthy
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	s := mustOpen(t, fx, Options{}).Snapshot(bg)
	mustGet(t, s, "first", "1")
	mustGet(t, s, "fourth", "4")
	mustMissing(t, s, "second")
	mustMissing(t, s, "third")
	if s.Seq() != 2 {
		t.Fatalf("Seq = %d, want 2", s.Seq())
	}
}

func TestCloseDuringApplies(t *testing.T) {
	for i := range short(10, 3) {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			fx := faultfs.New()
			fx.SetSyncDelay(100 * time.Microsecond)
			db := mustOpen(t, fx, Options{})
			const writers = 32
			acked := make([]uint64, writers) // highest acknowledged n+1
			var wg sync.WaitGroup
			for g := range writers {
				wg.Go(func() {
					for n := 0; ; n++ {
						var b Batch
						b.Put(fmt.Appendf(nil, "g%02d", g), strconv.AppendInt(nil, int64(n), 10))
						_, err := db.Apply(bg, &b)
						if errors.Is(err, ErrClosed) {
							return
						}
						if err != nil {
							t.Errorf("writer %d: %v", g, err)
							return
						}
						acked[g] = uint64(n) + 1
					}
				})
			}
			time.Sleep(time.Duration(2+i) * time.Millisecond)
			closed := make(chan error, 1)
			go func() { closed <- db.Close(bg) }()
			select {
			case err := <-closed:
				if err != nil {
					t.Errorf("Close: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not return")
			}
			wg.Wait()

			fx.SetSyncDelay(0)
			s := mustOpen(t, fx, Options{}).Snapshot(bg)
			for g, n := range acked {
				if n == 0 {
					continue
				}
				it, ok := s.Get(bg, fmt.Appendf(nil, "g%02d", g))
				got, _ := strconv.Atoi(string(it.Value))
				// The batch after the last acknowledged one may have been
				// committed by the final group.
				if !ok || uint64(got)+1 < n || uint64(got) > n {
					t.Errorf("writer %d: acked %d batches, recovered n=%d (%v)", g, n, got, ok)
				}
			}
		})
	}
}

// failure is one injected I/O fault and what a plain reopen (no power loss)
// must find of the failed group.
type failure struct {
	name   string
	inject func(fx *faultfs.FS)
	kept   bool // the group's bytes reach the file even though the call failed
	cause  error
}

func TestInjectedFailures(t *testing.T) {
	failures := []failure{
		{"write", func(fx *faultfs.FS) { fx.FailWrite(1, nil) }, false, faultfs.ErrInjected},
		{"short write", func(fx *faultfs.FS) { fx.ShortWrite(1, 10) }, false, nil},
		{"sync", func(fx *faultfs.FS) { fx.FailSync(1, nil) }, true, faultfs.ErrInjected},
	}
	for _, f := range failures {
		for _, powerLoss := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/powerloss=%v", f.name, powerLoss), func(t *testing.T) {
				testInjectedFailure(t, f, powerLoss)
			})
		}
	}
}

func testInjectedFailure(t *testing.T, f failure, powerLoss bool) {
	fx := faultfs.New()
	var logs syncBuffer
	ctx := zerowrap.WithCtx(bg, zerowrap.New(zerowrap.Config{Level: "debug", Format: "json", Output: &logs}))
	db := mustOpenCtx(t, ctx, fx, Options{GroupWindow: 150 * time.Millisecond})
	putKV(t, db, "before", "1")
	f.inject(fx)

	// Three batches form the failed group (the window gathers them).
	const group = 3
	errs := make([]error, group)
	var wg sync.WaitGroup
	for i := range group {
		wg.Go(func() {
			var b Batch
			b.Put([]byte(fmt.Sprintf("failed%d", i)), []byte("x"))
			_, errs[i] = db.Apply(bg, &b)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, ErrFailed) {
			t.Fatalf("batch %d: %v, want ErrFailed", i, err)
		}
		if f.cause != nil && !errors.Is(err, f.cause) {
			t.Fatalf("batch %d: %v does not wrap %v", i, err, f.cause)
		}
	}
	if !strings.Contains(logs.String(), `"level":"error"`) || !strings.Contains(logs.String(), "database failed") {
		t.Errorf("failure not logged: %q", logs.String())
	}

	// Every later Apply fails at once; reads serve the last snapshot.
	for range 3 {
		var b Batch
		b.Put([]byte("later"), nil)
		if _, err := db.Apply(bg, &b); !errors.Is(err, ErrFailed) {
			t.Fatalf("later Apply: %v, want ErrFailed", err)
		}
	}
	var empty Batch
	if _, err := db.Apply(bg, &empty); !errors.Is(err, ErrFailed) {
		t.Fatalf("empty Apply on a failed db: %v", err)
	}
	s := db.Snapshot(bg)
	if s.Seq() != 1 || s.Len() != 1 {
		t.Fatalf("snapshot after failure: seq %d len %d", s.Seq(), s.Len())
	}
	mustGet(t, s, "before", "1")
	mustMissing(t, s, "failed0")
	if err := db.Close(bg); !errors.Is(err, ErrFailed) {
		t.Fatalf("Close = %v, want the failure", err)
	}
	if err := db.Close(bg); err != nil {
		t.Fatalf("second Close = %v", err)
	}

	if powerLoss {
		fx.PowerLoss(faultfs.PowerLossOptions{})
	}
	s = mustOpen(t, fx, Options{}).Snapshot(bg)
	mustGet(t, s, "before", "1")
	present := 0
	for i := range group {
		if _, ok := s.Get(bg, fmt.Appendf(nil, "failed%d", i)); ok {
			present++
		}
	}
	want := 0
	if f.kept && !powerLoss {
		want = group
	}
	if present != want {
		t.Fatalf("%d of %d failed batches recovered, want %d", present, group, want)
	}
	if s.Seq() != uint64(1+present) {
		t.Fatalf("Seq = %d, want %d", s.Seq(), 1+present)
	}
}

// TestFailedWriterAnswersOverflow checks that no Apply hangs when the group
// that fails leaves a dequeued request behind.
func TestFailedWriterAnswersOverflow(t *testing.T) {
	fx := faultfs.New()
	db := mustOpen(t, fx, Options{GroupWindow: 100 * time.Millisecond})
	fx.FailWrite(1, nil)
	chunk := strings.Repeat("c", 3<<20) // two do not fit in one group
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() {
			var b Batch
			b.Put(fmt.Appendf(nil, "k%d", i), []byte(chunk))
			_, errs[i] = db.Apply(bg, &b)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, ErrFailed) {
			t.Errorf("batch %d: %v", i, err)
		}
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// powerLossBase returns the first seed; ERMITE_KV_SEED reproduces a run.
func powerLossBase(t *testing.T) uint64 {
	if v := os.Getenv("ERMITE_KV_SEED"); v != "" {
		seed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("ERMITE_KV_SEED: %v", err)
		}
		return seed
	}
	return uint64(time.Now().UnixNano())
}

// TestPowerLoss cuts the power at a random point while writers run, then
// checks AC2: every acknowledged batch is present and the recovered state is
// exactly a prefix of the applied batches.
//
// Every writer g applies batches n = 0, 1, … that set cur/g = n and
// all/g/n, and delete all/g/(n-1). The recovered state therefore says how
// many batches of each writer were applied, and the seq must equal their sum.
// The modes include KeepPages (a random subset of the unsynced pages
// survives). They always run with NoSync=false: with NoSync, KeepPages can
// legitimately make Open fail with ErrCorrupt, which is documented on
// Options.NoSync and not tested.
func TestPowerLoss(t *testing.T) {
	modes := []struct {
		name string
		opts faultfs.PowerLossOptions
	}{
		{"drop", faultfs.PowerLossOptions{}},
		{"zerofill", faultfs.PowerLossOptions{ZeroFill: true}},
		// Pages of the unsynced tail reach the disk in any order. One record
		// per group makes this safe when commits are synced.
		{"keeppages", faultfs.PowerLossOptions{KeepPages: true}},
		{"keeppages+zerofill", faultfs.PowerLossOptions{KeepPages: true, ZeroFill: true}},
	}
	base := powerLossBase(t)
	iterations := short(150, 10)
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			for i := range iterations {
				seed := base + uint64(i)
				if !runPowerLoss(t, seed, m.opts) {
					t.Fatalf("failed with seed %d (ERMITE_KV_SEED=%d reproduces it)", seed, seed)
				}
			}
		})
	}
}

func runPowerLoss(t *testing.T, seed uint64, plo faultfs.PowerLossOptions) (ok bool) {
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	fx := faultfs.New()
	db, err := open(testCtx(t), fx, testDir, Options{GroupWindow: time.Duration(rng.IntN(3)) * 100 * time.Microsecond})
	if err != nil {
		t.Errorf("seed %d: open: %v", seed, err)
		return false
	}
	fx.SetSyncDelay(time.Duration(rng.IntN(300)) * time.Microsecond)
	writers := 1 + rng.IntN(8)
	acked := make([]int, writers) // last acknowledged n, -1 if none
	ackSeq := make([]uint64, writers)
	var wg sync.WaitGroup
	for g := range writers {
		acked[g] = -1
		wg.Go(func() {
			for n := 0; ; n++ {
				var b Batch
				b.Put(fmt.Appendf(nil, "cur/%d", g), strconv.AppendInt(nil, int64(n), 10))
				b.Put(fmt.Appendf(nil, "all/%d/%d", g, n), nil)
				if n > 0 {
					b.Delete(fmt.Appendf(nil, "all/%d/%d", g, n-1))
				}
				seq, err := db.Apply(bg, &b)
				if err != nil {
					return
				}
				acked[g], ackSeq[g] = n, seq
			}
		})
	}
	time.Sleep(time.Duration(rng.IntN(4000)) * time.Microsecond)
	plo.Rand = rand.New(rand.NewPCG(seed, 7))
	if plo.KeepPages {
		plo.PageSize = 64 // small pages: a group record spans several of them
	}
	fx.PowerLoss(plo)
	wg.Wait()
	_ = db.Close(bg)
	fx.SetSyncDelay(0)

	db, err = open(testCtx(t), fx, testDir, Options{})
	if err != nil {
		t.Errorf("seed %d: reopen after power loss: %v", seed, err)
		return false
	}
	defer func() { _ = db.Close(bg) }()
	if !checkPowerLossState(t, seed, db.Snapshot(bg), acked, ackSeq) {
		return false
	}

	// The recovered log accepts new records and survives another reopen.
	seq := db.Snapshot(bg).Seq()
	if got := putKV(t, db, "after", "x"); got != seq+1 {
		t.Errorf("seed %d: seq after recovery = %d, want %d", seed, got, seq+1)
		return false
	}
	if err := db.Close(bg); err != nil {
		t.Errorf("seed %d: Close: %v", seed, err)
		return false
	}
	db, err = open(testCtx(t), fx, testDir, Options{})
	if err != nil {
		t.Errorf("seed %d: second reopen: %v", seed, err)
		return false
	}
	defer func() { _ = db.Close(bg) }()
	if db.Snapshot(bg).Seq() != seq+1 {
		t.Errorf("seed %d: seq after second reopen = %d, want %d", seed, db.Snapshot(bg).Seq(), seq+1)
		return false
	}
	return true
}

func checkPowerLossState(t *testing.T, seed uint64, s *Snapshot, acked []int, ackSeq []uint64) bool {
	fail := func(format string, args ...any) bool {
		t.Errorf("seed %d: "+format, append([]any{seed}, args...)...)
		return false
	}
	var applied uint64
	keys := 0
	for g := range acked {
		it, ok := s.Get(bg, fmt.Appendf(nil, "cur/%d", g))
		last := -1
		if ok {
			n, err := strconv.Atoi(string(it.Value))
			if err != nil {
				return fail("writer %d: bad cur value %q", g, it.Value)
			}
			last = n
		}
		if last < acked[g] {
			return fail("writer %d: acknowledged batch %d lost, recovered %d", g, acked[g], last)
		}
		if last == acked[g] && last >= 0 && it.Version != ackSeq[g] {
			return fail("writer %d: batch %d acknowledged at seq %d, recovered at %d", g, last, ackSeq[g], it.Version)
		}
		if !ok {
			continue
		}
		cur, ok := s.Get(bg, fmt.Appendf(nil, "all/%d/%d", g, last))
		if !ok || cur.Version != it.Version {
			return fail("writer %d: batch %d is partially applied (%v, %v)", g, last, cur, ok)
		}
		count := 0
		for range s.Prefix(bg, fmt.Appendf(nil, "all/%d/", g)) {
			count++
		}
		if count != 1 {
			return fail("writer %d: %d all/ keys, want 1", g, count)
		}
		applied += uint64(last) + 1
		keys += 2
	}
	if s.Seq() != applied {
		return fail("Seq = %d but %d batches are visible: not a prefix", s.Seq(), applied)
	}
	if s.Len() != keys {
		return fail("Len = %d, want %d", s.Len(), keys)
	}
	return true
}

// countFS counts the fdatasync calls of the files it opens.
type countFS struct {
	fsys.FS
	syncs atomic.Int64
}

func (c *countFS) OpenFile(name string, flag int, perm os.FileMode) (fsys.File, error) {
	f, err := c.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return countFile{f, &c.syncs}, nil
}

type countFile struct {
	fsys.File
	n *atomic.Int64
}

func (f countFile) Sync() error {
	f.n.Add(1)
	return f.File.Sync()
}

// BenchmarkApply measures group-commit throughput with parallel writers on a
// real directory (NoSync=false). The syncs are counted by a thin wrapper
// around the OS file system, so the numbers are real fdatasync calls.
func BenchmarkApply(b *testing.B) {
	fx := &countFS{FS: fsys.OS()}
	db, err := open(bg, fx, filepath.Join(b.TempDir(), "db"), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close(bg) }()
	b.SetParallelism(16)
	value := bytes.Repeat([]byte("v"), 100)
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var batch Batch
		for pb.Next() {
			batch.Reset()
			batch.Put(fmt.Appendf(nil, "key-%d", next.Add(1)), value)
			if _, err := db.Apply(bg, &batch); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	secs := b.Elapsed().Seconds()
	syncs := float64(fx.syncs.Load())
	b.ReportMetric(float64(b.N)/secs, "batches/s")
	b.ReportMetric(syncs/secs, "syncs/s")
	b.ReportMetric(float64(b.N)/syncs, "batches/sync")
}
