// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"bnema.dev/zerowrap"

	"bnema.dev/ermite-kv/internal/fsys"
	"bnema.dev/ermite-kv/internal/fsys/faultfs"
)

// coldCount returns the number of items of s served from a snapshot mapping.
func coldCount(s *Snapshot) int {
	n := 0
	for _, it := range s.t.ascend(nil, nil) {
		if it.m != nil {
			n++
		}
	}
	return n
}

// coldDir returns a directory holding a snapshot at seq 2 with a 2 KiB value
// under "big" and a 100 B value under "small".
func coldDir(t *testing.T) (dir string, big, small []byte) {
	t.Helper()
	dir = t.TempDir()
	big = bytes.Repeat([]byte("B"), 2048)
	small = bytes.Repeat([]byte("s"), 100)
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, 2, 2, func(yield func(k, v []byte, ver uint64) bool) {
		_ = yield([]byte("big"), big, 1) && yield([]byte("small"), small, 2)
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotName(2)), buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, big, small
}

func openCold(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := open(testCtx(t), fsys.OS(), dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(bg) })
	return db
}

func checkCold(t *testing.T, s *Snapshot, key string, want []byte, mapped bool) {
	t.Helper()
	it, ok := s.Get(bg, []byte(key))
	if !ok || !bytes.Equal(it.Value, want) {
		t.Fatalf("%s: wrong value (ok %v, len %d)", key, ok, len(it.Value))
	}
	if (it.m != nil) != mapped {
		t.Fatalf("%s: mapped = %v, want %v", key, it.m != nil, mapped)
	}
}

func TestColdValuesOnOpen(t *testing.T) {
	for _, tc := range []struct {
		name              string
		cold              int
		bigMapped, smallM bool
	}{
		{"default", 0, true, false},
		{"disabled", -1, false, false},
		{"all", 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, big, small := coldDir(t)
			opts := Options{ColdValueBytes: tc.cold}
			db := openCold(t, dir, opts)
			s := db.Snapshot(bg)
			checkCold(t, s, "big", big, tc.bigMapped)
			checkCold(t, s, "small", small, tc.smallM)
			want := 0
			for _, m := range []bool{tc.bigMapped, tc.smallM} {
				if m {
					want++
				}
			}
			if n := coldCount(s); n != want {
				t.Fatalf("coldCount = %d, want %d", n, want)
			}
			if err := db.Close(bg); err != nil {
				t.Fatal(err)
			}

			db = openCold(t, dir, opts)
			s = db.Snapshot(bg)
			checkCold(t, s, "big", big, tc.bigMapped)
			checkCold(t, s, "small", small, tc.smallM)
		})
	}
}

// closeBounded closes db and fails if Close does not return in time.
func closeBounded(t *testing.T, db *DB) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- db.Close(bg) }()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-timer.C:
		t.Fatal("Close hangs")
	}
}

func TestColdWorkFromCompaction(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: 1024})
	big1 := bytes.Repeat([]byte("1"), 1024)
	big2 := bytes.Repeat([]byte("2"), 3000)
	var b Batch
	b.Put([]byte("a"), big1)
	b.Put([]byte("b"), []byte("small"))
	b.Put([]byte("c"), big2)
	seq, err := db.Apply(bg, &b)
	if err != nil {
		t.Fatal(err)
	}
	waitColdJobs(t, db, 1)
	s := db.Snapshot(bg)
	if s.Seq() != seq {
		t.Fatalf("seq = %d, want %d: a swap must not change it", s.Seq(), seq)
	}
	checkCold(t, s, "a", big1, true)
	checkCold(t, s, "b", []byte("small"), false)
	checkCold(t, s, "c", big2, true)
	for _, k := range []string{"a", "c"} {
		it, _ := s.Get(bg, []byte(k))
		if it.Version != seq {
			t.Fatalf("%s: version %d, want %d", k, it.Version, seq)
		}
	}
}

func TestColdWorkDisabled(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: -1})
	for i := range 20 {
		mustApply(t, db, i+1)
	}
	closeBounded(t, db) // waits for the compactors
	if n := len(db.coldCh) + int(db.coldJobs.Load()) + coldCount(db.Snapshot(bg)); n != 0 {
		t.Fatalf("cold work seen with ColdValueBytes -1 (%d)", n)
	}
}

// waitColdJobs waits until db has completed n cold jobs.
func waitColdJobs(t *testing.T, db *DB, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for db.coldJobs.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("cold jobs done = %d, want %d", db.coldJobs.Load(), n)
		}
		runtime.Gosched()
	}
}

func TestColdSwapSkipsStaleCandidates(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: -1})
	m := fakeMapping("OLD-A OLD-B CUR-C")
	va, vb := putKV(t, db, "a", "old-a"), putKV(t, db, "b", "old-b")
	vc := putKV(t, db, "c", "cur-c")
	putKV(t, db, "a", "new-a") // rewritten after the snapshot
	var b Batch
	b.Delete([]byte("b")) // deleted after the snapshot
	if _, err := db.Apply(bg, &b); err != nil {
		t.Fatal(err)
	}
	seq := db.Snapshot(bg).Seq()
	db.coldCh <- &coldWork{m: m, cands: []coldCand{
		{key: []byte("a"), version: va, value: m.data[0:5]},
		{key: []byte("b"), version: vb, value: m.data[6:11]},
		{key: []byte("c"), version: vc, value: m.data[12:17]},
	}}
	waitColdJobs(t, db, 1)
	s := db.Snapshot(bg)
	checkCold(t, s, "a", []byte("new-a"), false)
	mustMissing(t, s, "b")
	checkCold(t, s, "c", []byte("CUR-C"), true)
	if s.Seq() != seq {
		t.Fatalf("seq = %d, want %d", s.Seq(), seq)
	}
}

func TestColdSwapInChunks(t *testing.T) {
	const n = 2500
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: 1024})
	val := func(i int) []byte { return bytes.Repeat(fmt.Appendf(nil, "%04d", i), 256) }
	var b Batch
	for i := range n {
		b.Put(treeKey(i), val(i))
	}
	if _, err := db.Apply(bg, &b); err != nil {
		t.Fatal(err)
	}
	putKV(t, db, "during", "swap") // a write while the cold job may be pending
	waitColdJobs(t, db, 1)
	if c := db.coldChunks.Load(); c < 3 {
		t.Fatalf("cold chunks = %d, want >= 3", c)
	}
	s := db.Snapshot(bg)
	if got := coldCount(s); got != n {
		t.Fatalf("coldCount = %d, want %d", got, n)
	}
	for i := range n {
		checkCold(t, s, string(treeKey(i)), val(i), true)
	}
	mustGet(t, s, "during", "swap")
}

// watchRelease returns a flag set once m is released by the GC. The caller
// must not keep m.
func watchRelease(m *mapping) *atomic.Bool {
	f := new(atomic.Bool)
	runtime.AddCleanup(m, func(f *atomic.Bool) { f.Store(true) }, f)
	return f
}

// watchItemMapping watches the mapping of the item under key without keeping
// it.
//
//go:noinline
func watchItemMapping(t *testing.T, db *DB, key string) *atomic.Bool {
	t.Helper()
	it, _ := db.Snapshot(bg).Get(bg, []byte(key))
	if it.m == nil {
		t.Fatalf("%s is not mapped", key)
	}
	return watchRelease(it.m)
}

// waitReleased runs the GC until released is set.
func waitReleased(t *testing.T, released *atomic.Bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !released.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("%s not released", what)
		}
		runtime.GC()
		runtime.Gosched()
	}
}

// gcSeveral runs the GC a few times.
func gcSeveral() {
	for range 5 {
		runtime.GC()
		runtime.Gosched()
	}
}

// compactUntil writes until db has completed n cold jobs. "big" is rewritten
// with the same value so that every compaction has a cold candidate.
func compactUntil(t *testing.T, db *DB, n int64, big []byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for db.coldJobs.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("cold jobs done = %d, want %d", db.coldJobs.Load(), n)
		}
		var b Batch
		b.Put([]byte("big"), big)
		if _, err := db.Apply(bg, &b); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
}

func TestLifetimeSnapshotAcrossSwaps(t *testing.T) {
	dir, big, small := coldDir(t)
	db := openCold(t, dir, Options{CompactAfter: 1})
	old := db.Snapshot(bg)
	checkCold(t, old, "big", big, true)
	compactUntil(t, db, 1, big)
	runtime.GC()
	checkCold(t, old, "big", big, true)
	checkCold(t, old, "small", small, false)
	compactUntil(t, db, 2, big)
	runtime.GC()
	checkCold(t, old, "big", big, true)
}

// bareValue returns the value of key without keeping the Item or the Snapshot.
//
//go:noinline
func bareValue(db *DB, key string) []byte {
	it, _ := db.Snapshot(bg).Get(bg, []byte(key))
	return it.Value
}

func TestLifetimeBareValuePinnedForOneSwap(t *testing.T) {
	dir, big, _ := coldDir(t)
	// Compaction is off: the test drives each swap itself.
	db := openCold(t, dir, Options{CompactAfter: -1})
	gen0 := watchItemMapping(t, db, "big")
	v := bareValue(db, "big")
	swap := func(n int64) *mapping {
		m := &mapping{data: slices.Clone(big)}
		db.coldCh <- &coldWork{m: m, cands: []coldCand{{key: []byte("big"), version: 1, value: m.data}}}
		waitColdJobs(t, db, n)
		return m
	}

	m1 := swap(1) // generation 0 is no longer referenced by the tree, only pinned
	if it, _ := db.Snapshot(bg).Get(bg, []byte("big")); it.m != m1 {
		t.Fatal("big not repointed by the swap")
	}
	gcSeveral()
	if gen0.Load() {
		t.Fatal("generation 0 released after one swap")
	}
	if !bytes.Equal(v, big) {
		t.Fatal("bare value changed")
	}

	swap(2) // releases generation 0
	waitReleased(t, gen0, "generation 0 after two swaps")
}

func TestPinsBookkeeping(t *testing.T) {
	g0, a, b, c := fakeMapping("0"), fakeMapping("a"), fakeMapping("b"), fakeMapping("c")
	p := pins{gen: g0}
	p.superseded(a) // job a published a chunk, then was replaced by b
	p.swapped(b)
	if p.gen != b || !slices.Equal(p.pinned, []*mapping{g0, a}) || p.partial != nil {
		t.Fatalf("after swap b: %+v", p)
	}
	p.swapped(c)
	if p.gen != c || !slices.Equal(p.pinned, []*mapping{b}) {
		t.Fatalf("after swap c: %+v", p)
	}
	q := pins{} // Open without a mapped value
	q.swapped(a)
	if q.gen != a || len(q.pinned) != 0 {
		t.Fatalf("first swap: %+v", q)
	}
}

// TestColdSwapUnderLoad checks that cold work arriving while requests are
// queued completes under that load. In a synctest bubble, writers batches
// bigger than half of maxGroupBytes all wait on the writer, which is parked
// in a slow sync: every group overflows, so a request is always pending and
// the writer never reaches its idle path until the queue is empty. The job
// must be taken and applied (after coldEvery groups) while the queue drains.
func TestColdSwapUnderLoad(t *testing.T) {
	const writers = coldEvery + 8
	synctest.Test(t, func(t *testing.T) {
		fx := faultfs.New()
		db := mustOpen(t, fx, Options{CompactAfter: -1})
		fx.SetSyncDelay(time.Millisecond)
		big := make([]byte, maxGroupBytes/2+1)
		var replies atomic.Int64
		var swappedAt atomic.Int64 // number of replies when the swap was first seen
		var wg sync.WaitGroup
		for g := range writers {
			wg.Go(func() {
				var b Batch
				b.Put(fmt.Appendf(nil, "w%d", g), big)
				if _, err := db.Apply(bg, &b); err != nil {
					t.Errorf("apply: %v", err)
				}
				n := replies.Add(1)
				if db.coldJobs.Load() > 0 {
					swappedAt.CompareAndSwap(0, n)
				}
			})
		}
		synctest.Wait() // the writer sleeps in a sync; the others are queued
		m := fakeMapping("v") // no cleanup inside the bubble
		db.coldCh <- &coldWork{m: m, cands: []coldCand{{key: []byte("w0"), version: 1, value: m.data}}}
		wg.Wait()
		if j := db.coldJobs.Load(); j != 1 {
			t.Fatalf("cold jobs = %d, want 1", j)
		}
		if at := swappedAt.Load(); at == 0 || at > writers-4 {
			t.Fatalf("swap first seen after reply %d of %d: not applied under load", at, writers)
		}
	})
}

// TestColdPinsClosedDB checks that Close keeps the pins: a bare value of the
// previous generation stays valid.
func TestColdPinsClosedDB(t *testing.T) {
	dir, big, _ := coldDir(t)
	db := openCold(t, dir, Options{CompactAfter: -1})
	gen0 := watchItemMapping(t, db, "big")
	v := bareValue(db, "big")
	m := &mapping{data: slices.Clone(big)}
	db.coldCh <- &coldWork{m: m, cands: []coldCand{{key: []byte("big"), version: 1, value: m.data}}}
	waitColdJobs(t, db, 1)
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	gcSeveral()
	if gen0.Load() {
		t.Fatal("generation 0 released after Close")
	}
	if !bytes.Equal(v, big) {
		t.Fatal("bare value changed")
	}
	runtime.KeepAlive(db)
}

// TestColdEmptyJobAdvancesPins checks that a compaction without large values
// still releases the generations before it.
func TestColdEmptyJobAdvancesPins(t *testing.T) {
	dir, _, _ := coldDir(t)
	db := openCold(t, dir, Options{CompactAfter: -1})
	gen0 := watchItemMapping(t, db, "big")
	putKV(t, db, "big", "small now") // no later snapshot has a cold value
	db.coldCh <- &coldWork{}         // what a compaction without candidates sends
	waitColdJobs(t, db, 1)           // gen0 pinned, no current generation
	db.coldCh <- &coldWork{}
	waitColdJobs(t, db, 2) // releases gen0
	waitReleased(t, gen0, "generation 0 after two empty swaps")
}

// TestColdWorkWithoutCandidates checks that compaction sends an empty job
// when the new snapshot has no large value.
func TestColdWorkWithoutCandidates(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1})
	putKV(t, db, "a", "small")
	waitColdJobs(t, db, 1)
	if n := coldCount(db.Snapshot(bg)); n != 0 {
		t.Fatalf("coldCount = %d", n)
	}
}

// fillKeys puts n keys treeKey(0..n-1) in one batch and returns its seq.
func fillKeys(t *testing.T, db *DB, n int) uint64 {
	t.Helper()
	var b Batch
	for i := range n {
		b.Put(treeKey(i), []byte("v"))
	}
	seq, err := db.Apply(bg, &b)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// jobFor returns a cold job repointing keys treeKey(0..n-1) at version to a
// new mapping, and a flag set once that mapping is released.
func jobFor(n int, version uint64) (*coldWork, *atomic.Bool) {
	m := &mapping{data: []byte("v")}
	cands := make([]coldCand, n)
	for i := range cands {
		cands[i] = coldCand{key: treeKey(i), version: version, value: m.data}
	}
	return &coldWork{m: m, cands: cands}, watchRelease(m)
}

// sendJob sends a job from jobFor without keeping it.
func sendJob(db *DB, n int, version uint64) *atomic.Bool {
	w, released := jobFor(n, version)
	db.coldCh <- w
	return released
}

// waitColdChunks waits until db has applied n cold chunks.
func waitColdChunks(t *testing.T, db *DB, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for db.coldChunks.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("cold chunks = %d, want %d", db.coldChunks.Load(), n)
		}
		runtime.Gosched()
	}
}

func TestCloseWithPendingColdWork(t *testing.T) {
	const n = 200 * coldChunk
	db := openCold(t, t.TempDir(), Options{CompactAfter: -1})
	seq := fillKeys(t, db, n)
	sendJob(db, n, seq)
	waitColdChunks(t, db, 1) // the job publishes: it is in progress
	if j := db.coldJobs.Load(); j != 0 {
		t.Fatalf("cold jobs = %d: the job finished before Close", j)
	}
	closeBounded(t, db)
	if j := db.coldJobs.Load(); j != 0 {
		t.Fatalf("cold jobs = %d: the job finished before Close", j)
	}
}

func TestColdMapFailureKeepsCompacting(t *testing.T) {
	fx := faultfs.New()
	var logs syncBuffer
	ctx := zerowrap.WithCtx(bg, zerowrap.New(zerowrap.Config{Level: "debug", Format: "json", Output: &logs}))
	fx.FailOp(faultfs.OpMap, 1, nil) // the map of the first new snapshot
	db := mustOpenCtx(t, ctx, fx, Options{CompactAfter: 1, ColdValueBytes: 1})
	seq := putKV(t, db, "a", "value")
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "compaction done") {
		if time.Now().After(deadline) {
			t.Fatalf("no compaction; logs:\n%s", logs.String())
		}
		runtime.Gosched()
	}
	if !strings.Contains(logs.String(), "cannot map new snapshot") {
		t.Fatalf("no map warning; logs:\n%s", logs.String())
	}
	if _, ok := fx.ReadFile(filepath.Join(testDir, snapshotName(seq))); !ok {
		t.Fatal("snapshot not written")
	}
	if j := db.coldJobs.Load(); j != 0 {
		t.Fatalf("cold jobs = %d after a failed map", j)
	}
	putKV(t, db, "b", "still works")
	mustGet(t, db.Snapshot(bg), "a", "value")
}

func TestColdSupersededPartialJobStaysPinned(t *testing.T) {
	const n = 200 * coldChunk
	db := openCold(t, t.TempDir(), Options{CompactAfter: -1})
	seq := fillKeys(t, db, n)
	first := sendJob(db, n, seq)
	waitColdChunks(t, db, 1) // the first job published a chunk
	second := sendJob(db, n, seq)
	if j := db.coldJobs.Load(); j != 0 {
		t.Fatalf("cold jobs = %d: the first job completed before being superseded", j)
	}
	waitColdJobs(t, db, 1) // the second job replaced it and completed
	if j := db.coldJobs.Load(); j != 1 {
		t.Fatalf("cold jobs = %d: the first job completed before being superseded", j)
	}
	// The tree no longer references the first mapping: only the pins do.
	gcSeveral()
	if first.Load() {
		t.Fatal("superseded partial mapping released before the next swap")
	}
	db.coldCh <- &coldWork{} // the next swap
	waitColdJobs(t, db, 2)
	waitReleased(t, first, "superseded partial mapping after the next swap")
	if second.Load() {
		t.Fatal("pinned generation released too early")
	}
}

// heapAfterOpen returns the heap growth caused by opening dir with cold, the
// DB still open.
func heapAfterOpen(t *testing.T, dir string, cold int) int64 {
	t.Helper()
	var ms runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	before := int64(ms.HeapAlloc)
	db, err := Open(bg, dir, Options{ColdValueBytes: cold, CompactAfter: -1})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	grown := int64(ms.HeapAlloc) - before
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	return grown
}

func TestColdHeapAfterOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 20 MB dataset")
	}
	entries, coldBytes := coldEntries()
	dir := writeColdDataset(t, entries)
	hot := heapAfterOpen(t, dir, -1)
	cold := heapAfterOpen(t, dir, 0)
	t.Logf("cold values %d bytes; heap after Open: %d bytes all on heap, %d bytes mapped (saved %d)",
		coldBytes, hot, cold, hot-cold)
	if saved := hot - cold; saved < coldBytes*9/10 {
		t.Fatalf("mapped Open saves %d bytes, want at least 90%% of %d", saved, coldBytes)
	}
}
