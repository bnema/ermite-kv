// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"bnema.dev/ermite-kv/internal/fsys"
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
	checkCold(t, s, "a", big1, true)
	checkCold(t, s, "b", []byte("small"), false)
	checkCold(t, s, "c", big2, true)
	for _, k := range []string{"a", "c"} {
		it, _ := s.Get(bg, []byte(k))
		if it.Version != seq || filepath.Base(it.m.name) != snapshotName(seq) {
			t.Fatalf("%s: version %d, mapping %s", k, it.Version, it.m.name)
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

func TestColdWorkDoesNotBlockClose(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: 1})
	for i := range 50 {
		mustApply(t, db, i+1)
	}
	closeBounded(t, db)
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

func TestColdSwapAfterCompaction(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: 1024})
	big := bytes.Repeat([]byte("x"), 2000)
	var b Batch
	b.Put([]byte("a"), big)
	b.Put([]byte("b"), []byte("small"))
	b.Put([]byte("c"), big)
	if _, err := db.Apply(bg, &b); err != nil {
		t.Fatal(err)
	}
	waitColdJobs(t, db, 1)
	s := db.Snapshot(bg)
	checkCold(t, s, "a", big, true)
	checkCold(t, s, "b", []byte("small"), false)
	checkCold(t, s, "c", big, true)
	if s.Seq() != 1 {
		t.Fatalf("seq = %d", s.Seq())
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

func TestCloseWithPendingColdWork(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: -1})
	m := fakeMapping("v")
	cands := make([]coldCand, 100*coldChunk)
	for i := range cands {
		cands[i] = coldCand{key: treeKey(i), version: 1, value: m.data}
	}
	db.coldCh <- &coldWork{m: m, cands: cands}
	closeBounded(t, db)
}

// watchReleases records the names of the mappings released while the test
// runs.
func watchReleases(t *testing.T) func(name string) bool {
	var mu sync.Mutex
	released := map[string]bool{}
	f := func(name string) {
		mu.Lock()
		released[name] = true
		mu.Unlock()
	}
	onRelease.Store(&f)
	t.Cleanup(func() { onRelease.Store(nil) })
	return func(name string) bool {
		mu.Lock()
		defer mu.Unlock()
		return released[name]
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
	released := watchReleases(t)
	dir, big, _ := coldDir(t)
	gen0 := filepath.Join(dir, snapshotName(2))
	// Compaction is off: the test drives each swap itself.
	db := openCold(t, dir, Options{CompactAfter: -1})
	v := bareValue(db, "big")
	swap := func(n int64) {
		m := &mapping{data: slices.Clone(big), name: fmt.Sprint("swap-", n)}
		db.coldCh <- &coldWork{m: m, cands: []coldCand{{key: []byte("big"), version: 1, value: m.data}}}
		waitColdJobs(t, db, n)
	}

	swap(1) // generation 0 is no longer referenced by the tree, only pinned
	if it, _ := db.Snapshot(bg).Get(bg, []byte("big")); it.m.name != "swap-1" {
		t.Fatalf("big still in %s", it.m.name)
	}
	for range 5 {
		runtime.GC()
		runtime.Gosched()
	}
	if released(gen0) {
		t.Fatal("generation 0 released after one swap")
	}
	if !bytes.Equal(v, big) {
		t.Fatal("bare value changed")
	}

	swap(2) // releases generation 0
	deadline := time.Now().Add(10 * time.Second)
	for !released(gen0) {
		if time.Now().After(deadline) {
			t.Fatal("generation 0 not released after two swaps")
		}
		runtime.GC()
		runtime.Gosched()
	}
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
