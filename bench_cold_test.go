// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// coldClass is one group of values of the benchmark dataset, stored under
// prefix/<n>.
type coldClass struct {
	prefix   string
	count    int
	min, max int // value size range
}

// coldDataset is about 20 MB shaped like production data: large job
// contexts and documents, mid-size records and many small values. Prefixes
// are in ascending order.
var coldDataset = []coldClass{
	{"ctx/", 400, 16 << 10, 18 << 10},
	{"job/", 800, 14 << 10, 16 << 10},
	{"meta/", 6000, 100, 300},
	{"rec/", 900, 1800, 2200},
}

type benchEntry struct {
	key, value []byte
}

// coldEntries returns the dataset in ascending key order and the total size
// of its values of at least 1 KiB.
func coldEntries() (entries []benchEntry, coldBytes int64) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, c := range coldDataset {
		for i := range c.count {
			v := make([]byte, c.min+rng.IntN(c.max-c.min+1))
			for j := range v {
				v[j] = byte('a' + rng.IntN(26))
			}
			entries = append(entries, benchEntry{fmt.Appendf(nil, "%s%010d", c.prefix, i), v})
			if len(v) >= defaultColdValue {
				coldBytes += int64(len(v))
			}
		}
	}
	return entries, coldBytes
}

// writeColdDataset writes the dataset as snapshot 1 in a new directory.
func writeColdDataset(tb testing.TB, entries []benchEntry) string {
	tb.Helper()
	dir := tb.TempDir()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, 1, uint64(len(entries)), func(yield func(k, v []byte, ver uint64) bool) {
		for _, e := range entries {
			if !yield(e.key, e.value, 1) {
				return
			}
		}
	}); err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotName(1)), buf.Bytes(), 0o600); err != nil {
		tb.Fatal(err)
	}
	return dir
}

func openBench(tb testing.TB, dir string, cold int) *DB {
	tb.Helper()
	db, err := Open(bg, dir, Options{ColdValueBytes: cold, CompactAfter: -1})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close(bg) })
	return db
}

var benchSink byte

func benchGet(b *testing.B, cold int) {
	entries, _ := coldEntries()
	db := openBench(b, writeColdDataset(b, entries), cold)
	var keys [][]byte
	for _, e := range entries {
		if len(e.value) >= defaultColdValue {
			keys = append(keys, e.key)
		}
	}
	s := db.Snapshot(bg)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		it, _ := s.Get(bg, keys[i%len(keys)])
		benchSink ^= it.Value[len(it.Value)/2]
	}
}

func BenchmarkColdGet(b *testing.B) { benchGet(b, 0) }
func BenchmarkHotGet(b *testing.B)  { benchGet(b, -1) }

func benchPrefix(b *testing.B, cold int) {
	entries, _ := coldEntries()
	db := openBench(b, writeColdDataset(b, entries), cold)
	s := db.Snapshot(bg)
	b.ResetTimer()
	for b.Loop() {
		for _, it := range s.Prefix(bg, []byte("job/")) {
			benchSink ^= it.Value[len(it.Value)/2]
		}
	}
}

func BenchmarkColdPrefix(b *testing.B) { benchPrefix(b, 0) }
func BenchmarkHotPrefix(b *testing.B)  { benchPrefix(b, -1) }

// heapAfterOpen returns the heap growth caused by opening dir with cold, the
// DB still open.
func heapAfterOpen(t *testing.T, dir string, cold int) int64 {
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
