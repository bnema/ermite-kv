// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"os"
	"path/filepath"
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

// receiveCold waits for the next cold work of db.
func receiveCold(t *testing.T, db *DB) *coldWork {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case w := <-db.coldCh:
		return w
	case <-timer.C:
		t.Fatal("no cold work")
	}
	return nil
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
	w := receiveCold(t, db)
	if w.m == nil || len(w.cands) != 2 {
		t.Fatalf("cold work = %+v", w)
	}
	want := map[string][]byte{"a": big1, "c": big2}
	for _, c := range w.cands {
		if !bytes.Equal(c.value, want[string(c.key)]) || c.version != seq {
			t.Fatalf("candidate %s: version %d (want %d), %d value bytes", c.key, c.version, seq, len(c.value))
		}
		delete(want, string(c.key))
	}
	if len(want) != 0 {
		t.Fatalf("missing candidates %v", want)
	}
	if filepath.Base(w.m.name) != snapshotName(seq) {
		t.Fatalf("mapping name = %s", w.m.name)
	}
}

func TestColdWorkDisabled(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: -1})
	for i := range 20 {
		mustApply(t, db, i+1)
	}
	closeBounded(t, db) // waits for the compactors
	if n := len(db.coldCh); n != 0 {
		t.Fatalf("%d cold work sent with ColdValueBytes -1", n)
	}
}

func TestColdWorkWithoutReaderDoesNotBlockClose(t *testing.T) {
	db := openCold(t, t.TempDir(), Options{CompactAfter: 1, ColdValueBytes: 1})
	for i := range 50 {
		mustApply(t, db, i+1)
	}
	closeBounded(t, db)
	if n := len(db.coldCh); n != 1 {
		t.Fatalf("coldCh holds %d jobs, want 1", n)
	}
}
