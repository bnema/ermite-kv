// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

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
