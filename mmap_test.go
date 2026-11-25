// SPDX-License-Identifier: MIT

package kv

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"bnema.dev/ermite-kv/internal/fsys"
)

// mapTempFile maps a temp file holding content. unmapped is set when the
// mapping's unmap function runs.
func mapTempFile(t *testing.T, content string) (data []byte, unmap func() error, unmapped *atomic.Bool) {
	t.Helper()
	name := filepath.Join(t.TempDir(), "snap")
	if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := fsys.OS().OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	data, u, err := f.Map()
	if err != nil {
		t.Fatal(err)
	}
	unmapped = new(atomic.Bool)
	return data, func() error { unmapped.Store(true); return u() }, unmapped
}

func TestMappingReleasedByGC(t *testing.T) {
	data, unmap, unmapped := mapTempFile(t, "cold value")
	m := newMapping(data, unmap)
	if string(m.data) != "cold value" {
		t.Fatalf("mapping = %q", m.data)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !unmapped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("mapping not released after 5s of GCs")
		}
		runtime.GC()
		runtime.Gosched()
	}
}

func TestMappingKeptWhileReachable(t *testing.T) {
	data, unmap, unmapped := mapTempFile(t, "hot")
	m := newMapping(data, unmap)
	for range 5 {
		runtime.GC()
		runtime.Gosched()
	}
	if unmapped.Load() {
		t.Fatal("reachable mapping was released")
	}
	if string(m.data) != "hot" {
		t.Fatalf("data = %q", m.data)
	}
	runtime.KeepAlive(m)
}
