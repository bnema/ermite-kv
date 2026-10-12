// SPDX-License-Identifier: MIT

package faultfs

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bnema.dev/ermite-kv/internal/fsys"
)

func create(t *testing.T, f *FS, name string) fsys.File {
	t.Helper()
	h, err := f.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func write(t *testing.T, h fsys.File, s string) {
	t.Helper()
	if _, err := h.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, f *FS, name string) string {
	t.Helper()
	b, ok := f.ReadFile(name)
	if !ok {
		t.Fatalf("%s missing", name)
	}
	return string(b)
}

func TestBasicIO(t *testing.T) {
	f := New()
	if err := f.MkdirAll("/d/sub", 0o700); err != nil {
		t.Fatal(err)
	}
	h := create(t, f, "/d/a")
	write(t, h, "hello ")
	write(t, h, "world")
	buf := make([]byte, 5)
	if n, err := h.ReadAt(buf, 6); n != 5 || err != nil || string(buf) != "world" {
		t.Fatalf("ReadAt = %d %v %q", n, err, buf)
	}
	if n, err := h.ReadAt(make([]byte, 4), 9); n != 2 || err != io.EOF {
		t.Fatalf("short ReadAt = %d %v", n, err)
	}
	if n, err := h.ReadAt(buf, 100); n != 0 || err != io.EOF {
		t.Fatalf("ReadAt past end = %d %v", n, err)
	}
	if err := h.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if sz, _ := h.Size(); sz != 5 {
		t.Fatalf("size %d", sz)
	}
	write(t, h, "!")
	if got := mustRead(t, f, "/d/a"); got != "hello!" {
		t.Fatalf("content %q", got)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write([]byte("x")); err == nil {
		t.Fatal("write on closed file succeeded")
	}
	names, _ := f.ReadDir("/d")
	if len(names) != 2 || names[0] != "a" || names[1] != "sub" {
		t.Fatalf("ReadDir = %v", names)
	}
	if _, err := f.OpenFile("/d/missing", os.O_RDONLY, 0); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("open missing: %v", err)
	}
	if _, err := f.OpenFile("/d/a", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0); !errors.Is(err, os.ErrExist) {
		t.Fatalf("O_EXCL: %v", err)
	}
	if err := f.Rename("/d/a", "/d/b"); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("/d/b"); err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("/d/b"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("second remove: %v", err)
	}
}

func TestFailWriteAndShortWrite(t *testing.T) {
	f := New()
	h := create(t, f, "/a")
	f.FailWrite(2, nil)
	write(t, h, "one")
	if n, err := h.Write([]byte("two")); n != 0 || !errors.Is(err, ErrInjected) {
		t.Fatalf("failed write = %d %v", n, err)
	}
	write(t, h, "three") // faults fire once
	f.ShortWrite(1, 2)
	if n, err := h.Write([]byte("abcdef")); n != 2 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write = %d %v", n, err)
	}
	if got := mustRead(t, f, "/a"); got != "onethreeab" {
		t.Fatalf("content %q", got)
	}
	if f.Writes() != 4 {
		t.Fatalf("Writes = %d", f.Writes())
	}
}

func TestFailSyncAndCounters(t *testing.T) {
	f := New()
	h := create(t, f, "/a")
	write(t, h, "x")
	f.FailSync(2, nil)
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	write(t, h, "y")
	if err := h.SyncAll(); !errors.Is(err, ErrInjected) {
		t.Fatalf("SyncAll = %v", err)
	}
	if d, _ := f.DurableFile("/a"); string(d) != "x" {
		t.Fatalf("failed sync made data durable: %q", d)
	}
	if err := h.SyncAll(); err != nil {
		t.Fatal(err)
	}
	if f.Syncs() != 1 || f.SyncAlls() != 2 {
		t.Fatalf("Syncs=%d SyncAlls=%d", f.Syncs(), f.SyncAlls())
	}
}

func TestSyncDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := New()
		h := create(t, f, "/a")
		f.SetSyncDelay(20 * time.Millisecond)
		for name, sync := range map[string]func() error{"Sync": h.Sync, "SyncAll": h.SyncAll} {
			start := time.Now()
			if err := sync(); err != nil {
				t.Fatal(err)
			}
			if got := time.Since(start); got != 20*time.Millisecond {
				t.Fatalf("%s took %v, want exactly 20ms", name, got)
			}
		}
		f.SetSyncDelay(0)
		start := time.Now()
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 0 {
			t.Fatalf("Sync without delay took %v", got)
		}
	})
}

func TestPowerLossDropsUnsyncedBytes(t *testing.T) {
	f := New()
	f.WriteFile("/d/a", nil)
	h, _ := f.OpenFile("/d/a", os.O_RDWR, 0)
	write(t, h, "synced")
	if err := h.Sync(); err != nil {
		t.Fatal(err)
	}
	write(t, h, "-lost")
	f.PowerLoss(PowerLossOptions{})
	if got := mustRead(t, f, "/d/a"); got != "synced" {
		t.Fatalf("after power loss %q", got)
	}
	if _, err := h.Write([]byte("x")); !errors.Is(err, ErrPowerLoss) {
		t.Fatalf("stale handle write: %v", err)
	}
	h2, _ := f.OpenFile("/d/a", os.O_RDWR, 0)
	write(t, h2, "+new")
	if sz, _ := h2.Size(); sz != 10 {
		t.Fatalf("size %d", sz)
	}
}

func TestPowerLossTruncateNeedsSync(t *testing.T) {
	f := New()
	f.WriteFile("/d/a", []byte("0123456789"))
	h, _ := f.OpenFile("/d/a", os.O_RDWR, 0)
	if err := h.Truncate(4); err != nil {
		t.Fatal(err)
	}
	f.PowerLoss(PowerLossOptions{})
	if got := mustRead(t, f, "/d/a"); got != "0123456789" {
		t.Fatalf("unsynced truncate survived: %q", got)
	}
	h, _ = f.OpenFile("/d/a", os.O_RDWR, 0)
	_ = h.Truncate(4)
	_ = h.SyncAll()
	f.PowerLoss(PowerLossOptions{})
	if got := mustRead(t, f, "/d/a"); got != "0123" {
		t.Fatalf("synced truncate lost: %q", got)
	}
}

func TestPowerLossZeroFillAndPages(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, 5*16)
	run := func(o PowerLossOptions) []byte {
		f := New()
		f.WriteFile("/d/a", []byte("head"))
		h, _ := f.OpenFile("/d/a", os.O_RDWR, 0)
		write(t, h, string(data))
		f.PowerLoss(o)
		b, _ := f.ReadFile("/d/a")
		return b
	}
	zf := run(PowerLossOptions{ZeroFill: true, PageSize: 16})
	if len(zf) != 4+len(data) || string(zf[:4]) != "head" || !bytes.Equal(zf[4:], make([]byte, len(data))) {
		t.Fatalf("zero fill: len=%d %q", len(zf), zf)
	}
	kept := map[int]bool{}
	for seed := uint64(0); seed < 40; seed++ {
		b := run(PowerLossOptions{KeepPages: true, PageSize: 16, Rand: rand.New(rand.NewPCG(seed, 1))})
		if len(b) < 4 || string(b[:4]) != "head" || len(b) > 4+len(data) {
			t.Fatalf("seed %d: bad result len %d", seed, len(b))
		}
		kept[len(b)] = true
		for i, c := range b[4:] {
			if c != 'x' && c != 0 {
				t.Fatalf("seed %d: byte %d = %d", seed, i, c)
			}
		}
	}
	if len(kept) < 3 {
		t.Fatalf("KeepPages gave too little variety: %v", kept)
	}
	if got := run(PowerLossOptions{}); string(got) != "head" {
		t.Fatalf("default power loss kept %q", got)
	}
}

func TestPowerLossDirectoryOps(t *testing.T) {
	f := New()
	if err := f.MkdirAll("/db", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("/db"); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("/"); err != nil {
		t.Fatal(err)
	}

	// Create + sync data, but no SyncDir: the file vanishes.
	h := create(t, f, "/db/unsynced")
	write(t, h, "data")
	_ = h.SyncAll()
	// Create, sync data and dir: survives.
	h = create(t, f, "/db/kept.tmp")
	write(t, h, "kept")
	_ = h.SyncAll()
	if err := f.Rename("/db/kept.tmp", "/db/kept"); err != nil {
		t.Fatal(err)
	}
	// Remove without SyncDir: file comes back.
	f.WriteFile("/db/removed", []byte("old"))
	if err := f.Remove("/db/removed"); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncDir("/other"); err == nil {
		t.Fatal("SyncDir of a missing dir succeeded")
	}
	f.PowerLoss(PowerLossOptions{})
	names, _ := f.ReadDir("/db")
	if len(names) != 1 || names[0] != "removed" {
		t.Fatalf("after power loss without SyncDir: %v", names)
	}

	// Now with SyncDir after each step.
	h = create(t, f, "/db/new.tmp")
	write(t, h, "new")
	_ = h.SyncAll()
	_ = f.Rename("/db/new.tmp", "/db/new")
	_ = f.Remove("/db/removed")
	_ = f.SyncDir("/db")
	f.PowerLoss(PowerLossOptions{})
	names, _ = f.ReadDir("/db")
	if len(names) != 1 || names[0] != "new" {
		t.Fatalf("after power loss with SyncDir: %v", names)
	}
	if got := mustRead(t, f, "/db/new"); got != "new" {
		t.Fatalf("content %q", got)
	}
}

func TestPowerLossRenameOverExisting(t *testing.T) {
	f := New()
	f.WriteFile("/db/x", []byte("old"))
	h := create(t, f, "/db/x.tmp")
	write(t, h, "new")
	_ = h.SyncAll()
	_ = f.Rename("/db/x.tmp", "/db/x")
	f.PowerLoss(PowerLossOptions{})
	if got := mustRead(t, f, "/db/x"); got != "old" {
		t.Fatalf("x = %q, want old", got)
	}
	if _, ok := f.ReadFile("/db/x.tmp"); ok {
		t.Fatal("x.tmp survived without SyncDir")
	}
}

func TestPowerLossUncreatedDirectory(t *testing.T) {
	f := New()
	_ = f.MkdirAll("/a/b", 0o700)
	h := create(t, f, "/a/b/file")
	write(t, h, "x")
	_ = h.SyncAll()
	_ = f.SyncDir("/a/b") // the file entry is durable, its parents are not
	f.PowerLoss(PowerLossOptions{})
	if _, err := f.ReadDir("/a/b"); err == nil {
		t.Fatal("directory created without SyncDir survived")
	}
	if _, ok := f.ReadFile("/a/b/file"); ok {
		t.Fatal("file in a lost directory survived")
	}
}

func TestLockExclusive(t *testing.T) {
	f := New()
	_ = f.MkdirAll("/db", 0o700)
	_ = f.SyncDir("/db")
	_ = f.SyncDir("/")
	l1, err := f.Lock("/db/LOCK")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Lock("/db/LOCK"); !errors.Is(err, fsys.ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	other, err := f.Lock("/db/OTHER")
	if err != nil {
		t.Fatalf("lock on another path: %v", err)
	}
	_ = other.Close()
	_ = l1.Close()
	_ = l1.Close() // idempotent
	l2, err := f.Lock("/db/LOCK")
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	// A stale handle must not release a newer lock after power loss.
	f.PowerLoss(PowerLossOptions{})
	l3, err := f.Lock("/db/LOCK")
	if err != nil {
		t.Fatalf("lock after power loss: %v", err)
	}
	_ = l2.Close()
	if _, err := f.Lock("/db/LOCK"); !errors.Is(err, fsys.ErrLocked) {
		t.Fatalf("stale Close released the lock: %v", err)
	}
	_ = l3.Close()
}

func TestConcurrentUse(t *testing.T) {
	f := New()
	_ = f.MkdirAll("/db", 0o700)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "/db/f" + string(rune('a'+i))
			h, err := f.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
			if err != nil {
				t.Error(err)
				return
			}
			for range 100 {
				_, _ = h.Write([]byte("x"))
				_ = h.Sync()
				_, _ = h.Size()
				_, _ = f.ReadDir("/db")
				_ = f.SyncDir("/db")
			}
			_ = h.Close()
		}()
	}
	wg.Wait()
	if f.Syncs() != 800 || f.Writes() != 800 {
		t.Fatalf("Syncs=%d Writes=%d", f.Syncs(), f.Writes())
	}
}

func TestFailOp(t *testing.T) {
	f := New()
	f.WriteFile("/d/a", []byte("x"))
	f.WriteFile("/d/b", nil)

	f.FailOp(OpOpenFile, 2, nil)
	h, err := f.OpenFile("/d/a", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.OpenFile("/d/a", os.O_RDWR, 0); !errors.Is(err, ErrInjected) {
		t.Fatalf("OpenFile = %v", err)
	}
	if _, err := f.OpenFile("/d/a", os.O_RDWR, 0); err != nil {
		t.Fatalf("faults fire once: %v", err)
	}

	boom := errors.New("boom")
	f.FailOp(OpTruncate, 1, boom)
	if err := h.Truncate(0); !errors.Is(err, boom) {
		t.Fatalf("Truncate = %v", err)
	}
	if sz, _ := h.Size(); sz != 1 {
		t.Fatalf("failed Truncate changed the file: size %d", sz)
	}
	if err := h.Truncate(0); err != nil {
		t.Fatal(err)
	}

	f.FailOp(OpRename, 1, nil)
	if err := f.Rename("/d/b", "/d/c"); !errors.Is(err, ErrInjected) {
		t.Fatalf("Rename = %v", err)
	}
	if _, ok := f.ReadFile("/d/b"); !ok {
		t.Fatal("failed Rename moved the file")
	}
	if err := f.Rename("/d/b", "/d/c"); err != nil {
		t.Fatal(err)
	}

	f.FailOp(OpRemove, 1, nil)
	if err := f.Remove("/d/c"); !errors.Is(err, ErrInjected) {
		t.Fatalf("Remove = %v", err)
	}
	if _, ok := f.ReadFile("/d/c"); !ok {
		t.Fatal("failed Remove deleted the file")
	}

	f.FailOp(OpSyncDir, 1, nil)
	if err := f.SyncDir("/d"); !errors.Is(err, ErrInjected) {
		t.Fatalf("SyncDir = %v", err)
	}
	if err := f.SyncDir("/d"); err != nil {
		t.Fatal(err)
	}
	if got := f.OpCalls(OpSyncDir); got != 2 {
		t.Fatalf("OpCalls(SyncDir) = %d", got)
	}
}

func TestFailedSyncDirKeepsOpsVolatile(t *testing.T) {
	f := New()
	_ = f.MkdirAll("/d", 0o700)
	_ = f.SyncDir("/d")
	create(t, f, "/d/a")
	f.FailOp(OpSyncDir, 1, nil)
	if err := f.SyncDir("/d"); err == nil {
		t.Fatal("SyncDir succeeded")
	}
	f.PowerLoss(PowerLossOptions{})
	if _, ok := f.ReadFile("/d/a"); ok {
		t.Fatal("create survived a failed SyncDir")
	}
}

func TestDropOnSyncFailure(t *testing.T) {
	for _, drop := range []bool{false, true} {
		f := New()
		f.DropOnSyncFailure(drop)
		h := create(t, f, "/a")
		write(t, h, "kept")
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
		write(t, h, "-lost?")
		f.FailSync(1, nil)
		if err := h.Sync(); !errors.Is(err, ErrInjected) {
			t.Fatalf("Sync = %v", err)
		}
		want := "kept-lost?"
		if drop {
			want = "kept"
		}
		if got := mustRead(t, f, "/a"); got != want {
			t.Fatalf("drop=%v: content %q, want %q", drop, got, want)
		}
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
		if d, _ := f.DurableFile("/a"); string(d) != want {
			t.Fatalf("drop=%v: durable %q, want %q", drop, d, want)
		}
	}
}
