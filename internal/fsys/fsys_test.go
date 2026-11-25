// SPDX-License-Identifier: MIT

package fsys

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOS(t *testing.T) {
	fx := OS()
	dir := filepath.Join(t.TempDir(), "a", "b")
	if err := fx.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fx.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "f.tmp")
	f, err := fx.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"hello ", "world"} {
		if _, err := f.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncAll(); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(8); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("!")); err != nil { // appends after truncation
		t.Fatal(err)
	}
	if sz, err := f.Size(); err != nil || sz != 9 {
		t.Fatalf("Size = %d, %v", sz, err)
	}
	buf := make([]byte, 9)
	if n, err := f.ReadAt(buf, 0); n != 9 || (err != nil && !errors.Is(err, io.EOF)) || string(buf) != "hello wo!" {
		t.Fatalf("ReadAt = %d %v %q", n, err, buf)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	final := filepath.Join(dir, "f")
	if err := fx.Rename(name, final); err != nil {
		t.Fatal(err)
	}
	names, err := fx.ReadDir(dir)
	if err != nil || len(names) != 1 || names[0] != "f" {
		t.Fatalf("ReadDir = %v, %v", names, err)
	}
	if err := fx.Remove(final); err != nil {
		t.Fatal(err)
	}
}

func TestOSLock(t *testing.T) {
	fx := OS()
	path := filepath.Join(t.TempDir(), "LOCK")
	l, err := fx.Lock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fx.Lock(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = fx.Lock(path)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	_ = l.Close()
}

func TestOSMapOutlivesFileAndPath(t *testing.T) {
	fx := OS()
	name := filepath.Join(t.TempDir(), "m")
	want := []byte("mapped content")
	if err := os.WriteFile(name, want, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := fx.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, unmap, err := f.Map()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fx.Remove(name); err != nil {
		t.Fatal(err)
	}
	if string(data) != string(want) {
		t.Fatalf("data = %q, want %q", data, want)
	}
	if err := unmap(); err != nil {
		t.Fatalf("unmap: %v", err)
	}
	if err := unmap(); err != nil {
		t.Fatalf("second unmap: %v", err)
	}
}

func TestOSMapEmptyFile(t *testing.T) {
	fx := OS()
	name := filepath.Join(t.TempDir(), "e")
	f, err := fx.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, _, err := f.Map(); err == nil {
		t.Fatal("Map of empty file succeeded")
	}
}
