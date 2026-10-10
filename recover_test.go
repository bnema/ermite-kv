// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"bnema.dev/ermite-kv/internal/fsys"
	"bnema.dev/ermite-kv/internal/fsys/faultfs"
)

const testDir = "/db"

// state is the replayed content of a database.
type state struct {
	m        map[string]string
	versions map[string]uint64
	seqs     []uint64 // seqs passed to apply, in order
}

func (s *state) load(k, v []byte, ver uint64) {
	s.m[string(k)] = string(v)
	s.versions[string(k)] = ver
}

func (s *state) apply(seq uint64, ops []op) {
	s.seqs = append(s.seqs, seq)
	for _, o := range ops {
		switch o.kind {
		case opPut:
			s.m[string(o.key)] = string(o.value)
			s.versions[string(o.key)] = seq
		case opDelete:
			delete(s.m, string(o.key))
			delete(s.versions, string(o.key))
		}
	}
}

func recoverState(t *testing.T, fx fsys.FS, logger *slog.Logger) (*state, recovered, error) {
	t.Helper()
	s := &state{m: map[string]string{}, versions: map[string]uint64{}}
	rec, err := recoverDir(fx, testDir, logger, s.load, s.apply)
	return s, rec, err
}

// mustRecover recovers, checks the invariants of a successful recovery and
// releases the lock and the log file.
func mustRecover(t *testing.T, fx fsys.FS) (*state, recovered) {
	t.Helper()
	s, rec, err := recoverState(t, fx, nil)
	if err != nil {
		t.Fatalf("recoverDir: %v", err)
	}
	size, err := rec.log.Size()
	if err != nil || size != rec.logSize {
		t.Fatalf("log size = %d (err %v), recovered.logSize = %d", size, err, rec.logSize)
	}
	t.Cleanup(func() { _ = rec.log.Close(); _ = rec.lock.Close() })
	return s, rec
}

// release closes the log file and the lock of a recovery result.
func release(rec recovered) {
	_ = rec.log.Close()
	_ = rec.lock.Close()
}

func putOp(k, v string) op { return op{kind: opPut, key: []byte(k), value: []byte(v)} }
func delOp(k string) op    { return op{kind: opDelete, key: []byte(k)} }

// logBytes builds a log file: header for base, then one record per batch with
// consecutive seqs starting at base+1.
func logBytes(base uint64, batches ...[]op) []byte {
	b := appendLogHeader(nil, base)
	for i, ops := range batches {
		b = appendRecord(b, base+1+uint64(i), ops)
	}
	return b
}

func put(fx *faultfs.FS, name string, data []byte) { fx.WriteFile(filepath.Join(testDir, name), data) }

func snapshotBytes(t *testing.T, seq uint64, entries ...snapEntry) []byte {
	t.Helper()
	return encodeSnapshot(t, seq, entries)
}

func snapEnt(k, v string, ver uint64) snapEntry { return snapEntry{[]byte(k), []byte(v), ver} }

func wantState(t *testing.T, s *state, m map[string]string) {
	t.Helper()
	if len(s.m) != len(m) {
		t.Fatalf("state = %v, want %v", s.m, m)
	}
	for k, v := range m {
		if got, ok := s.m[k]; !ok || got != v {
			t.Fatalf("state = %v, want %v", s.m, m)
		}
	}
}

func TestRecoverEmptyDirCreatesLog(t *testing.T) {
	fx := faultfs.New()
	s, rec := mustRecover(t, fx)
	wantState(t, s, nil)
	if rec.snapSeq != 0 || rec.lastSeq != 0 || rec.logBase != 0 || rec.logSize != logHeaderSize {
		t.Fatalf("rec = %+v", rec)
	}
	// The new log must survive a power loss: file creation rules.
	_ = rec.log.Close()
	_ = rec.lock.Close()
	fx.PowerLoss(faultfs.PowerLossOptions{})
	names, err := fx.ReadDir(testDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(names, ","); got != "LOCK,"+logName(0) && got != logName(0) {
		t.Fatalf("after power loss: %v", names)
	}
	data, _ := fx.ReadFile(filepath.Join(testDir, logName(0)))
	if !bytes.Equal(data, appendLogHeader(nil, 0)) {
		t.Fatalf("log content %x", data)
	}
}

func TestRecoverLockedAndRelease(t *testing.T) {
	fx := faultfs.New()
	_, rec := mustRecover(t, fx)
	if _, _, err := recoverState(t, fx, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second recover: err = %v, want ErrLocked", err)
	}
	_ = rec.log.Close()
	_ = rec.lock.Close()
	_, rec2, err := recoverState(t, fx, nil)
	if err != nil {
		t.Fatalf("recover after release: %v", err)
	}
	_ = rec2.log.Close()
	_ = rec2.lock.Close()
}

func TestRecoverReleasesLockOnError(t *testing.T) {
	fx := faultfs.New()
	put(fx, logName(0), []byte("garbage"))
	if _, _, err := recoverState(t, fx, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	l, err := fx.Lock(filepath.Join(testDir, lockName))
	if err != nil {
		t.Fatalf("lock still held after failure: %v", err)
	}
	_ = l.Close()
}

func TestRecoverCleanLog(t *testing.T) {
	fx := faultfs.New()
	data := logBytes(0,
		[]op{putOp("a", "1"), putOp("b", "2")},
		[]op{delOp("a"), putOp("c", "3")},
		[]op{putOp("b", "4")},
	)
	put(fx, logName(0), data)
	s, rec := mustRecover(t, fx)
	wantState(t, s, map[string]string{"b": "4", "c": "3"})
	if got := fmt.Sprint(s.seqs); got != "[1 2 3]" {
		t.Fatalf("applied seqs %s", got)
	}
	if s.versions["b"] != 3 || s.versions["c"] != 2 {
		t.Fatalf("versions = %v", s.versions)
	}
	if rec.snapSeq != 0 || rec.lastSeq != 3 || rec.logBase != 0 || rec.logSize != int64(len(data)) {
		t.Fatalf("rec = %+v", rec)
	}
	// Appends go to the end of the recovered log.
	if _, err := rec.log.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if size, _ := rec.log.Size(); size != int64(len(data))+1 {
		t.Fatalf("size after append = %d", size)
	}
}

func TestRecoverDeletesTmpFiles(t *testing.T) {
	fx := faultfs.New()
	put(fx, logName(0), logBytes(0, []op{putOp("a", "1")}))
	put(fx, snapshotName(5)+tmpSuffix, []byte("partial"))
	put(fx, logName(5)+tmpSuffix, []byte("partial"))
	s, _ := mustRecover(t, fx)
	wantState(t, s, map[string]string{"a": "1"})
	names, _ := fx.ReadDir(testDir)
	for _, n := range names {
		if strings.HasSuffix(n, tmpSuffix) {
			t.Fatalf("tmp file %s left", n)
		}
	}
	// Deletion is durable.
	fx.PowerLoss(faultfs.PowerLossOptions{})
	names, _ = fx.ReadDir(testDir)
	for _, n := range names {
		if strings.HasSuffix(n, tmpSuffix) {
			t.Fatalf("tmp file %s back after power loss", n)
		}
	}
}

func TestRecoverTornTail(t *testing.T) {
	good := logBytes(0, []op{putOp("a", "1")}, []op{putOp("b", "2")})
	next := appendRecord(nil, 3, []op{putOp("c", "3")})
	cases := map[string][]byte{
		"short frame":      next[:3],
		"short record":     next[:len(next)-2],
		"zero-filled tail": make([]byte, 100),
		"garbage":          []byte("this is not a record at all, just junk bytes"),
		"bad crc":          append(append([]byte(nil), next[:len(next)-1]...), next[len(next)-1]^1),
		"huge length":      {0xff, 0xff, 0xff, 0x7f, 1, 2, 3, 4, 5, 6, 7, 8},
		"one byte":         {0x01},
	}
	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			fx := faultfs.New()
			put(fx, logName(0), append(append([]byte(nil), good...), tail...))
			var logs bytes.Buffer
			s, rec, err := recoverState(t, fx, slog.New(slog.NewTextHandler(&logs, nil)))
			if err != nil {
				t.Fatal(err)
			}
			defer release(rec)
			wantState(t, s, map[string]string{"a": "1", "b": "2"})
			if rec.lastSeq != 2 || rec.logSize != int64(len(good)) {
				t.Fatalf("rec = %+v, want size %d", rec, len(good))
			}
			if got, _ := fx.ReadFile(filepath.Join(testDir, logName(0))); !bytes.Equal(got, good) {
				t.Fatalf("log not truncated: %d bytes, want %d", len(got), len(good))
			}
			if got, _ := fx.DurableFile(filepath.Join(testDir, logName(0))); !bytes.Equal(got, good) {
				t.Fatal("truncation not synced")
			}
			if !strings.Contains(logs.String(), "dropped torn log tail") {
				t.Fatalf("no warning logged: %q", logs.String())
			}
		})
	}
}

func TestRecoverTornTailOnlyHeaderSurvives(t *testing.T) {
	fx := faultfs.New()
	put(fx, logName(0), append(logBytes(0), make([]byte, 7)...))
	s, rec := mustRecover(t, fx)
	wantState(t, s, nil)
	if rec.logSize != logHeaderSize {
		t.Fatalf("logSize = %d", rec.logSize)
	}
}

func TestRecoverDamageFollowedByValidRecord(t *testing.T) {
	r1 := appendRecord(nil, 1, []op{putOp("a", "1")})
	r2 := appendRecord(nil, 2, []op{putOp("b", "2")})
	r3 := appendRecord(nil, 3, []op{putOp("c", "3")})
	flip := func(b []byte, i int) []byte {
		c := append([]byte(nil), b...)
		c[i] ^= 0xff
		return c
	}
	hdr := appendLogHeader(nil, 0)
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	cases := map[string][]byte{
		"bad crc in middle record":     cat(hdr, r1, flip(r2, len(r2)-1), r3),
		"bad payload in first record":  cat(hdr, flip(r1, 9), r2),
		"bad checksum field":           cat(hdr, r1, flip(r2, 4), r3),
		"damaged record before last":   cat(hdr, flip(r1, len(r1)-1), r2, r3),
		"record with wrong count":      cat(hdr, rawRecord(rawPayload(1, 5, opDelete, 1, 'k')), r2),
		"corrupt but len looks intact": cat(hdr, r1, flip(r2, 12), r3),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			fx := faultfs.New()
			put(fx, logName(0), data)
			_, _, err := recoverState(t, fx, nil)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
			// The file must be left untouched.
			if got, _ := fx.ReadFile(filepath.Join(testDir, logName(0))); !bytes.Equal(got, data) {
				t.Fatal("corrupt log was modified")
			}
		})
	}
}

func TestRecoverDamageInOlderLog(t *testing.T) {
	old := logBytes(0, []op{putOp("a", "1")}, []op{putOp("b", "2")})
	cases := map[string][]byte{
		"torn tail":   old[:len(old)-3],
		"bad crc":     append(append([]byte(nil), old[:len(old)-1]...), old[len(old)-1]^1),
		"zero tail":   append(append([]byte(nil), old...), make([]byte, 30)...),
		"extra bytes": append(append([]byte(nil), old...), 1, 2, 3),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			fx := faultfs.New()
			put(fx, logName(0), data)
			put(fx, logName(2), logBytes(2, []op{putOp("c", "3")}))
			if _, _, err := recoverState(t, fx, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestRecoverBadLogHeader(t *testing.T) {
	good := logBytes(0, []op{putOp("a", "1")})
	flip := func(i int) []byte {
		c := append([]byte(nil), good...)
		c[i] ^= 1
		return c
	}
	cases := map[string]struct {
		name string
		data []byte
	}{
		"empty file":      {logName(0), nil},
		"short header":    {logName(0), good[:10]},
		"bad magic":       {logName(0), flip(0)},
		"bad crc":         {logName(0), flip(18)},
		"flipped base":    {logName(0), flip(9)},
		"base != name":    {logName(7), good},
		"all zero header": {logName(0), make([]byte, logHeaderSize)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			fx := faultfs.New()
			put(fx, c.name, c.data)
			if _, _, err := recoverState(t, fx, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
	t.Run("bad header in older log", func(t *testing.T) {
		fx := faultfs.New()
		put(fx, logName(0), good[:10])
		put(fx, logName(1), logBytes(1, []op{putOp("b", "2")}))
		if _, _, err := recoverState(t, fx, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v, want ErrCorrupt", err)
		}
	})
}

func TestRecoverSeqGap(t *testing.T) {
	cases := map[string]func(fx *faultfs.FS){
		"gap inside a log": func(fx *faultfs.FS) {
			b := appendLogHeader(nil, 0)
			b = appendRecord(b, 1, []op{putOp("a", "1")})
			b = appendRecord(b, 3, []op{putOp("b", "2")})
			put(fx, logName(0), b)
		},
		"first record not base+1": func(fx *faultfs.FS) {
			b := appendRecord(appendLogHeader(nil, 0), 2, []op{putOp("a", "1")})
			put(fx, logName(0), b)
		},
		"gap between logs": func(fx *faultfs.FS) {
			put(fx, logName(0), logBytes(0, []op{putOp("a", "1")}))
			put(fx, logName(2), logBytes(2, []op{putOp("c", "3")}))
		},
		"first log starts after seq 0": func(fx *faultfs.FS) {
			put(fx, logName(4), logBytes(4, []op{putOp("a", "1")}))
		},
		"log after snapshot has a gap": func(fx *faultfs.FS) {
			put(fx, snapshotName(3), snapshotBytes(t, 3, snapEnt("a", "1", 3)))
			put(fx, logName(5), logBytes(5, []op{putOp("b", "2")}))
		},
		"logs end before snapshot": func(fx *faultfs.FS) {
			put(fx, snapshotName(5), snapshotBytes(t, 5, snapEnt("a", "1", 3)))
			put(fx, logName(0), logBytes(0, []op{putOp("a", "1")}))
		},
		"duplicate seq": func(fx *faultfs.FS) {
			b := appendLogHeader(nil, 0)
			b = appendRecord(b, 1, []op{putOp("a", "1")})
			b = appendRecord(b, 1, []op{putOp("b", "2")})
			put(fx, logName(0), b)
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			fx := faultfs.New()
			setup(fx)
			if _, _, err := recoverState(t, fx, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestRecoverSnapshotThenLogs(t *testing.T) {
	fx := faultfs.New()
	put(fx, snapshotName(3), snapshotBytes(t, 3, snapEnt("a", "1", 1), snapEnt("b", "2", 3)))
	// log-0 still holds records already in the snapshot; log-3 continues it.
	put(fx, logName(0), logBytes(0, []op{putOp("a", "old")}, []op{putOp("x", "gone")}, []op{putOp("b", "2")}))
	put(fx, logName(3), logBytes(3, []op{putOp("c", "3")}, []op{delOp("a")}))
	s, rec := mustRecover(t, fx)
	wantState(t, s, map[string]string{"b": "2", "c": "3"})
	if got := fmt.Sprint(s.seqs); got != "[4 5]" {
		t.Fatalf("applied seqs %s", got)
	}
	if s.versions["b"] != 3 || s.versions["c"] != 4 {
		t.Fatalf("versions = %v", s.versions)
	}
	if rec.snapSeq != 3 || rec.lastSeq != 5 || rec.logBase != 3 {
		t.Fatalf("rec = %+v", rec)
	}
}

func TestRecoverSnapshotWithoutLogCreatesLog(t *testing.T) {
	fx := faultfs.New()
	put(fx, snapshotName(7), snapshotBytes(t, 7, snapEnt("a", "1", 7)))
	s, rec := mustRecover(t, fx)
	wantState(t, s, map[string]string{"a": "1"})
	if rec.snapSeq != 7 || rec.lastSeq != 7 || rec.logBase != 7 || rec.logSize != logHeaderSize {
		t.Fatalf("rec = %+v", rec)
	}
	if _, ok := fx.ReadFile(filepath.Join(testDir, logName(7))); !ok {
		t.Fatal("log-7 not created")
	}
}

func TestRecoverInvalidNewestSnapshotFallsBack(t *testing.T) {
	good := snapshotBytes(t, 2, snapEnt("a", "1", 1), snapEnt("b", "2", 2))
	newer := snapshotBytes(t, 4, snapEnt("a", "1", 1), snapEnt("z", "9", 4))
	truncated := newer[:len(newer)-2]
	flipped := append([]byte(nil), newer...)
	flipped[30] ^= 1
	wrongName := snapshotBytes(t, 3, snapEnt("a", "1", 1))
	for name, bad := range map[string][]byte{"truncated": truncated, "flipped": flipped, "empty": nil, "seq/name mismatch": wrongName} {
		t.Run(name, func(t *testing.T) {
			fx := faultfs.New()
			put(fx, snapshotName(2), good)
			put(fx, snapshotName(4), bad)
			put(fx, logName(2), logBytes(2, []op{putOp("c", "3")}, []op{putOp("d", "4")}, []op{delOp("a")}))
			var logs bytes.Buffer
			s, rec, err := recoverState(t, fx, slog.New(slog.NewTextHandler(&logs, nil)))
			if err != nil {
				t.Fatal(err)
			}
			defer release(rec)
			wantState(t, s, map[string]string{"b": "2", "c": "3", "d": "4"})
			if rec.snapSeq != 2 || rec.lastSeq != 5 {
				t.Fatalf("rec = %+v", rec)
			}
			if !strings.Contains(logs.String(), "invalid snapshot") {
				t.Fatalf("fallback not logged: %q", logs.String())
			}
		})
	}
}

func TestRecoverInvalidSnapshotNoOtherFallsBackToEmpty(t *testing.T) {
	fx := faultfs.New()
	put(fx, snapshotName(3), []byte("junk"))
	put(fx, logName(0), logBytes(0, []op{putOp("a", "1")}))
	s, rec := mustRecover(t, fx)
	wantState(t, s, map[string]string{"a": "1"})
	if rec.snapSeq != 0 || rec.lastSeq != 1 {
		t.Fatalf("rec = %+v", rec)
	}
}

func TestRecoverNoSnapshotStartsAtZero(t *testing.T) {
	fx := faultfs.New()
	put(fx, logName(0), logBytes(0, []op{putOp("a", "1")}))
	put(fx, logName(1), logBytes(1, []op{putOp("b", "2")}))
	put(fx, "unrelated.txt", []byte("ignored"))
	put(fx, "log-xyz", []byte("ignored"))
	s, rec := mustRecover(t, fx)
	wantState(t, s, map[string]string{"a": "1", "b": "2"})
	if rec.snapSeq != 0 || rec.lastSeq != 2 || rec.logBase != 1 {
		t.Fatalf("rec = %+v", rec)
	}
}

func TestRecoverSnapshotLoadOrderAndOwnership(t *testing.T) {
	fx := faultfs.New()
	put(fx, snapshotName(1), snapshotBytes(t, 1, snapEnt("a", "1", 1), snapEnt("b", "2", 1), snapEnt("c", "3", 1)))
	var keys []string
	var held [][]byte
	_, err := recoverDir(fx, testDir, nil, func(k, v []byte, ver uint64) {
		keys = append(keys, string(k))
		held = append(held, k, v)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(keys, "") != "abc" {
		t.Fatalf("keys = %v", keys)
	}
	var all []byte
	for _, h := range held {
		all = append(all, h...)
	}
	if string(all) != "a1b2c3" {
		t.Fatalf("retained slices changed: %q", all)
	}
}

func TestRecoverPowerLossAfterRecovery(t *testing.T) {
	// A fresh directory survives a power loss right after recovery and
	// reopens cleanly with the same state.
	fx := faultfs.New()
	_, rec := mustRecover(t, fx)
	_ = rec.log.Close()
	_ = rec.lock.Close()
	fx.PowerLoss(faultfs.PowerLossOptions{})
	s, rec2 := mustRecover(t, fx)
	wantState(t, s, nil)
	if rec2.logBase != 0 {
		t.Fatalf("rec = %+v", rec2)
	}
}

func TestRecoverOSFilesystem(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "db")
	data := logBytes(0, []op{putOp("a", "1")}, []op{putOp("b", "2")})
	torn := append(append([]byte(nil), data...), 9, 9, 9)

	rec, err := recoverDir(fsys.OS(), dir, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoverDir(fsys.OS(), dir, nil, nil, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second recover: err = %v, want ErrLocked", err)
	}
	if _, err := rec.log.Write(data[logHeaderSize:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.log.Write(torn[len(data):]); err != nil {
		t.Fatal(err)
	}
	if err := rec.log.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = rec.log.Close()
	_ = rec.lock.Close()

	s := &state{m: map[string]string{}, versions: map[string]uint64{}}
	rec, err = recoverDir(fsys.OS(), dir, nil, s.load, s.apply)
	if err != nil {
		t.Fatal(err)
	}
	defer release(rec)
	wantState(t, s, map[string]string{"a": "1", "b": "2"})
	if rec.lastSeq != 2 || rec.logSize != int64(len(data)) {
		t.Fatalf("rec = %+v", rec)
	}
	buf := make([]byte, len(data)+10)
	n, err := rec.log.ReadAt(buf, 0)
	if n != len(data) || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatalf("ReadAt = %d, %v: tail not truncated", n, err)
	}
}
