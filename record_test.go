// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"slices"
	"testing"
)

func sampleOps() []op {
	return []op{
		{kind: opPut, key: []byte("a"), value: []byte("1")},
		{kind: opDelete, key: []byte("b")},
		{kind: opPut, key: []byte("c"), value: nil},
		{kind: opPut, key: bytes.Repeat([]byte("k"), 200), value: bytes.Repeat([]byte("v"), 300)},
	}
}

func opsEqual(a, b []op) bool {
	return slices.EqualFunc(a, b, func(x, y op) bool {
		return x.kind == y.kind && bytes.Equal(x.key, y.key) && bytes.Equal(x.value, y.value)
	})
}

func TestRecordRoundTrip(t *testing.T) {
	ops := sampleOps()
	second := []op{{kind: opDelete, key: []byte("z")}}
	enc := appendRecord([]byte("prefix"), 42, ops, second, ops[:1])[6:]
	size := 0
	for _, b := range [][]op{ops, second, ops[:1]} {
		size += batchHeader
		for _, o := range b {
			size += opSize(o)
		}
	}
	if want := recordFrame + recordHead + size; len(enc) != want {
		t.Fatalf("encoded size = %d, want %d", len(enc), want)
	}
	seq, got, n, err := decodeRecord(append(enc, 0xff, 0xee))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 42 || n != len(enc) || len(got) != 3 ||
		!opsEqual(got[0], ops) || !opsEqual(got[1], second) || !opsEqual(got[2], ops[:1]) {
		t.Fatalf("got seq=%d n=%d batches=%v", seq, n, got)
	}
	owned := ownOps(got[0])
	clear(enc)
	if !opsEqual(owned, ops) {
		t.Fatal("ownOps aliases the source buffer")
	}
}

// TestRecordBuilder checks that beginRecord/appendBatch/finishRecord produce
// the same bytes as appendRecord.
func TestRecordBuilder(t *testing.T) {
	ops := sampleOps()
	rec := beginRecord(nil, 7)
	rec = appendBatch(rec, ops)
	rec = appendBatch(rec, ops[:2])
	finishRecord(rec, 2)
	if want := appendRecord(nil, 7, ops, ops[:2]); !bytes.Equal(rec, want) {
		t.Fatal("builder output differs from appendRecord")
	}
}

func TestLogHeader(t *testing.T) {
	h := appendLogHeader(nil, 0x0102030405)
	if len(h) != logHeaderSize {
		t.Fatalf("header size %d", len(h))
	}
	if base, err := decodeLogHeader(h); err != nil || base != 0x0102030405 {
		t.Fatalf("base=%d err=%v", base, err)
	}
	for i := range h {
		bad := slices.Clone(h)
		bad[i] ^= 1
		if _, err := decodeLogHeader(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flip byte %d: err=%v", i, err)
		}
	}
	if _, err := decodeLogHeader(h[:logHeaderSize-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short header: err=%v", err)
	}
}

// rawRecord frames payload with a correct length and checksum.
func rawRecord(payload []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(payload, castagnoli))
	return append(b, payload...)
}

// rawPayload builds a payload from the seq, the batch count and raw batch
// bytes.
func rawPayload(seq uint64, batches uint32, body ...byte) []byte {
	p := binary.LittleEndian.AppendUint64(nil, seq)
	p = binary.LittleEndian.AppendUint32(p, batches)
	return append(p, body...)
}

// rawBatch encodes a batch header and raw op bytes.
func rawBatch(count uint32, ops ...byte) []byte {
	return append(binary.LittleEndian.AppendUint32(nil, count), ops...)
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestDecodeRecordUnreadable(t *testing.T) {
	one := []op{{kind: opPut, key: []byte("k"), value: []byte("v")}}
	good := appendRecord(nil, 1, one)
	badCRC := slices.Clone(good)
	badCRC[len(badCRC)-1] ^= 1
	tooBig := binary.LittleEndian.AppendUint32(nil, maxRecord+1)
	tooBig = append(tooBig, make([]byte, 16)...)
	put := rawBatch(1, opPut, 1, 'k', 1, 'v')
	del := rawBatch(1, opDelete, 1, 'k')
	cases := map[string][]byte{
		"empty":            nil,
		"short frame":      good[:7],
		"short payload":    good[:len(good)-1],
		"bad crc":          badCRC,
		"len too small":    rawRecord(make([]byte, 15)),
		"len too big":      tooBig,
		"zero batches":     rawRecord(rawPayload(1, 0, put...)),
		"batches too big":  rawRecord(rawPayload(1, 2, put...)),
		"batches too few":  rawRecord(rawPayload(1, 1, cat(put, del)...)),
		"zero ops":         rawRecord(rawPayload(1, 1, rawBatch(0, opDelete, 1, 'k')...)),
		"zero ops 2nd":     rawRecord(rawPayload(1, 2, cat(put, rawBatch(0))...)),
		"ops too many":     rawRecord(rawPayload(1, 1, rawBatch(2, opPut, 1, 'k', 1, 'v')...)),
		"ops too few":      rawRecord(rawPayload(1, 1, rawBatch(1, opPut, 1, 'k', 1, 'v', opDelete, 1, 'k')...)),
		"bad kind":         rawRecord(rawPayload(1, 1, rawBatch(1, 3, 1, 'k')...)),
		"empty key":        rawRecord(rawPayload(1, 1, rawBatch(1, opDelete, 0, 0)...)),
		"key past end":     rawRecord(rawPayload(1, 1, rawBatch(1, opDelete, 9, 'k')...)),
		"value past end":   rawRecord(rawPayload(1, 1, rawBatch(1, opPut, 1, 'k', 5, 'v')...)),
		"trailing byte":    rawRecord(rawPayload(1, 1, cat(del, []byte{0})...)),
		"huge batches":     rawRecord(rawPayload(1, 0xffffffff, del...)),
		"huge op count":    rawRecord(rawPayload(1, 1, rawBatch(0xffffffff, opDelete, 1, 'k')...)),
		"truncated batch":  rawRecord(rawPayload(1, 2, cat(del, []byte{1, 0})...)),
		"second batch bad": rawRecord(rawPayload(1, 2, cat(del, rawBatch(1, 9, 1, 'k'))...)),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := decodeRecord(b); !errors.Is(err, errUnreadable) {
				t.Fatalf("err = %v, want errUnreadable", err)
			}
		})
	}
	if _, _, _, err := decodeRecord(rawRecord(rawPayload(1, 1, del...))); err != nil {
		t.Fatalf("control record: %v", err)
	}
}

type snapEntry struct {
	key, value []byte
	version    uint64
}

func iterOf(entries []snapEntry) entryIter {
	return func(yield func(key, value []byte, version uint64) bool) {
		for _, e := range entries {
			if !yield(e.key, e.value, e.version) {
				return
			}
		}
	}
}

func encodeSnapshot(t testing.TB, seq uint64, entries []snapEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, seq, uint64(len(entries)), iterOf(entries)); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSnapshotRoundTrip(t *testing.T) {
	entries := []snapEntry{
		{[]byte("a"), []byte("1"), 3},
		{[]byte("b"), nil, 7},
		{[]byte("c"), bytes.Repeat([]byte("x"), 1000), 1},
	}
	for _, in := range [][]snapEntry{nil, entries} {
		data := encodeSnapshot(t, 9, in)
		view, err := openSnapshot(data)
		if err != nil {
			t.Fatal(err)
		}
		if view.seq != 9 || view.count != uint64(len(in)) {
			t.Fatalf("view = %+v", view)
		}
		var got []snapEntry
		view.each(func(k, v []byte, ver uint64) { got = append(got, snapEntry{k, v, ver}) })
		if len(got) != len(in) {
			t.Fatalf("got %d entries, want %d", len(got), len(in))
		}
		for i := range in {
			if !bytes.Equal(got[i].key, in[i].key) || !bytes.Equal(got[i].value, in[i].value) || got[i].version != in[i].version {
				t.Fatalf("entry %d = %+v, want %+v", i, got[i], in[i])
			}
		}
	}
}

func TestSnapshotCorrupt(t *testing.T) {
	entries := []snapEntry{{[]byte("a"), []byte("1"), 1}, {[]byte("b"), []byte("2"), 2}}
	data := encodeSnapshot(t, 2, entries)
	for i := range data {
		bad := slices.Clone(data)
		bad[i] ^= 0x40
		if _, err := openSnapshot(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flip byte %d: err=%v", i, err)
		}
	}
	for _, n := range []int{0, 1, snapHeaderSize, len(data) - 1} {
		if _, err := openSnapshot(data[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncate to %d: err=%v", n, err)
		}
	}
	if _, err := openSnapshot(append(slices.Clone(data), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing byte: err=%v", err)
	}
}

// forgeSnapshot builds a snapshot with valid checksums around arbitrary body
// bytes, to reach the structural checks.
func forgeSnapshot(seq, count uint64, body []byte) []byte {
	b := append([]byte(snapshotMagic), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(b[8:], seq)
	binary.LittleEndian.PutUint64(b[16:], count)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	b = append(b, body...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(body, castagnoli))
}

func TestSnapshotStructure(t *testing.T) {
	entry := func(k string, ver uint64, v string) []byte {
		b := binary.AppendUvarint(nil, uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, ver)
		b = binary.AppendUvarint(b, uint64(len(v)))
		return append(b, v...)
	}
	cases := map[string][]byte{
		"not ascending":  forgeSnapshot(5, 2, cat(entry("b", 1, ""), entry("a", 1, ""))),
		"duplicate key":  forgeSnapshot(5, 2, cat(entry("a", 1, ""), entry("a", 2, ""))),
		"version > seq":  forgeSnapshot(5, 1, entry("a", 6, "")),
		"too few":        forgeSnapshot(5, 2, entry("a", 1, "")),
		"too many":       forgeSnapshot(5, 1, cat(entry("a", 1, ""), entry("b", 1, ""))),
		"empty key":      forgeSnapshot(5, 1, entry("", 1, "")),
		"huge count":     forgeSnapshot(5, 1<<62, entry("a", 1, "")),
		"value past end": forgeSnapshot(5, 1, []byte{1, 'a', 1, 9, 'v'}),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := openSnapshot(b); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
	if _, err := openSnapshot(forgeSnapshot(5, 2, cat(entry("a", 1, ""), entry("b", 5, "x")))); err != nil {
		t.Fatalf("control snapshot: %v", err)
	}
}

func TestWriteSnapshotRejectsBadInput(t *testing.T) {
	long := bytes.Repeat([]byte("k"), maxKeyLen+1)
	cases := map[string]struct {
		count   uint64
		entries []snapEntry
	}{
		"unsorted":     {2, []snapEntry{{[]byte("b"), nil, 1}, {[]byte("a"), nil, 1}}},
		"duplicate":    {2, []snapEntry{{[]byte("a"), nil, 1}, {[]byte("a"), nil, 1}}},
		"empty key":    {1, []snapEntry{{nil, nil, 1}}},
		"long key":     {1, []snapEntry{{long, nil, 1}}},
		"version":      {1, []snapEntry{{[]byte("a"), nil, 4}}},
		"too few":      {2, []snapEntry{{[]byte("a"), nil, 1}}},
		"too many":     {1, []snapEntry{{[]byte("a"), nil, 1}, {[]byte("b"), nil, 1}}},
		"unsorted mid": {3, []snapEntry{{[]byte("a"), nil, 1}, {[]byte("c"), nil, 1}, {[]byte("b"), nil, 1}}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeSnapshot(&buf, 3, c.count, iterOf(c.entries)); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func FuzzDecodeRecord(f *testing.F) {
	f.Add(appendRecord(nil, 1, sampleOps()))
	f.Add(appendRecord(nil, 1, sampleOps(), sampleOps()[:2]))
	f.Add(appendRecord(nil, 1<<40, []op{{kind: opDelete, key: []byte("k")}}))
	f.Add(rawRecord(rawPayload(1, 1, rawBatch(1, opDelete, 1, 'k')...)))
	f.Add([]byte{})
	f.Add(make([]byte, 64))
	f.Add(appendLogHeader(nil, 7))
	f.Add(encodeSnapshot(f, 3, []snapEntry{{[]byte("a"), []byte("1"), 2}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		// Decoders must not panic, whatever the input.
		_, _ = decodeLogHeader(b)
		if view, err := openSnapshot(b); err == nil {
			view.each(func(k, v []byte, ver uint64) {})
		}
		peekRecord(b)
		seq, batches, n, err := decodeRecord(b)
		if err != nil {
			return
		}
		// A readable record survives re-encoding (varints may be
		// non-canonical in the input, so the bytes can differ).
		if n > len(b) || len(batches) == 0 {
			t.Fatalf("n=%d len=%d batches=%d", n, len(b), len(batches))
		}
		for _, ops := range batches {
			if len(ops) == 0 {
				t.Fatal("readable record with an empty batch")
			}
		}
		enc := appendRecord(nil, seq, batches...)
		seq2, batches2, n2, err := decodeRecord(enc)
		if err != nil || seq2 != seq || n2 != len(enc) || len(batches2) != len(batches) {
			t.Fatalf("round trip failed: err=%v", err)
		}
		for i := range batches {
			if !opsEqual(batches[i], batches2[i]) {
				t.Fatalf("batch %d differs after round trip", i)
			}
		}
	})
}
