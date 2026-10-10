// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestBatchCopiesInputs(t *testing.T) {
	var b Batch
	key, val := []byte("key"), []byte("value")
	b.Put(key, val)
	b.Delete(key)
	b.RequireAbsent(key)
	b.RequireVersion(key, 3)
	key[0], val[0] = 'X', 'X'
	if got := b.ops[0]; string(got.key) != "key" || string(got.value) != "value" {
		t.Fatalf("put aliases its inputs: %q=%q", got.key, got.value)
	}
	if got := b.ops[1]; string(got.key) != "key" {
		t.Fatalf("delete aliases its key: %q", got.key)
	}
	for i, p := range b.pre {
		if string(p.key) != "key" {
			t.Fatalf("precondition %d aliases its key: %q", i, p.key)
		}
	}
	if b.Len() != 4 {
		t.Fatalf("Len = %d, want 4", b.Len())
	}
}

func TestBatchErrors(t *testing.T) {
	long := bytes.Repeat([]byte("k"), maxKeyLen+1)
	cases := map[string]func(b *Batch){
		"put empty":      func(b *Batch) { b.Put(nil, []byte("v")) },
		"put long":       func(b *Batch) { b.Put(long, nil) },
		"delete empty":   func(b *Batch) { b.Delete([]byte{}) },
		"absent empty":   func(b *Batch) { b.RequireAbsent(nil) },
		"version empty":  func(b *Batch) { b.RequireVersion(nil, 1) },
		"version zero":   func(b *Batch) { b.RequireVersion([]byte("k"), 0) },
		"after valid op": func(b *Batch) { b.Put([]byte("a"), nil); b.Put(nil, nil); b.Put([]byte("b"), nil) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			var b Batch
			fn(&b)
			if !errors.Is(b.err, ErrInvalidKey) {
				t.Fatalf("err = %v, want ErrInvalidKey", b.err)
			}
			b.Reset()
			if b.err != nil || b.Len() != 0 || b.payload() != 0 {
				t.Fatalf("Reset left err=%v len=%d payload=%d", b.err, b.Len(), b.payload())
			}
		})
	}

	var b Batch
	b.Put(bytes.Repeat([]byte("k"), maxKeyLen), nil) // exactly the limit
	if b.err != nil {
		t.Fatalf("64 KiB key rejected: %v", b.err)
	}
}

func TestBatchPayload(t *testing.T) {
	var b Batch
	if b.payload() != 0 {
		t.Fatal("empty batch has a payload")
	}
	b.RequireAbsent([]byte("k"))
	if b.payload() != 0 {
		t.Fatal("precondition-only batch has a payload")
	}
	b.Put([]byte("k"), []byte(strings.Repeat("v", 200)))
	b.Delete([]byte("kk"))
	want := len(appendRecord(nil, 1, b.ops)) - recordFrame
	if got := b.payload(); got != want {
		t.Fatalf("payload = %d, encoded record payload = %d", got, want)
	}
}
