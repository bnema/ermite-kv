// SPDX-License-Identifier: MIT

package kv

import (
	"context"
	"iter"
)

// Item is the value stored under a key.
type Item struct {
	Value   []byte // owned by the snapshot: must not be modified
	Version uint64 // seq of the batch that last wrote the key
}

// Snapshot is an immutable, consistent view of the database at one seq. It is
// safe for concurrent use and stays valid, unchanged, after later writes.
// Keys yielded by iterators and Item.Value are owned by the snapshot and must
// not be modified.
type Snapshot struct {
	t   tree
	seq uint64
}

func newSnapshot(t tree, seq uint64) *Snapshot {
	return &Snapshot{t: t, seq: seq}
}

// Seq returns the seq of the last batch applied in this snapshot.
func (s *Snapshot) Seq() uint64 { return s.seq }

// Len returns the number of keys.
func (s *Snapshot) Len() int { return s.t.len() }

// Get returns the item stored under key. Reads never block or log: ctx is
// not checked.
func (s *Snapshot) Get(_ context.Context, key []byte) (Item, bool) { return s.t.get(key) }

// Range iterates over the keys in [start, end) in ascending order. A nil end
// (or an empty start) is unbounded. Breaking out of the loop stops the walk.
func (s *Snapshot) Range(_ context.Context, start, end []byte) iter.Seq2[[]byte, Item] {
	return s.t.ascend(start, end)
}

// Prefix iterates over the keys that begin with p in ascending order. An
// empty p matches every key.
func (s *Snapshot) Prefix(_ context.Context, p []byte) iter.Seq2[[]byte, Item] {
	return s.t.ascend(p, prefixEnd(p))
}

// prefixEnd returns the smallest key greater than every key that begins with
// p, or nil if there is none (p is empty or all 0xFF).
func prefixEnd(p []byte) []byte {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] < 0xFF {
			end := make([]byte, i+1)
			copy(end, p)
			end[i]++
			return end
		}
	}
	return nil
}
