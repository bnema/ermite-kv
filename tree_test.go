// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

// model is the reference implementation: a map plus a sort on demand.
type model map[string]Item

func (m model) clone() model {
	c := make(model, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// sorted returns the keys of m in ascending order.
func (m model) sorted() []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

// within returns the part of the sorted keys in [start, end); nil end = unbounded.
func within(sorted []string, start, end []byte) []string {
	lo, _ := slices.BinarySearch(sorted, string(start))
	hi := len(sorted)
	if end != nil {
		hi, _ = slices.BinarySearch(sorted, string(end))
	}
	if hi < lo {
		return nil
	}
	return sorted[lo:hi]
}

type kvPair struct {
	key  string
	item Item
}

func collect(seq func(func([]byte, Item) bool)) []kvPair {
	var out []kvPair
	seq(func(k []byte, it Item) bool {
		out = append(out, kvPair{string(k), it})
		return true
	})
	return out
}

func equalItem(a, b Item) bool {
	return a.Version == b.Version && bytes.Equal(a.Value, b.Value)
}

// checkStructure verifies the B-tree invariants: ordering, fill bounds,
// uniform leaf depth and the entry count.
func checkStructure(t *testing.T, tr tree) {
	t.Helper()
	if tr.root == nil {
		if tr.n != 0 {
			t.Fatalf("empty root but n=%d", tr.n)
		}
		return
	}
	var count, leafDepth int
	var prev []byte
	var walk func(n *node, depth int, isRoot bool)
	walk = func(n *node, depth int, isRoot bool) {
		if len(n.entries) == 0 || len(n.entries) > maxEntries || (!isRoot && len(n.entries) < minEntries) {
			t.Fatalf("node with %d entries at depth %d", len(n.entries), depth)
		}
		if n.leaf() {
			if leafDepth == 0 {
				leafDepth = depth
			} else if leafDepth != depth {
				t.Fatalf("leaves at depths %d and %d", leafDepth, depth)
			}
		} else if len(n.children) != len(n.entries)+1 {
			t.Fatalf("%d entries but %d children", len(n.entries), len(n.children))
		}
		for i, e := range n.entries {
			if !n.leaf() {
				walk(n.children[i], depth+1, false)
			}
			if prev != nil && bytes.Compare(prev, e.key) >= 0 {
				t.Fatalf("keys out of order: %x then %x", prev, e.key)
			}
			prev = e.key
			count++
		}
		if !n.leaf() {
			walk(n.children[len(n.entries)], depth+1, false)
		}
	}
	walk(tr.root, 1, true)
	if count != tr.n {
		t.Fatalf("tree has %d entries, n=%d", count, tr.n)
	}
}

// checkAgainst compares the tree with the model through get, full scan,
// random ranges and random prefixes.
func checkAgainst(t *testing.T, rng *rand.Rand, tr tree, m model, genKey func() []byte) {
	t.Helper()
	checkStructure(t, tr)
	if tr.len() != len(m) {
		t.Fatalf("len = %d, want %d", tr.len(), len(m))
	}
	s := newSnapshot(tr, 7)
	if s.Len() != len(m) || s.Seq() != 7 {
		t.Fatalf("snapshot Len/Seq = %d/%d", s.Len(), s.Seq())
	}
	sorted := m.sorted()
	verify := func(what string, got []kvPair, start, end []byte) {
		t.Helper()
		want := within(sorted, start, end)
		if len(got) != len(want) {
			t.Fatalf("%s: got %d items, want %d", what, len(got), len(want))
		}
		for i, k := range want {
			if got[i].key != k || !equalItem(got[i].item, m[k]) {
				t.Fatalf("%s: item %d = %x, want %x", what, i, got[i].key, k)
			}
		}
	}
	verify("full scan", collect(s.Range(bg, nil, nil)), nil, nil)

	for range 20 {
		k := genKey()
		got, ok := s.Get(bg, k)
		want, wok := m[string(k)]
		if ok != wok || (ok && !equalItem(got, want)) {
			t.Fatalf("Get(%x) = %v,%v want %v,%v", k, got, ok, want, wok)
		}
	}
	for k, want := range m {
		if got, ok := s.Get(bg, []byte(k)); !ok || !equalItem(got, want) {
			t.Fatalf("Get(%x) = %v,%v want %v", k, got, ok, want)
		}
		break // one arbitrary present key is enough per check
	}
	for range 10 {
		a, b := genKey(), genKey()
		if bytes.Compare(a, b) > 0 {
			a, b = b, a
		}
		verify("range", collect(s.Range(bg, a, b)), a, b)
		verify("range open end", collect(s.Range(bg, a, nil)), a, nil)
		verify("range open start", collect(s.Range(bg, nil, b)), nil, b)
	}
	for range 10 {
		p := genKey()
		p = p[:rng.IntN(len(p)+1)] // may be empty
		var want []string
		for _, k := range sorted {
			if bytes.HasPrefix([]byte(k), p) {
				want = append(want, k)
			}
		}
		got := collect(s.Prefix(bg, p))
		if len(got) != len(want) {
			t.Fatalf("prefix %x: got %d items, want %d", p, len(got), len(want))
		}
		for i, k := range want {
			if got[i].key != k {
				t.Fatalf("prefix %x: item %d = %x, want %x", p, i, got[i].key, k)
			}
		}
	}
	// Early break stops the walk.
	seen := 0
	for range s.Range(bg, nil, nil) {
		seen++
		if seen == 3 {
			break
		}
	}
	if seen != min(3, len(m)) {
		t.Fatalf("early break saw %d items", seen)
	}
}

type frozen struct {
	tr tree
	m  model
}

func TestTreeModel(t *testing.T) {
	seeds, ops := 50, 10000
	if testing.Short() {
		seeds, ops = 8, 2000
	}
	alphabet := []byte{0, 1, 2, 'a', 'b', 'c', 0x7F, 0x80, 0xFD, 0xFE, 0xFF, 'x', 'y', 'z', 0x10, 0x20}
	for seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 0xe131))
			alpha := alphabet[:2+seed%(len(alphabet)-1)]
			genKey := func() []byte {
				k := make([]byte, 1+rng.IntN(5))
				for i := range k {
					k[i] = alpha[rng.IntN(len(alpha))]
				}
				return k
			}
			var (
				tr      tree
				m       = model{}
				version uint64
				history []frozen
			)
			apply := func(del bool, k []byte, put func(k, v []byte, ver uint64), rm func(k []byte)) {
				version++
				if del {
					rm(k)
					delete(m, string(k))
					return
				}
				v := make([]byte, rng.IntN(8))
				for i := range v {
					v[i] = byte(rng.IntN(256))
				}
				put(k, v, version)
				m[string(k)] = Item{Value: v, Version: version}
			}
			for done, next := 0, 0; done < ops; {
				if rng.IntN(5) == 0 {
					// A group of ops through one builder.
					b := tr.builder()
					n := 1 + rng.IntN(40)
					for range n {
						k := genKey()
						apply(rng.IntN(3) == 0, k, b.put, b.delete)
						if got, ok := b.get(k); ok != (m[string(k)].Version != 0) || (ok && !equalItem(got, m[string(k)])) {
							t.Fatalf("seed %d: builder.get mismatch", seed)
						}
						if rng.IntN(512) == 0 {
							// Freeze mid-group and keep using the builder: the
							// frozen tree must never see later builder writes.
							history = append(history, frozen{b.freeze(), m.clone()})
						}
					}
					if b.len() != len(m) {
						t.Fatalf("seed %d: builder.len = %d, want %d", seed, b.len(), len(m))
					}
					tr = b.freeze()
					done += n
				} else {
					k := genKey()
					apply(rng.IntN(3) == 0, k, func(k, v []byte, ver uint64) { tr = tr.put(k, v, ver) }, func(k []byte) { tr = tr.delete(k) })
					done++
				}
				if done >= next {
					next = done + 500 + rng.IntN(500)
					checkAgainst(t, rng, tr, m, genKey)
					history = append(history, frozen{tr, m.clone()})
				}
			}
			checkAgainst(t, rng, tr, m, genKey)
			// Persistence: every older tree still equals its model.
			for _, h := range history {
				checkAgainst(t, rng, h.tr, h.m, genKey)
			}
			// Drain the tree in random order, with and without a builder.
			keys := m.sorted()
			rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			for i, k := range keys {
				if i%2 == 0 {
					tr = tr.delete([]byte(k))
				} else {
					b := tr.builder()
					b.delete([]byte(k))
					b.delete([]byte(k)) // absent: no-op
					tr = b.freeze()
				}
				delete(m, k)
				if i%97 == 0 {
					checkStructure(t, tr)
				}
			}
			checkAgainst(t, rng, tr, m, genKey)
			if tr.root != nil || tr.len() != 0 {
				t.Fatalf("seed %d: tree not empty after draining", seed)
			}
			// Draining must not have disturbed the older trees.
			for _, h := range history {
				checkAgainst(t, rng, h.tr, h.m, genKey)
			}
		})
	}
}

// TestBuilderReuse checks that nodes of a frozen tree are never mutated by
// later builder operations, and that nodes owned by the builder are reused.
func TestBuilderReuse(t *testing.T) {
	var base tree
	for i := range 500 {
		base = base.put(fmt.Appendf(nil, "k%04d", i), []byte("v"), 1)
	}
	b := base.builder()
	b.put([]byte("k0100"), []byte("new"), 2)
	first := b.root
	b.put([]byte("k0101"), []byte("new"), 2)
	if b.root != first {
		t.Fatal("builder copied its own root twice")
	}
	frozenTree := b.freeze()
	b.put([]byte("k0102"), []byte("again"), 3)
	if b.root == frozenTree.root {
		t.Fatal("builder mutated the root of a frozen tree")
	}
	if it, _ := frozenTree.get([]byte("k0102")); string(it.Value) != "v" {
		t.Fatalf("frozen tree changed: %q", it.Value)
	}
	if it, _ := base.get([]byte("k0100")); string(it.Value) != "v" || it.Version != 1 {
		t.Fatalf("base tree changed: %v", it)
	}
	checkStructure(t, base)
	checkStructure(t, frozenTree)
	checkStructure(t, b.freeze())
}

func TestPrefixEnd(t *testing.T) {
	tests := []struct{ in, want []byte }{
		{nil, nil},
		{[]byte{}, nil},
		{[]byte("a"), []byte("b")},
		{[]byte("ab"), []byte("ac")},
		{[]byte{'a', 0xFF}, []byte("b")},
		{[]byte{'a', 0xFF, 0xFF}, []byte("b")},
		{[]byte{0xFF}, nil},
		{[]byte{0xFF, 0xFF}, nil},
	}
	for _, tt := range tests {
		if got := prefixEnd(tt.in); !bytes.Equal(got, tt.want) || (got == nil) != (tt.want == nil) {
			t.Errorf("prefixEnd(%x) = %x, want %x", tt.in, got, tt.want)
		}
	}
}

func TestPrefixAllFF(t *testing.T) {
	var tr tree
	for i, k := range [][]byte{{0xFE}, {0xFF}, {0xFF, 0x00}, {0xFF, 0xFF, 0xFF}} {
		tr = tr.put(k, nil, uint64(i+1))
	}
	s := newSnapshot(tr, 4)
	if got := collect(s.Prefix(bg, []byte{0xFF})); len(got) != 3 {
		t.Fatalf("prefix FF: %d items, want 3", len(got))
	}
	if got := collect(s.Prefix(bg, nil)); len(got) != 4 {
		t.Fatalf("empty prefix: %d items, want 4", len(got))
	}
}

func benchTree(n int) (*Snapshot, [][]byte) {
	keys := make([][]byte, n)
	b := tree{}.builder()
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key/%08d", i)
		b.put(keys[i], []byte("value"), uint64(i+1))
	}
	return newSnapshot(b.freeze(), uint64(n)), keys
}

func BenchmarkSnapshotGet(b *testing.B) {
	s, keys := benchTree(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, ok := s.Get(bg, keys[(i*7919)%len(keys)]); !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkRange(b *testing.B) {
	s, _ := benchTree(100_000)
	b.ReportAllocs()
	for b.Loop() {
		n := 0
		for range s.Range(bg, nil, nil) {
			n++
		}
		if n != 100_000 {
			b.Fatalf("saw %d items", n)
		}
	}
}

func BenchmarkTreePut(b *testing.B) {
	_, keys := benchTree(100_000)
	b.ReportAllocs()
	var tr tree
	for i := 0; b.Loop(); i++ {
		tr = tr.put(keys[(i*7919)%len(keys)], nil, uint64(i))
	}
}

func BenchmarkBuilderPut(b *testing.B) {
	_, keys := benchTree(100_000)
	b.ReportAllocs()
	bl := tree{}.builder()
	for i := 0; b.Loop(); i++ {
		bl.put(keys[(i*7919)%len(keys)], nil, uint64(i))
		if i%64 == 63 {
			bl.freeze()
		}
	}
}

// TestTreeDeep builds a tree of several levels, then deletes ascending,
// exercising merges and borrows on internal nodes.
func TestTreeDeep(t *testing.T) {
	const n = 20000
	var tr tree
	for i := range n {
		tr = tr.put(fmt.Appendf(nil, "%06d", i), nil, 1)
	}
	checkStructure(t, tr)
	depth := 0
	for x := tr.root; x != nil; depth++ {
		if x.leaf() {
			depth++
			break
		}
		x = x.children[0]
	}
	if depth < 3 {
		t.Fatalf("depth = %d, want >= 3", depth)
	}
	full := tr
	for i := range n {
		tr = tr.delete(fmt.Appendf(nil, "%06d", i))
		if i%251 == 0 {
			checkStructure(t, tr)
		}
	}
	if tr.len() != 0 || tr.root != nil {
		t.Fatalf("tree not empty: len=%d", tr.len())
	}
	checkStructure(t, full)
}

// TestConcurrentReaders reads published snapshots without locks while one
// builder keeps writing and freezing, so -race sees any shared mutation.
func TestConcurrentReaders(t *testing.T) {
	var cur atomic.Pointer[Snapshot]
	cur.Store(newSnapshot(tree{}, 0))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := cur.Load()
				n := 0
				var prev []byte
				for k, it := range s.Range(bg, nil, nil) {
					if prev != nil && bytes.Compare(prev, k) >= 0 {
						t.Errorf("seq %d: keys out of order", s.Seq())
						return
					}
					if it.Version > s.Seq() {
						t.Errorf("seq %d: version %d from the future", s.Seq(), it.Version)
						return
					}
					prev = k
					n++
				}
				if n != s.Len() {
					t.Errorf("seq %d: ranged %d keys, Len %d", s.Seq(), n, s.Len())
					return
				}
			}
		})
	}
	rng := rand.New(rand.NewPCG(1, 2))
	b := cur.Load().t.builder()
	for seq := uint64(1); seq <= 3000; seq++ {
		for range 1 + rng.IntN(8) {
			k := fmt.Appendf(nil, "k%04d", rng.IntN(2000))
			if rng.IntN(3) == 0 {
				b.delete(k)
			} else {
				b.put(k, []byte("v"), seq)
			}
		}
		cur.Store(newSnapshot(b.freeze(), seq))
	}
	close(stop)
	wg.Wait()
}
