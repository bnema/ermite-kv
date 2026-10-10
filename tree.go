// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"iter"
	"slices"
)

// The in-memory index is a persistent (copy-on-write) B-tree of order 32:
// a node holds at most maxEntries entries and, except for the root, at least
// minEntries. Entries live in internal nodes too.
const (
	degree     = 32
	maxEntries = 2*degree - 1
	minEntries = degree - 1
)

// entry is a key with its value and the seq of the batch that last wrote it.
// The tree never copies or modifies key and value: callers pass owned slices.
type entry struct {
	key     []byte
	value   []byte
	version uint64
}

// owner identifies one builder epoch. It has a non-zero size so that every
// new(owner) is a distinct pointer.
type owner struct{ _ byte }

// node is a B-tree node. A node without children is a leaf; otherwise it has
// len(entries)+1 children.
//
// Invariant: a node is mutated only by the builder whose current owner token
// equals node.own. freeze gives the builder a fresh token, so no live builder
// holds the token of any node reachable from a frozen tree: published nodes
// are never mutated, and a builder clones a node the first time it touches it.
type node struct {
	own      *owner
	entries  []entry
	children []*node
}

func (n *node) leaf() bool { return len(n.children) == 0 }

// find returns the index of the first entry whose key is >= key and whether
// that entry's key equals key.
func (n *node) find(key []byte) (int, bool) {
	return slices.BinarySearchFunc(n.entries, key, func(e entry, k []byte) int {
		return bytes.Compare(e.key, k)
	})
}

// lookup returns the entry stored under key, if any.
func lookup(n *node, key []byte) (entry, bool) {
	for n != nil {
		i, found := n.find(key)
		if found {
			return n.entries[i], true
		}
		if n.leaf() {
			break
		}
		n = n.children[i]
	}
	return entry{}, false
}

// tree is an immutable persistent B-tree; the zero value is empty. Its methods
// never modify existing nodes, so a tree can be shared between goroutines.
type tree struct {
	root *node
	n    int
}

func (t tree) len() int { return t.n }

func (t tree) get(key []byte) (Item, bool) {
	e, ok := lookup(t.root, key)
	return Item{Value: e.value, Version: e.version}, ok
}

// put returns a tree where key maps to value at version; t is untouched.
func (t tree) put(key, value []byte, version uint64) tree {
	b := t.builder()
	b.put(key, value, version)
	return b.freeze()
}

// delete returns a tree without key; t is untouched.
func (t tree) delete(key []byte) tree {
	b := t.builder()
	b.delete(key)
	return b.freeze()
}

// ascend yields the entries with start <= key < end in ascending order. An
// empty start and a nil end are unbounded. It walks with an explicit stack and
// does not allocate per yielded item.
func (t tree) ascend(start, end []byte) iter.Seq2[[]byte, Item] {
	return func(yield func([]byte, Item) bool) {
		// A frame's i is the index of the next entry of n to yield; the
		// child left of that entry has already been visited or skipped.
		type frame struct {
			n *node
			i int
		}
		var buf [16]frame
		stack := buf[:0]

		// Seek to the first entry >= start.
		for n := t.root; n != nil; {
			i, found := n.find(start)
			stack = append(stack, frame{n, i})
			if found || n.leaf() {
				break
			}
			n = n.children[i]
		}

		for len(stack) > 0 {
			f := &stack[len(stack)-1]
			if f.i >= len(f.n.entries) {
				stack = stack[:len(stack)-1]
				continue
			}
			n := f.n
			e := &n.entries[f.i]
			f.i++
			if end != nil && bytes.Compare(e.key, end) >= 0 {
				return
			}
			if !yield(e.key, Item{Value: e.value, Version: e.version}) {
				return
			}
			// Descend to the leftmost leaf of the subtree right of e.
			for c := n; !c.leaf(); {
				c = c.children[f.i]
				stack = append(stack, frame{c, 0})
				f = &stack[len(stack)-1]
			}
		}
	}
}

// builder applies many mutations to a tree while copying each touched node at
// most once. It is not safe for concurrent use. Trees returned by freeze are
// immutable and independent of later builder operations.
type builder struct {
	root *node
	n    int
	own  *owner
}

// builder returns a builder starting from t; t is untouched.
func (t tree) builder() *builder {
	return &builder{root: t.root, n: t.n, own: new(owner)}
}

// freeze returns the current state as an immutable tree. The builder remains
// usable: it switches to a new owner token, so nodes of the returned tree are
// cloned before any further mutation.
func (b *builder) freeze() tree {
	b.own = new(owner)
	return tree{root: b.root, n: b.n}
}

func (b *builder) len() int { return b.n }

func (b *builder) get(key []byte) (Item, bool) {
	e, ok := lookup(b.root, key)
	return Item{Value: e.value, Version: e.version}, ok
}

// mutable returns n if this builder owns it, else an owned shallow copy.
func (b *builder) mutable(n *node) *node {
	if n.own == b.own {
		return n
	}
	c := &node{own: b.own, entries: slices.Clone(n.entries)}
	if len(n.children) > 0 {
		c.children = slices.Clone(n.children)
	}
	return c
}

// mutableChild makes n.children[i] owned and returns it. n must be owned.
func (b *builder) mutableChild(n *node, i int) *node {
	c := b.mutable(n.children[i])
	n.children[i] = c
	return c
}

// split cuts the owned node n at entry i: n keeps entries[:i], the returned
// entry is entries[i], and the returned node holds the rest.
func (b *builder) split(n *node, i int) (entry, *node) {
	mid := n.entries[i]
	r := &node{own: b.own, entries: slices.Clone(n.entries[i+1:])}
	clear(n.entries[i:])
	n.entries = n.entries[:i]
	if !n.leaf() {
		r.children = slices.Clone(n.children[i+1:])
		clear(n.children[i+1:])
		n.children = n.children[:i+1]
	}
	return mid, r
}

// put sets key to value at version, replacing any existing entry.
func (b *builder) put(key, value []byte, version uint64) {
	e := entry{key: key, value: value, version: version}
	if b.root == nil {
		b.root = &node{own: b.own, entries: []entry{e}}
		b.n = 1
		return
	}
	b.root = b.mutable(b.root)
	if len(b.root.entries) >= maxEntries {
		mid, r := b.split(b.root, maxEntries/2)
		b.root = &node{
			own:      b.own,
			entries:  []entry{mid},
			children: []*node{b.root, r},
		}
	}
	if !b.insert(b.root, e) {
		b.n++
	}
}

// insert adds e below the owned, non-full node n and reports whether it
// replaced an existing entry. Full children are split on the way down.
func (b *builder) insert(n *node, e entry) (replaced bool) {
	for {
		i, found := n.find(e.key)
		if found {
			n.entries[i] = e
			return true
		}
		if n.leaf() {
			n.entries = slices.Insert(n.entries, i, e)
			return false
		}
		c := b.mutableChild(n, i)
		if len(c.entries) >= maxEntries {
			mid, r := b.split(c, maxEntries/2)
			n.entries = slices.Insert(n.entries, i, mid)
			n.children = slices.Insert(n.children, i+1, r)
			switch cmp := bytes.Compare(e.key, mid.key); {
			case cmp == 0:
				n.entries[i] = e
				return true
			case cmp > 0:
				c = r
			}
		}
		n = c
	}
}

// delete removes key if present.
func (b *builder) delete(key []byte) {
	if _, ok := lookup(b.root, key); !ok {
		return
	}
	b.root = b.mutable(b.root)
	b.remove(b.root, key, false)
	b.n--
	switch {
	case len(b.root.entries) > 0:
	case b.root.leaf():
		b.root = nil
	default:
		b.root = b.root.children[0]
	}
}

// remove deletes key (or, with max, the largest entry) from the subtree of the
// owned node n and returns the removed entry. The key must exist. Every child
// descended into is first guaranteed to hold more than minEntries entries.
func (b *builder) remove(n *node, key []byte, max bool) entry {
	for {
		var i int
		var found bool
		switch {
		case max && n.leaf():
			i, found = len(n.entries)-1, true
		case max:
			i = len(n.entries)
		default:
			i, found = n.find(key)
		}
		if n.leaf() {
			e := n.entries[i]
			n.entries = slices.Delete(n.entries, i, i+1)
			return e
		}
		if len(n.children[i].entries) <= minEntries {
			b.growChild(n, i)
			continue // the shape of n changed: search again
		}
		c := b.mutableChild(n, i)
		if found {
			// Replace the entry by its predecessor.
			e := n.entries[i]
			n.entries[i] = b.remove(c, nil, true)
			return e
		}
		n = c
	}
}

// growChild ensures n.children[i] has more than minEntries entries by
// borrowing from a sibling or merging with one. n must be owned.
func (b *builder) growChild(n *node, i int) {
	switch {
	case i > 0 && len(n.children[i-1].entries) > minEntries:
		c := b.mutableChild(n, i)
		l := b.mutableChild(n, i-1)
		last := len(l.entries) - 1
		c.entries = slices.Insert(c.entries, 0, n.entries[i-1])
		n.entries[i-1] = l.entries[last]
		l.entries[last] = entry{}
		l.entries = l.entries[:last]
		if !c.leaf() {
			lc := len(l.children) - 1
			c.children = slices.Insert(c.children, 0, l.children[lc])
			l.children[lc] = nil
			l.children = l.children[:lc]
		}
	case i < len(n.entries) && len(n.children[i+1].entries) > minEntries:
		c := b.mutableChild(n, i)
		r := b.mutableChild(n, i+1)
		c.entries = append(c.entries, n.entries[i])
		n.entries[i] = r.entries[0]
		r.entries = slices.Delete(r.entries, 0, 1)
		if !c.leaf() {
			c.children = append(c.children, r.children[0])
			r.children = slices.Delete(r.children, 0, 1)
		}
	default:
		// Merge children i and i+1 around entry i (children i-1 and i
		// around entry i-1 for the last child).
		if i == len(n.entries) {
			i--
		}
		c := b.mutableChild(n, i)
		r := n.children[i+1] // only read
		c.entries = append(c.entries, n.entries[i])
		c.entries = append(c.entries, r.entries...)
		c.children = append(c.children, r.children...)
		n.entries = slices.Delete(n.entries, i, i+1)
		n.children = slices.Delete(n.children, i+1, i+2)
	}
}
