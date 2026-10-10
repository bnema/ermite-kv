// SPDX-License-Identifier: MIT

package kv

// Batch is an ordered set of mutations and preconditions that Apply commits
// atomically. The zero value is ready to use. A Batch must not be modified
// while Apply runs and is not safe for concurrent use.
//
// Batch methods do not return errors: the batch records its first error and
// Apply returns it.
type Batch struct {
	ops      []op
	pre      []precond
	opsBytes int // encoded size of ops
	err      error
}

// precond is a precondition checked by the writer before any op.
type precond struct {
	key     []byte
	absent  bool   // the key must not exist
	version uint64 // otherwise the key must exist at this version
}

// Put sets key to value. It copies key and value.
func (b *Batch) Put(key, value []byte) {
	if !b.validKey(key) {
		return
	}
	// One allocation holds both; the tree retains the slices, so they are
	// never reused (not even by Reset).
	buf := make([]byte, len(key)+len(value))
	n := copy(buf, key)
	copy(buf[n:], value)
	b.add(op{kind: opPut, key: buf[:n:n], value: buf[n:len(buf):len(buf)]})
}

// Delete removes key. It copies key. Deleting a missing key is not an error.
func (b *Batch) Delete(key []byte) {
	if !b.validKey(key) {
		return
	}
	b.add(op{kind: opDelete, key: append([]byte(nil), key...)})
}

// RequireAbsent adds the precondition that key does not exist. If it does
// not hold when the batch is committed, Apply returns ErrConflict.
func (b *Batch) RequireAbsent(key []byte) {
	if !b.validKey(key) {
		return
	}
	b.pre = append(b.pre, precond{key: append([]byte(nil), key...), absent: true})
}

// RequireVersion adds the precondition that key exists at version v, the seq
// of the batch that last wrote it. v must be greater than zero, otherwise the
// batch records ErrInvalidKey.
func (b *Batch) RequireVersion(key []byte, v uint64) {
	if !b.validKey(key) {
		return
	}
	if v == 0 {
		b.fail(ErrInvalidKey)
		return
	}
	b.pre = append(b.pre, precond{key: append([]byte(nil), key...), version: v})
}

// Len returns the number of ops and preconditions.
func (b *Batch) Len() int { return len(b.ops) + len(b.pre) }

// Reset empties the batch, clears its error and keeps allocated capacity for
// the op list.
func (b *Batch) Reset() {
	clear(b.ops)
	b.ops = b.ops[:0]
	clear(b.pre)
	b.pre = b.pre[:0]
	b.opsBytes = 0
	b.err = nil
}

// payload returns the encoded record payload size; 0 for a batch without
// ops, which writes nothing.
func (b *Batch) payload() int {
	if len(b.ops) == 0 {
		return 0
	}
	return recordMinBody + b.opsBytes
}

func (b *Batch) add(o op) {
	b.ops = append(b.ops, o)
	b.opsBytes += opSize(o)
}

func (b *Batch) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// validKey records ErrInvalidKey and reports false for an invalid key.
func (b *Batch) validKey(key []byte) bool {
	if len(key) == 0 || len(key) > maxKeyLen {
		b.fail(ErrInvalidKey)
		return false
	}
	return b.err == nil
}
