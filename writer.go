// SPDX-License-Identifier: MIT

package kv

import (
	"fmt"
	"io"
	"time"
)

// maxGroupBytes caps the encoded payload of one commit group. A single batch
// larger than this forms a group of one.
const maxGroupBytes = 4 << 20

// coldChunk is the number of cold candidates the writer repoints between two
// commit groups.
const coldChunk = 1024

// coldEvery is the number of commit groups after which pending cold work
// gets a chunk even if requests are waiting, so that it completes under a
// constant write load.
const coldEvery = 16

// request is a batch waiting for the writer. The writer answers on reply
// exactly once. Requests are allocated per Apply and never pooled: a pooled
// channel would be shared across testing/synctest bubbles, and receiving on a
// bubbled channel from outside its bubble is fatal.
type request struct {
	b     *Batch
	reply chan result // capacity 1
}

type result struct {
	seq uint64
	err error
}

// writer is the single goroutine that owns the log file and builds the new
// state. It exits on Close or when the database fails.
func (db *DB) writer() {
	defer close(db.done)
	defer func() {
		// Drop the pins: after Close the mappings live as long as the
		// snapshots and items that reference them.
		db.pins = pins{}
		db.cold = nil
	}()
	var pending *request // dequeued but did not fit in the previous group
	defer func() {
		// A request taken but never grouped must still be answered.
		if pending != nil {
			err := db.stopped()
			if err == nil {
				err = ErrClosed // not reachable: the writer only exits on close or failure
			}
			pending.reply <- result{err: err}
		}
	}()
	for {
		// Closing takes priority over queued requests.
		select {
		case <-db.closing:
			return
		default:
		}
		if db.cold != nil && db.coldGroups >= coldEvery {
			db.applyColdChunk()
			continue
		}
		first := pending
		pending = nil
		if first == nil {
			// Requests go first; pending cold work runs when none is
			// ready, or after coldEvery groups. A new cold job replaces the pending one: it comes
			// from a newer snapshot.
			var ok bool
			if first, ok = db.next(); !ok {
				return
			}
			if first == nil {
				continue // cold work was received or applied
			}
		}
		pending = db.collect(first)
		if !db.commit() {
			return
		}
		db.coldGroups++
		if !db.maybeCompact() {
			return
		}
	}
}

// next waits for the next request. Without cold work it blocks; with cold
// work pending it applies one chunk when no request is ready. It returns a
// nil request when it handled cold work instead, and false on close.
func (db *DB) next() (*request, bool) {
	if db.cold == nil {
		select {
		case <-db.closing:
			return nil, false
		case r := <-db.reqCh:
			return r, true
		case w := <-db.coldCh:
			db.takeCold(w)
			return nil, true
		}
	}
	select {
	case <-db.closing:
		return nil, false
	case r := <-db.reqCh:
		return r, true
	case w := <-db.coldCh:
		db.takeCold(w)
		return nil, true
	default:
		db.applyColdChunk()
		return nil, true
	}
}

// takeCold makes w the pending cold job. It replaces a pending job, whose
// mapping stays pinned if the tree already references it.
func (db *DB) takeCold(w *coldWork) {
	if db.cold != nil && db.coldPublished {
		db.pins.superseded(db.cold.m)
	}
	db.cold, db.coldNext, db.coldPublished = w, 0, false
}

// applyColdChunk repoints up to coldChunk candidates of db.cold to the
// mapping and publishes the result with the same seq: readers see the same
// bytes. A candidate whose key was rewritten or deleted since the snapshot
// no longer matches its version and is skipped. It runs on the writer
// between commit groups.
func (db *DB) applyColdChunk() {
	w := db.cold
	end := min(db.coldNext+coldChunk, len(w.cands))
	cur := db.snap.Load()
	bld := cur.t.builder()
	changed := false
	for _, c := range w.cands[db.coldNext:end] {
		if bld.setValue(c.key, c.version, c.value, w.m) {
			changed = true
		}
	}
	if changed {
		db.snap.Store(newSnapshot(bld.freeze(), cur.seq))
		db.coldPublished = true
	}
	db.coldNext, db.coldGroups = end, 0
	db.coldChunks.Add(1)
	if end == len(w.cands) {
		db.coldSwapped()
	}
}

// coldSwapped ends the cold job: every candidate has been repointed and its
// mapping becomes the current generation.
func (db *DB) coldSwapped() {
	db.pins.swapped(db.cold.m)
	db.cold, db.coldNext, db.coldPublished = nil, 0, false
	db.coldJobs.Add(1)
}

// pins keeps snapshot mappings alive after the tree stops referencing them.
//
// A reader may keep a bare value slice (item.Value) without the Item or the
// Snapshot, and Go does not keep the Item alive for it: such a value is only
// protected by the references the published trees hold to its mapping. So
// a mapping referenced by a published snapshot stays pinned until one full
// swap has completed after it stopped being the current generation:
//   - gen is the current generation: the mapping Open kept values in, then
//     the mapping of each completed swap;
//   - partial holds the mappings of superseded cold jobs that had already
//     published a chunk, since the tree references them;
//   - pinned holds the previous generation and the partial mappings it was
//     swapped out with. The next completed swap releases them.
//
// Owned by the writer goroutine; dropped when the writer exits.
type pins struct {
	gen     *mapping
	partial []*mapping
	pinned  []*mapping
}

// superseded records that the job of m was replaced after publishing a chunk.
func (p *pins) superseded(m *mapping) { p.partial = append(p.partial, m) }

// swapped makes m the current generation once its swap is complete: the
// previous generation and the partial mappings stay pinned until the next
// swap, anything pinned before is released.
func (p *pins) swapped(m *mapping) {
	p.pinned = p.pinned[:0:0]
	if p.gen != nil {
		p.pinned = append(p.pinned, p.gen)
	}
	p.pinned = append(p.pinned, p.partial...)
	p.gen, p.partial = m, nil
}

// collect fills db.group starting with first: it drains the queued requests
// and, with GroupWindow > 0, waits for more until the window expires or the
// database is closing. It returns a request that was taken but did not fit
// under maxGroupBytes.
func (db *DB) collect(first *request) (overflow *request) {
	db.group = append(db.group[:0], first)
	size := recordHead + first.b.encodedSize()
	var expired <-chan time.Time
	if w := db.opts.GroupWindow; w > 0 {
		t := time.NewTimer(w)
		defer t.Stop()
		expired = t.C
	}
	for {
		var r *request
		if expired == nil {
			select {
			case r = <-db.reqCh:
			default:
				return nil
			}
		} else {
			select {
			case r = <-db.reqCh:
			case <-expired:
				return nil
			case <-db.closing:
				return nil
			}
		}
		if p := r.b.encodedSize(); size+p <= maxGroupBytes {
			db.group = append(db.group, r)
			size += p
		} else {
			return r
		}
	}
}

// commit applies db.group to a fresh builder, writes and syncs the records,
// publishes the new state and answers every request. It reports false if the
// database failed.
func (db *DB) commit() bool {
	defer clear(db.group)
	cur := db.snap.Load()
	bld := cur.t.builder()
	seq := cur.seq
	buf := db.buf[:0]
	results := make([]result, len(db.group))
	var batches int
	var firstSeq uint64

	for i, r := range db.group {
		b := r.b
		if !checkPreconditions(bld, b.pre) {
			results[i] = result{err: ErrConflict}
			continue
		}
		if len(b.ops) == 0 {
			results[i] = result{seq: seq}
			continue
		}
		seq++
		for _, o := range b.ops {
			if o.kind == opPut {
				bld.put(o.key, o.value, seq)
			} else {
				bld.delete(o.key)
			}
		}
		if batches == 0 {
			firstSeq = seq
			buf = beginRecord(buf, seq)
		}
		buf = appendBatch(buf, b.ops)
		batches++
		results[i] = result{seq: seq}
	}
	db.buf = buf[:0]
	if cap(db.buf) > 2*maxGroupBytes {
		db.buf = nil // do not pin the memory of one huge batch
	}

	if batches > 0 {
		finishRecord(buf, batches)
		syncTime, err := db.persist(buf)
		if err != nil {
			err = db.fail(err)
			for _, r := range db.group {
				r.reply <- result{err: err}
			}
			return false
		}
		db.logSize += int64(len(buf))
		db.snap.Store(newSnapshot(bld.freeze(), seq))
		db.log.Debug().Uint64("seq", firstSeq).Int("batches", batches).Int("bytes", len(buf)).
			Dur("sync", syncTime).Msg("group committed")
	}
	for i, r := range db.group {
		r.reply <- results[i]
	}
	return true
}

// persist appends buf to the log and syncs it (unless NoSync). It returns
// the time spent in the sync.
func (db *DB) persist(buf []byte) (time.Duration, error) {
	n, err := db.file.Write(buf)
	if err == nil && n < len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, fmt.Errorf("write log: %w", err)
	}
	if db.opts.NoSync {
		return 0, nil
	}
	start := time.Now()
	if err := db.file.Sync(); err != nil {
		return 0, fmt.Errorf("sync log: %w", err)
	}
	return time.Since(start), nil
}

// fail puts the database in the failed state, keeping the first cause, and
// returns the error that requests receive.
func (db *DB) fail(cause error) error {
	db.failOnce.Do(func() {
		db.failErr = fmt.Errorf("%w: %w", ErrFailed, cause)
		db.log.Error().Err(cause).Msg("database failed")
		close(db.failed)
	})
	return db.failErr
}

// checkPreconditions reports whether all preconditions hold on bld.
func checkPreconditions(bld *builder, pre []precond) bool {
	for _, p := range pre {
		item, ok := bld.get(p.key)
		if p.absent && ok || !p.absent && (!ok || item.Version != p.version) {
			return false
		}
	}
	return true
}
