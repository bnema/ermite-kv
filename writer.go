// SPDX-License-Identifier: MIT

package kv

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// maxGroupBytes caps the encoded payload of one commit group. A single batch
// larger than this forms a group of one.
const maxGroupBytes = 4 << 20

// request is a batch waiting for the writer. The writer answers on reply
// exactly once.
type request struct {
	b     *Batch
	reply chan result // capacity 1
}

type result struct {
	seq uint64
	err error
}

var requestPool = sync.Pool{New: func() any {
	return &request{reply: make(chan result, 1)}
}}

// release returns r to the pool (it must not be referenced by the writer)
// and passes err through.
func (r *request) release(err error) error {
	r.b = nil
	requestPool.Put(r)
	return err
}

// writer is the single goroutine that owns the log file and builds the new
// state. It exits on Close or when the database fails.
func (db *DB) writer() {
	defer close(db.done)
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
		first := pending
		pending = nil
		if first == nil {
			select {
			case <-db.closing:
				return
			case first = <-db.reqCh:
			}
		}
		pending = db.collect(first)
		if !db.commit() {
			return
		}
		if !db.maybeCompact() {
			return
		}
	}
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
