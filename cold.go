// SPDX-License-Identifier: MIT

package kv

import (
	"fmt"

	"bnema.dev/ermite-kv/internal/fsys"
)

// Cold values: values of at least Options.ColdValueBytes served from the
// mapped snapshot file. Open keeps them mapped (see open); after each
// compaction the compactor collects the large values of the new snapshot
// (collectCold) and hands them to the writer (sendCold), which repoints the
// tree entries to the new mapping in chunks between commit groups.

// coldChunk is the number of cold candidates the writer repoints between two
// commit groups.
const coldChunk = 1024

// coldEvery is the number of commit groups after which pending cold work
// gets a chunk even if requests are waiting, so that it completes under a
// constant write load.
const coldEvery = 16

// coldState is the cold-value state of the writer. Only the writer goroutine
// touches it.
type coldState struct {
	job       *coldWork // being applied, nil if none
	next      int       // index of the next candidate of job
	groups    int       // commit groups since the last chunk
	published bool      // a chunk of job was published: the tree references job.m
	pins      pins
}

// coldWork is the set of values of a new snapshot file that the writer can
// serve from its mapping instead of the heap.
type coldWork struct {
	m     *mapping
	cands []coldCand
}

// coldCand is one large value of the snapshot. key and value alias m.data:
// the writer keeps the tree's existing heap key (setValue) and only uses key
// to find the entry.
type coldCand struct {
	key     []byte
	version uint64
	value   []byte
}

// collectCold maps the synced snapshot file f, stored at path, and collects its
// values of at least Options.ColdValueBytes. Without such a value it returns
// an empty job (nil m), which still advances the pins. It returns nil when
// the option is disabled or on failure, which only logs a warning: the
// compaction itself has succeeded.
func (db *DB) collectCold(f fsys.File, path string, seq uint64) *coldWork {
	if db.opts.ColdValueBytes < 0 {
		return nil
	}
	data, unmap, err := f.Map()
	if err != nil {
		db.log.Warn().Err(err).Str("file", path).Msg("cannot map new snapshot")
		return nil
	}
	view, err := openSnapshot(data)
	if err == nil && view.seq != seq {
		err = fmt.Errorf("%w: snapshot seq %d, want %d", ErrCorrupt, view.seq, seq)
	}
	if err != nil {
		_ = unmap()
		db.log.Warn().Err(err).Str("file", path).Msg("new snapshot does not validate once mapped")
		return nil
	}
	var cands []coldCand
	view.each(func(key, value []byte, version uint64) {
		if len(value) >= db.opts.ColdValueBytes {
			cands = append(cands, coldCand{key: key, version: version, value: value})
		}
	})
	if len(cands) == 0 {
		// A job without candidates still advances the pins: the previous
		// generations, whose files compaction removed, get released.
		_ = unmap()
		return &coldWork{}
	}
	return &coldWork{m: newMapping(data, unmap), cands: cands}
}

// sendCold hands w to the writer through the one-slot db.coldCh. A job still
// waiting in the slot is dropped first: w comes from a newer snapshot and
// supersedes it. Only the compactor sends and at most one compactor runs, so
// the slot is free after the drain and the send never blocks, even once the
// writer has exited.
func (db *DB) sendCold(w *coldWork) {
	select {
	case <-db.coldCh:
	default:
	}
	select {
	case db.coldCh <- w:
	default: // unreachable while a single compactor sends
	}
}

// next waits for the next request. Without cold work it blocks; with cold
// work pending it applies one chunk when no request is ready. It returns a
// nil request when it handled cold work instead, and false on close.
func (db *DB) next() (*request, bool) {
	if db.cold.job == nil {
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
	if db.cold.job != nil && db.cold.published {
		db.cold.pins.superseded(db.cold.job.m)
	}
	db.cold.job, db.cold.next, db.cold.published, db.cold.groups = w, 0, false, 0
}

// applyColdChunk repoints up to coldChunk candidates of db.cold.job to the
// mapping and publishes the result with the same seq: readers see the same
// bytes. A candidate whose key was rewritten or deleted since the snapshot
// no longer matches its version and is skipped. It runs on the writer
// between commit groups. A job without candidates (m nil) completes at once.
func (db *DB) applyColdChunk() {
	w := db.cold.job
	end := min(db.cold.next+coldChunk, len(w.cands))
	cur := db.snap.Load()
	bld := cur.t.builder()
	changed := false
	for _, c := range w.cands[db.cold.next:end] {
		if bld.setValue(c.key, c.version, c.value, w.m) {
			changed = true
		}
	}
	if changed {
		db.snap.Store(newSnapshot(bld.freeze(), cur.seq))
		db.cold.published = true
	}
	db.cold.next, db.cold.groups = end, 0
	db.coldChunks.Add(1)
	if end == len(w.cands) {
		db.coldSwapped()
	}
}

// coldSwapped ends the cold job: every candidate has been repointed and its
// mapping (nil for a job without candidates) becomes the current generation.
func (db *DB) coldSwapped() {
	db.cold.pins.swapped(db.cold.job.m)
	db.cold.job, db.cold.next, db.cold.published = nil, 0, false
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
// A value read while a mapping is current is released at the earliest by the
// second swap after the read, which comes at the earliest from the first
// compaction that completes after the read: the lifetime documented on
// Item.Value.
//
// Owned by the writer goroutine. The pins stay after the writer exits (Close
// or failure) and live as long as the DB is reachable, since no swap can
// release them anymore.
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
	p.pinned = nil // a [:0:0] slice would still hold the old backing array
	if p.gen != nil {
		p.pinned = append(p.pinned, p.gen)
	}
	p.pinned = append(p.pinned, p.partial...)
	p.gen, p.partial = m, nil
}
