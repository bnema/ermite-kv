// SPDX-License-Identifier: MIT

package kv

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/zerowrap"

	"bnema.dev/ermite-kv/internal/fsys"
)

// Defaults for zero Options fields.
const (
	defaultMaxBatchBytes = 16 << 20
	defaultCompactAfter  = 64 << 20

	// maxBatchLimit is the largest encoded batch that fits in a record
	// together with the record head (seq and batch count).
	maxBatchLimit = maxRecord - recordHead
)

// Options configures Open. The zero value of every field means "default";
// Options{} is safe and durable.
type Options struct {
	// NoSync skips fdatasync on commits. For tests and benchmarks only: an
	// acknowledged batch may be lost on power failure, and a power loss can
	// leave a log that Open rejects with ErrCorrupt (the pages of one
	// unsynced group may reach the disk out of order, which recovery cannot
	// tell from damage to acknowledged data).
	NoSync bool
	// MaxBatchBytes is the encoded batch limit (op count and ops). 0 means
	// 16 MiB; it must be at most 64 MiB minus 12 bytes (the format limit).
	MaxBatchBytes int
	// CompactAfter is the number of bytes in the current log that trigger
	// compaction. 0 means 64 MiB; a negative value disables compaction.
	CompactAfter int64
	// GroupWindow is the extra time the writer waits to grow a commit
	// group. 0 means it only drains what is already queued.
	GroupWindow time.Duration
}

// resolve validates o and fills in the defaults.
func (o Options) resolve() (Options, error) {
	switch {
	case o.MaxBatchBytes < 0 || o.MaxBatchBytes > maxBatchLimit:
		return o, fmt.Errorf("kv: invalid MaxBatchBytes %d: must be in [0, %d]", o.MaxBatchBytes, maxBatchLimit)
	case o.GroupWindow < 0:
		return o, fmt.Errorf("kv: invalid GroupWindow %v: must not be negative", o.GroupWindow)
	}
	if o.MaxBatchBytes == 0 {
		o.MaxBatchBytes = defaultMaxBatchBytes
	}
	if o.CompactAfter == 0 {
		o.CompactAfter = defaultCompactAfter
	}
	return o, nil
}

// DB is an embedded, durable, ordered key-value store. All methods are safe
// for concurrent use.
type DB struct {
	fx   fsys.FS
	dir  string
	opts Options
	log  zerowrap.Logger          // captured at Open; used by the writer
	snap atomic.Pointer[Snapshot] // last published state

	reqCh   chan *request // never closed
	closing chan struct{} // closed by Close
	done    chan struct{} // closed when the writer has exited

	failed   chan struct{} // closed when the database enters the failed state
	failOnce sync.Once
	failErr  error // first cause, wrapping ErrFailed; set before failed is closed

	closeOnce sync.Once

	lock io.Closer

	compacting atomic.Bool    // a compactor goroutine is running
	compactors sync.WaitGroup // started by the writer only; Close waits after the writer exits

	// Owned by the writer goroutine (see writer.go).
	file    fsys.File
	logBase uint64 // seq the current log starts after
	logSize int64  // bytes in the current log, header included
	retryAt int64  // do not try a log switch again before logSize reaches this
	buf     []byte // group encoding buffer
	group   []*request
}

// Open opens the database in dir, creating it (mode 0700) if needed. It
// takes an exclusive lock on the directory, returning ErrLocked if another
// DB holds it, and recovers the state from the snapshot and logs. Invalid
// options are an error.
//
// The logger of ctx (zerowrap.FromCtx), enriched with the component and the
// directory, is used for the life of the DB; ctx is not used for anything
// else and does not bound the life of the DB.
func Open(ctx context.Context, dir string, opts Options) (*DB, error) {
	return open(ctx, fsys.OS(), dir, opts)
}

func open(ctx context.Context, fx fsys.FS, dir string, opts Options) (*DB, error) {
	opts, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	log := zerowrap.FromCtxWithFields(ctx, map[string]any{"component": "ermite-kv", "dir": dir})
	bld := tree{}.builder()
	rec, err := recoverDir(fx, dir, log,
		func(key, value []byte, version uint64) {
			// Snapshot entries alias one shared file buffer: copy them so
			// the tree does not keep the whole buffer alive.
			buf := make([]byte, len(key)+len(value))
			n := copy(buf, key)
			copy(buf[n:], value)
			bld.put(buf[:n:n], buf[n:len(buf):len(buf)], version)
		},
		func(seq uint64, ops []op) {
			for _, o := range ops {
				if o.kind == opPut {
					bld.put(o.key, o.value, seq)
				} else {
					bld.delete(o.key)
				}
			}
		})
	if err != nil {
		return nil, err
	}
	db := &DB{
		fx:      fx,
		dir:     dir,
		opts:    opts,
		log:     log,
		reqCh:   make(chan *request),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
		failed:  make(chan struct{}),
		lock:    rec.lock,
		file:    rec.log,
		logBase: rec.logBase,
		logSize: rec.logSize,
	}
	t := bld.freeze()
	db.snap.Store(newSnapshot(t, rec.lastSeq))
	go db.writer()
	log.Info().Uint64("seq", rec.lastSeq).Int("keys", t.len()).Uint64("snapshot_seq", rec.snapSeq).
		Dur("duration", time.Since(start)).Msg("opened")
	return db, nil
}

// Close stops the database: requests not yet taken by the writer get
// ErrClosed, the group in progress is committed and acknowledged, then the
// log is closed and the directory lock released. Snapshots stay readable.
// Close always completes: ctx is only used for the logger of the closing
// message.
//
// Close returns the first error met while stopping, including the cause of
// the failed state if the database failed, or nil. It is idempotent: later
// calls return nil.
func (db *DB) Close(ctx context.Context) error {
	var err error
	db.closeOnce.Do(func() {
		close(db.closing)
		<-db.done            // the writer finishes its group and exits
		db.compactors.Wait() // an in-flight compaction is not cancelled
		select {
		case <-db.failed:
			err = db.failErr
		default:
		}
		if cerr := db.file.Close(); err == nil {
			err = cerr
		}
		if cerr := db.lock.Close(); err == nil {
			err = cerr
		}
		log := zerowrap.FromCtxWithFields(ctx, map[string]any{"component": "ermite-kv", "dir": db.dir})
		log.Info().Uint64("seq", db.snap.Load().Seq()).Err(err).Msg("closed")
	})
	return err
}

// Snapshot returns the current immutable state. It is lock-free and O(1), and
// remains usable after Close. Like every read, it ignores ctx.
func (db *DB) Snapshot(_ context.Context) *Snapshot { return db.snap.Load() }

// Get returns the item stored under key in the current snapshot.
func (db *DB) Get(ctx context.Context, key []byte) (Item, bool) {
	return db.snap.Load().Get(ctx, key)
}

// Apply commits b atomically and returns its seq once it is durable (unless
// Options.NoSync) and visible in Snapshot.
//
// It returns the first error recorded by the batch (ErrInvalidKey), then
// ErrBatchTooLarge if the encoded batch exceeds Options.MaxBatchBytes, both
// without touching the database. ErrConflict (seq 0) means a precondition did
// not hold at commit time. A batch without puts and deletes writes nothing,
// consumes no seq and returns the current seq.
//
// If ctx ends before the writer takes the batch, Apply returns ctx.Err() and
// the batch is not applied. Once the writer has taken it, ctx is ignored and
// Apply waits for the outcome. ErrClosed is returned after Close. ErrFailed
// (wrapping the I/O error) means the database failed: for the batch that was
// being committed its outcome is unknown, since its bytes may be on disk and
// replayed by the next Open.
func (db *DB) Apply(ctx context.Context, b *Batch) (uint64, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.encodedSize() > db.opts.MaxBatchBytes {
		return 0, ErrBatchTooLarge
	}
	if err := db.stopped(); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	r := requestPool.Get().(*request)
	r.b = b
	select {
	case db.reqCh <- r:
	case <-ctx.Done():
		return 0, r.release(ctx.Err())
	case <-db.closing:
		return 0, r.release(db.stopped())
	case <-db.failed:
		return 0, r.release(db.stopped())
	}
	res := <-r.reply // the writer always answers a request it has taken
	_ = r.release(nil)
	return res.seq, res.err
}

// stopped returns the failure (which wins over Close, so callers see a
// stable error), ErrClosed, or nil while the database accepts batches.
func (db *DB) stopped() error {
	select {
	case <-db.failed:
		return db.failErr
	default:
	}
	select {
	case <-db.closing:
		return ErrClosed
	default:
		return nil
	}
}
