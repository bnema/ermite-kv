// SPDX-License-Identifier: MIT

// Package faultfs is an in-memory fsys.FS for tests. It models a page cache in
// front of durable storage and can inject write and sync failures, delay
// syncs, count syncs and simulate a power loss.
//
// Durability model: file bytes become durable on Sync or SyncAll; directory
// operations (create, rename, remove, mkdir) become durable on SyncDir of the
// directory that contains them. PowerLoss discards everything that is not.
//
// All methods are safe for concurrent use.
package faultfs

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"bnema.dev/ermite-kv/internal/fsys"
)

// ErrPowerLoss is returned by handles that were open when PowerLoss was called.
var ErrPowerLoss = errors.New("faultfs: handle invalidated by power loss")

// ErrInjected is the default error of injected failures.
var ErrInjected = errors.New("faultfs: injected failure")

const defaultPageSize = 4096

type node struct {
	data      []byte // current content (page cache)
	durable   []byte // content as of the last successful sync
	truncated bool   // data was truncated since the last sync
}

type nsOp struct {
	kind    byte // 'c' create, 'r' rename, 'd' remove, 'm' mkdir
	path    string
	oldPath string // rename source
	n       *node  // created, removed or renamed node
	prev    *node  // node replaced by a rename
	dirs    map[string]bool
}

// FS is an in-memory file system with fault injection.
type FS struct {
	mu    sync.Mutex
	files map[string]*node
	dirs  map[string]bool
	locks map[string]bool
	ns    []*nsOp // directory operations not yet made durable
	epoch uint64  // incremented by PowerLoss

	writes, syncs, syncAlls int
	failWrite               map[int]writeFault // by absolute write number
	failSync                map[int]error      // by absolute sync number (Sync+SyncAll)
	syncDelay               time.Duration
	nSyncCalls              int
}

type writeFault struct {
	keep int // bytes stored before failing; <0 means a plain failure
	err  error
}

var _ fsys.FS = (*FS)(nil)

// New returns an empty file system.
func New() *FS {
	return &FS{
		files:     map[string]*node{},
		dirs:      map[string]bool{"/": true, ".": true},
		locks:     map[string]bool{},
		failWrite: map[int]writeFault{},
		failSync:  map[int]error{},
	}
}

// FailWrite makes the n-th Write call from now (1 = the next one) fail with
// err (ErrInjected if nil) without storing any byte.
func (f *FS) FailWrite(n int, err error) {
	if err == nil {
		err = ErrInjected
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWrite[f.writes+n] = writeFault{keep: -1, err: err}
}

// ShortWrite makes the n-th Write call from now store only its first keep
// bytes and return (keep, io.ErrShortWrite). A call shorter than keep+1 bytes
// is unaffected.
func (f *FS) ShortWrite(n, keep int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWrite[f.writes+n] = writeFault{keep: keep, err: io.ErrShortWrite}
}

// FailSync makes the n-th Sync or SyncAll call from now (1 = the next one)
// fail with err (ErrInjected if nil). Nothing becomes durable.
func (f *FS) FailSync(n int, err error) {
	if err == nil {
		err = ErrInjected
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failSync[f.nSyncCalls+n] = err
}

// SetSyncDelay makes every Sync and SyncAll block for d before taking effect.
func (f *FS) SetSyncDelay(d time.Duration) {
	f.mu.Lock()
	f.syncDelay = d
	f.mu.Unlock()
}

// Writes returns the number of Write calls so far.
func (f *FS) Writes() int { f.mu.Lock(); defer f.mu.Unlock(); return f.writes }

// Syncs returns the number of Sync (fdatasync) calls so far.
func (f *FS) Syncs() int { f.mu.Lock(); defer f.mu.Unlock(); return f.syncs }

// SyncAlls returns the number of SyncAll (fsync) calls so far.
func (f *FS) SyncAlls() int { f.mu.Lock(); defer f.mu.Unlock(); return f.syncAlls }

// PowerLossOptions tunes PowerLoss.
type PowerLossOptions struct {
	// KeepPages keeps a random subset of the unsynced pages of each file.
	// Dropped pages inside the kept range read as zeros.
	KeepPages bool
	// ZeroFill keeps the unsynced size extension: the file keeps its length
	// and every unsynced page that is not kept reads as zeros.
	ZeroFill bool
	// PageSize is the page granularity; 0 means 4096.
	PageSize int
	// Rand drives KeepPages; nil uses a fixed seed.
	Rand *rand.Rand
}

// PowerLoss simulates a power failure: file bytes written after their last
// sync are dropped (see PowerLossOptions) and directory operations done after
// the last SyncDir of their directory are undone. Open handles become invalid
// and locks are released.
func (f *FS) PowerLoss(o PowerLossOptions) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o.PageSize <= 0 {
		o.PageSize = defaultPageSize
	}
	if o.Rand == nil {
		o.Rand = rand.New(rand.NewPCG(1, 2))
	}
	for i := len(f.ns) - 1; i >= 0; i-- {
		f.undo(f.ns[i])
	}
	f.ns = nil
	// Drop entries whose parent directory did not survive.
	for again := true; again; {
		again = false
		for d := range f.dirs {
			if !f.dirs[filepath.Dir(d)] {
				delete(f.dirs, d)
				again = true
			}
		}
	}
	for p := range f.files {
		if !f.dirs[filepath.Dir(p)] {
			delete(f.files, p)
		}
	}
	// Crash files in path order so a seeded Rand gives reproducible results.
	seen := map[*node]bool{}
	for _, p := range slices.Sorted(maps.Keys(f.files)) {
		n := f.files[p]
		if !seen[n] {
			seen[n] = true
			n.crash(o)
		}
	}
	f.locks = map[string]bool{}
	f.epoch++
}

func (f *FS) undo(op *nsOp) {
	switch op.kind {
	case 'c':
		delete(f.files, op.path)
	case 'm':
		delete(f.dirs, op.path)
	case 'd':
		f.files[op.path] = op.n
	case 'r':
		f.files[op.oldPath] = op.n
		if op.prev != nil {
			f.files[op.path] = op.prev
		} else {
			delete(f.files, op.path)
		}
	}
}

func (n *node) crash(o PowerLossOptions) {
	defer func() {
		n.durable = append([]byte(nil), n.data...)
		n.truncated = false
	}()
	if n.truncated || len(n.data) <= len(n.durable) {
		n.data = append([]byte(nil), n.durable...)
		return
	}
	off, end := len(n.durable), len(n.data)
	out := append(make([]byte, 0, end), n.durable...)
	out = append(out, make([]byte, end-off)...) // zero-filled extension
	kept := off
	for p := off / o.PageSize; p*o.PageSize < end; p++ {
		lo, hi := max(p*o.PageSize, off), min((p+1)*o.PageSize, end)
		if o.KeepPages && o.Rand.IntN(2) == 0 {
			copy(out[lo:hi], n.data[lo:hi])
			kept = hi
		}
	}
	if o.ZeroFill {
		kept = end
	}
	n.data = out[:kept]
}

// WriteFile creates or replaces a durable file (and its parent directories)
// without going through fault injection. It is meant for test setup.
func (f *FS) WriteFile(name string, data []byte) {
	name = filepath.Clean(name)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mkdirAll(filepath.Dir(name), true)
	f.files[name] = &node{
		data:    append([]byte(nil), data...),
		durable: append([]byte(nil), data...),
	}
}

// ReadFile returns a copy of the current content of name.
func (f *FS) ReadFile(name string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.files[filepath.Clean(name)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), n.data...), true
}

// DurableFile returns a copy of the content of name that would survive a
// power loss right now, ignoring directory entries.
func (f *FS) DurableFile(name string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.files[filepath.Clean(name)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), n.durable...), true
}

func (f *FS) journal(op *nsOp, dirs ...string) {
	op.dirs = map[string]bool{}
	for _, d := range dirs {
		op.dirs[d] = true
	}
	f.ns = append(f.ns, op)
}

// mkdirAll creates dir and parents; with durable set nothing is journaled.
func (f *FS) mkdirAll(dir string, durable bool) {
	for d := dir; !f.dirs[d]; d = filepath.Dir(d) {
		f.dirs[d] = true
		if !durable {
			f.journal(&nsOp{kind: 'm', path: d}, filepath.Dir(d))
		}
	}
}

// OpenFile implements fsys.FS. Supported flags: O_RDONLY, O_WRONLY, O_RDWR,
// O_CREATE, O_EXCL, O_TRUNC. Writing handles append.
func (f *FS) OpenFile(name string, flag int, perm os.FileMode) (fsys.File, error) {
	name = filepath.Clean(name)
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.files[name]
	switch {
	case f.dirs[name]:
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.New("is a directory")}
	case ok && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrExist}
	case !ok && flag&os.O_CREATE == 0:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	case !ok:
		if !f.dirs[filepath.Dir(name)] {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		n = &node{}
		f.files[name] = n
		f.journal(&nsOp{kind: 'c', path: name, n: n}, filepath.Dir(name))
	}
	acc := flag & (os.O_WRONLY | os.O_RDWR)
	if flag&os.O_TRUNC != 0 && acc != 0 {
		f.truncate(n, 0)
	}
	return &file{fs: f, n: n, epoch: f.epoch, write: acc != 0, read: acc != os.O_WRONLY}, nil
}

// Rename implements fsys.FS.
func (f *FS) Rename(oldpath, newpath string) error {
	oldpath, newpath = filepath.Clean(oldpath), filepath.Clean(newpath)
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.files[oldpath]
	if !ok {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrNotExist}
	}
	if !f.dirs[filepath.Dir(newpath)] {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrNotExist}
	}
	if oldpath == newpath {
		return nil
	}
	prev := f.files[newpath]
	delete(f.files, oldpath)
	f.files[newpath] = n
	f.journal(&nsOp{kind: 'r', path: newpath, oldPath: oldpath, n: n, prev: prev},
		filepath.Dir(oldpath), filepath.Dir(newpath))
	return nil
}

// Remove implements fsys.FS. It only removes files.
func (f *FS) Remove(name string) error {
	name = filepath.Clean(name)
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.files[name]
	if !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(f.files, name)
	f.journal(&nsOp{kind: 'd', path: name, n: n}, filepath.Dir(name))
	return nil
}

// ReadDir implements fsys.FS.
func (f *FS) ReadDir(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[dir] {
		return nil, &fs.PathError{Op: "readdir", Path: dir, Err: fs.ErrNotExist}
	}
	var names []string
	for p := range f.files {
		if filepath.Dir(p) == dir {
			names = append(names, filepath.Base(p))
		}
	}
	for p := range f.dirs {
		if p != dir && filepath.Dir(p) == dir {
			names = append(names, filepath.Base(p))
		}
	}
	sort.Strings(names)
	return names, nil
}

// MkdirAll implements fsys.FS.
func (f *FS) MkdirAll(dir string, _ os.FileMode) error {
	dir = filepath.Clean(dir)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[dir]; ok {
		return &fs.PathError{Op: "mkdir", Path: dir, Err: errors.New("not a directory")}
	}
	f.mkdirAll(dir, false)
	return nil
}

// SyncDir implements fsys.FS.
func (f *FS) SyncDir(dir string) error {
	dir = filepath.Clean(dir)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[dir] {
		return &fs.PathError{Op: "sync", Path: dir, Err: fs.ErrNotExist}
	}
	keep := f.ns[:0]
	for _, op := range f.ns {
		delete(op.dirs, dir)
		if len(op.dirs) > 0 {
			keep = append(keep, op)
		}
	}
	clear(f.ns[len(keep):])
	f.ns = keep
	return nil
}

// Lock implements fsys.FS. The lock is exclusive per path within this FS.
func (f *FS) Lock(path string) (io.Closer, error) {
	path = filepath.Clean(path)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.locks[path] {
		return nil, fsys.ErrLocked
	}
	if _, ok := f.files[path]; !ok {
		if !f.dirs[filepath.Dir(path)] {
			return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrNotExist}
		}
		n := &node{}
		f.files[path] = n
		f.journal(&nsOp{kind: 'c', path: path, n: n}, filepath.Dir(path))
	}
	f.locks[path] = true
	return &lock{fs: f, path: path, epoch: f.epoch}, nil
}

type lock struct {
	fs    *FS
	path  string
	epoch uint64
	once  sync.Once
}

func (l *lock) Close() error {
	l.once.Do(func() {
		l.fs.mu.Lock()
		defer l.fs.mu.Unlock()
		if l.fs.epoch == l.epoch {
			delete(l.fs.locks, l.path)
		}
	})
	return nil
}

func (f *FS) truncate(n *node, size int64) {
	if size < int64(len(n.data)) {
		n.data = n.data[:size]
		n.truncated = true
	} else {
		n.data = append(n.data, make([]byte, size-int64(len(n.data)))...)
	}
}

type file struct {
	fs          *FS
	n           *node
	epoch       uint64
	read, write bool
	closed      bool
}

// check must be called with fs.mu held.
func (h *file) check() error {
	switch {
	case h.closed:
		return os.ErrClosed
	case h.epoch != h.fs.epoch:
		return ErrPowerLoss
	}
	return nil
}

func (h *file) Write(p []byte) (int, error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := h.check(); err != nil {
		return 0, err
	}
	if !h.write {
		return 0, &fs.PathError{Op: "write", Err: fs.ErrPermission}
	}
	f.writes++
	if wf, ok := f.failWrite[f.writes]; ok {
		delete(f.failWrite, f.writes)
		switch {
		case wf.keep < 0:
			return 0, wf.err
		case wf.keep < len(p):
			h.n.data = append(h.n.data, p[:wf.keep]...)
			return wf.keep, wf.err
		}
	}
	h.n.data = append(h.n.data, p...)
	return len(p), nil
}

func (h *file) ReadAt(p []byte, off int64) (int, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if err := h.check(); err != nil {
		return 0, err
	}
	if !h.read {
		return 0, &fs.PathError{Op: "read", Err: fs.ErrPermission}
	}
	if off < 0 {
		return 0, errors.New("faultfs: negative offset")
	}
	if off >= int64(len(h.n.data)) {
		return 0, io.EOF
	}
	n := copy(p, h.n.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *file) Sync() error    { return h.sync(false) }
func (h *file) SyncAll() error { return h.sync(true) }

func (h *file) sync(all bool) error {
	f := h.fs
	f.mu.Lock()
	if err := h.check(); err != nil {
		f.mu.Unlock()
		return err
	}
	f.nSyncCalls++
	if all {
		f.syncAlls++
	} else {
		f.syncs++
	}
	if err, ok := f.failSync[f.nSyncCalls]; ok {
		delete(f.failSync, f.nSyncCalls)
		f.mu.Unlock()
		return err
	}
	d := f.syncDelay
	f.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := h.check(); err != nil {
		return err
	}
	n := h.n
	if !n.truncated && len(n.data) >= len(n.durable) {
		n.durable = append(n.durable, n.data[len(n.durable):]...)
	} else {
		n.durable = append(n.durable[:0], n.data...)
	}
	n.truncated = false
	return nil
}

func (h *file) Truncate(size int64) error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if err := h.check(); err != nil {
		return err
	}
	if !h.write || size < 0 {
		return &fs.PathError{Op: "truncate", Err: fs.ErrInvalid}
	}
	h.fs.truncate(h.n, size)
	return nil
}

func (h *file) Size() (int64, error) {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if err := h.check(); err != nil {
		return 0, err
	}
	return int64(len(h.n.data)), nil
}

func (h *file) Close() error {
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.closed {
		return os.ErrClosed
	}
	h.closed = true
	return nil
}
