// SPDX-License-Identifier: MIT

// Package fsys is the minimal file-system abstraction used by the store, with
// an os-backed implementation. The faultfs sub-package provides an in-memory
// implementation with fault injection for tests.
package fsys

import (
	"errors"
	"io"
	"os"
	"sort"
	"syscall"
)

// ErrLocked is returned by FS.Lock when the lock is already held.
var ErrLocked = errors.New("fsys: locked")

// FS is a file system. Paths are used as given; implementations do not
// resolve them against a working directory other than the OS one.
type FS interface {
	// OpenFile opens name with os.OpenFile flags. Files opened for writing
	// always append: Write adds bytes at the current end of the file.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// Rename atomically replaces newpath with oldpath.
	Rename(oldpath, newpath string) error
	// Remove deletes a file.
	Remove(name string) error
	// ReadDir returns the sorted entry names of dir.
	ReadDir(dir string) ([]string, error)
	// MkdirAll creates dir and any missing parents.
	MkdirAll(dir string, perm os.FileMode) error
	// SyncDir makes the entries of dir (creations, renames, removals)
	// durable.
	SyncDir(dir string) error
	// Lock takes an exclusive, non-blocking lock on the file at path,
	// creating it if needed. It returns ErrLocked if the lock is held,
	// including by the same process. Closing the result releases the lock.
	Lock(path string) (io.Closer, error)
}

// File is an open file.
type File interface {
	// Write appends p to the end of the file.
	Write(p []byte) (int, error)
	ReadAt(p []byte, off int64) (int, error)
	// Sync flushes data and the size needed to read it (fdatasync).
	Sync() error
	// SyncAll flushes data and all metadata (fsync).
	SyncAll() error
	Truncate(size int64) error
	Size() (int64, error)
	Close() error
}

// OS returns the FS backed by the operating system.
func OS() FS { return osFS{} }

type osFS struct{}

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		flag |= os.O_APPEND
	}
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) Remove(name string) error { return os.Remove(name) }

func (osFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)
	return names, nil
}

func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }

func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

func (osFS) Lock(path string) (io.Closer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := control(f, func(fd int) error {
		return syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
	}); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil // closing the descriptor releases the flock
}

type osFile struct{ *os.File }

func (f osFile) Sync() error {
	return control(f.File, syscall.Fdatasync)
}

func (f osFile) SyncAll() error { return f.File.Sync() }

func (f osFile) Size() (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// control runs fn on the file descriptor, retrying on EINTR.
func control(f *os.File, fn func(fd int) error) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var ferr error
	if err := rc.Control(func(fd uintptr) {
		for {
			ferr = fn(int(fd))
			if !errors.Is(ferr, syscall.EINTR) {
				return
			}
		}
	}); err != nil {
		return err
	}
	return ferr
}
