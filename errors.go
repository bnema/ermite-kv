// SPDX-License-Identifier: MIT

package kv

import "errors"

var (
	// ErrClosed is returned when the database is closed.
	ErrClosed = errors.New("kv: closed")
	// ErrFailed is returned once an I/O error put the database in the failed
	// state. It wraps the first I/O error.
	ErrFailed = errors.New("kv: database failed")
	// ErrConflict is returned when a batch precondition does not hold.
	ErrConflict = errors.New("kv: precondition failed")
	// ErrBatchTooLarge is returned when an encoded batch exceeds the limit.
	ErrBatchTooLarge = errors.New("kv: batch too large")
	// ErrInvalidKey is returned for an empty key or one longer than 64 KiB.
	ErrInvalidKey = errors.New("kv: invalid key")
	// ErrLocked is returned when another process holds the directory lock.
	ErrLocked = errors.New("kv: directory locked by another process")
	// ErrCorrupt is returned when on-disk data is damaged. It is wrapped with
	// the file and offset.
	ErrCorrupt = errors.New("kv: corrupt data")
)
