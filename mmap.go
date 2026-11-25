// SPDX-License-Identifier: MIT

package kv

import (
	"runtime"
	"sync/atomic"
)

// mappingsReleased counts the mappings unmapped by their cleanup. Tests use it
// to observe the GC-driven release.
var mappingsReleased atomic.Int64

// onRelease, when set by a test, is called with the name of each mapping
// released by its cleanup, after the unmap.
var onRelease atomic.Pointer[func(name string)]

// mapping is a whole snapshot file mapped read-only.
//
// It is unmapped by a cleanup once nothing references the *mapping: no entry,
// Item or pinned generation (see pins: the DB keeps a generation for one more
// swap so that bare value slices stay valid). Values that alias data are only
// valid while the *mapping is reachable, so whoever holds such a value must
// also hold the *mapping.
type mapping struct {
	data []byte
	name string // snapshot file the data comes from
}

// newMapping wraps data, mapped from the file name, and arranges for unmap to
// run once the mapping is unreachable.
func newMapping(data []byte, unmap func() error, name string) *mapping {
	m := &mapping{data: data, name: name}
	// The cleanup must not reference m, or m would never become unreachable.
	type release struct {
		unmap func() error
		name  string
	}
	runtime.AddCleanup(m, func(r release) {
		_ = r.unmap() // no context to report to; the error only leaks the address range
		mappingsReleased.Add(1)
		if f := onRelease.Load(); f != nil {
			(*f)(r.name)
		}
	}, release{unmap, name})
	return m
}
