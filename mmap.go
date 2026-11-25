// SPDX-License-Identifier: MIT

package kv

import "runtime"

// mapping is a whole snapshot file mapped read-only.
//
// It is unmapped by a cleanup once nothing references the *mapping: no entry,
// Item or pinned generation (see pins: the DB keeps a generation for one more
// swap so that bare value slices stay valid). Values that alias data are only
// valid while the *mapping is reachable, so whoever holds such a value must
// also hold the *mapping.
type mapping struct {
	data []byte
}

// newMapping wraps data, mapped from a snapshot file, and arranges for unmap
// to run once the mapping is unreachable.
func newMapping(data []byte, unmap func() error) *mapping {
	m := &mapping{data: data}
	// The cleanup must not reference m, or m would never become unreachable.
	runtime.AddCleanup(m, func(unmap func() error) {
		_ = unmap() // no context to report to; the error only leaks the address range
	}, unmap)
	return m
}
