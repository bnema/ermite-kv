// SPDX-License-Identifier: MIT

package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Operation kinds.
const (
	opPut    byte = 1
	opDelete byte = 2
)

// Format limits.
const (
	maxRecord = 64 << 20 // maximum record payload size
	maxKeyLen = 64 << 10 // maximum key length
)

const (
	logMagic       = "EKVLOG01"
	snapshotMagic  = "EKVSNP01"
	logHeaderSize  = 8 + 8 + 4
	recordFrame    = 4 + 4 // len + crc
	recordMinBody  = 8 + 4 // seq + count
	snapHeaderSize = 8 + 8 + 8 + 4
	minOpSize      = 3 // kind + klen + 1 key byte
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errUnreadable marks a log record that is not readable (see decodeRecord).
var errUnreadable = errors.New("unreadable record")

// op is one mutation of a batch. value is nil for opDelete.
type op struct {
	kind       byte
	key, value []byte
}

// opSize returns the encoded size of o.
func opSize(o op) int {
	n := 1 + uvarintLen(uint64(len(o.key))) + len(o.key)
	if o.kind == opPut {
		n += uvarintLen(uint64(len(o.value))) + len(o.value)
	}
	return n
}

func uvarintLen(v uint64) int {
	n := 1
	for ; v >= 0x80; v >>= 7 {
		n++
	}
	return n
}

// appendLogHeader appends the 20-byte log header for base to dst.
func appendLogHeader(dst []byte, base uint64) []byte {
	start := len(dst)
	dst = append(dst, logMagic...)
	dst = binary.LittleEndian.AppendUint64(dst, base)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

// decodeLogHeader validates the log header at the start of b and returns its
// base. Errors wrap ErrCorrupt.
func decodeLogHeader(b []byte) (base uint64, err error) {
	if len(b) < logHeaderSize {
		return 0, fmt.Errorf("%w: short log header", ErrCorrupt)
	}
	if string(b[:8]) != logMagic {
		return 0, fmt.Errorf("%w: bad log magic", ErrCorrupt)
	}
	if crc32.Checksum(b[:16], castagnoli) != binary.LittleEndian.Uint32(b[16:20]) {
		return 0, fmt.Errorf("%w: bad log header checksum", ErrCorrupt)
	}
	return binary.LittleEndian.Uint64(b[8:16]), nil
}

// appendRecord appends one encoded record (len, crc, seq, count, ops) to dst.
// The payload must not exceed maxRecord and ops must be non-empty with valid
// keys; the caller enforces this (batches are validated before encoding).
func appendRecord(dst []byte, seq uint64, ops []op) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, recordFrame)...)
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(ops)))
	for _, o := range ops {
		dst = append(dst, o.kind)
		dst = binary.AppendUvarint(dst, uint64(len(o.key)))
		dst = append(dst, o.key...)
		if o.kind == opPut {
			dst = binary.AppendUvarint(dst, uint64(len(o.value)))
			dst = append(dst, o.value...)
		}
	}
	payload := dst[start+recordFrame:]
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(payload, castagnoli))
	return dst
}

// peekRecord returns the total encoded size (frame + payload) announced by
// the record frame at the start of b. ok is false if fewer than 8 bytes are
// present or the announced payload length is outside [12, maxRecord]. It does
// not check that the payload is present.
func peekRecord(b []byte) (total int, ok bool) {
	if len(b) < recordFrame {
		return 0, false
	}
	n := binary.LittleEndian.Uint32(b)
	if n < recordMinBody || n > maxRecord {
		return 0, false
	}
	return recordFrame + int(n), true
}

// decodeRecord decodes the record at the start of b. A record is readable
// only if the frame is present, 12 ≤ len ≤ maxRecord, len payload bytes are
// present, the CRC matches, and the payload holds exactly count ≥ 1 valid
// ops consuming exactly len bytes. Otherwise the error wraps errUnreadable.
//
// The decoded ops are appended to ops[:0]; their keys and values alias b.
// n is the number of bytes consumed.
func decodeRecord(b []byte, ops []op) (seq uint64, out []op, n int, err error) {
	total, ok := peekRecord(b)
	if !ok {
		return 0, nil, 0, fmt.Errorf("%w: bad or short frame", errUnreadable)
	}
	if len(b) < total {
		return 0, nil, 0, fmt.Errorf("%w: short payload", errUnreadable)
	}
	payload := b[recordFrame:total]
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(b[4:8]) {
		return 0, nil, 0, fmt.Errorf("%w: checksum mismatch", errUnreadable)
	}
	seq, out, err = decodePayload(payload, ops)
	if err != nil {
		return 0, nil, 0, err
	}
	return seq, out, total, nil
}

// decodePayload decodes seq, count and ops from a whole payload.
func decodePayload(p []byte, ops []op) (uint64, []op, error) {
	seq := binary.LittleEndian.Uint64(p)
	count := binary.LittleEndian.Uint32(p[8:])
	p = p[recordMinBody:]
	if count == 0 || uint64(count) > uint64(len(p)/minOpSize) {
		return 0, nil, fmt.Errorf("%w: bad op count %d", errUnreadable, count)
	}
	ops = ops[:0]
	for range count {
		if len(p) == 0 {
			return 0, nil, fmt.Errorf("%w: truncated op", errUnreadable)
		}
		o := op{kind: p[0]}
		if o.kind != opPut && o.kind != opDelete {
			return 0, nil, fmt.Errorf("%w: bad op kind %d", errUnreadable, o.kind)
		}
		var ok bool
		if o.key, p, ok = cutBytes(p[1:], maxKeyLen); !ok || len(o.key) == 0 {
			return 0, nil, fmt.Errorf("%w: bad key", errUnreadable)
		}
		if o.kind == opPut {
			if o.value, p, ok = cutBytes(p, maxRecord); !ok {
				return 0, nil, fmt.Errorf("%w: bad value", errUnreadable)
			}
		}
		ops = append(ops, o)
	}
	if len(p) != 0 {
		return 0, nil, fmt.Errorf("%w: %d trailing payload bytes", errUnreadable, len(p))
	}
	return seq, ops, nil
}

// cutBytes reads a uvarint length (at most limit) and that many bytes from
// the start of b. rest is what follows. The returned slice has no spare
// capacity.
func cutBytes(b []byte, limit int) (v, rest []byte, ok bool) {
	l, n := binary.Uvarint(b)
	if n <= 0 || l > uint64(limit) || l > uint64(len(b)-n) {
		return nil, nil, false
	}
	end := n + int(l)
	return b[n:end:end], b[end:], true
}

// ownOps returns a copy of ops whose keys and values live in one new
// allocation, so the result does not pin the buffer the ops were decoded from.
func ownOps(ops []op) []op {
	size := 0
	for _, o := range ops {
		size += len(o.key) + len(o.value)
	}
	buf := make([]byte, 0, size)
	out := make([]op, len(ops))
	for i, o := range ops {
		out[i].kind = o.kind
		buf = append(buf, o.key...)
		out[i].key = buf[len(buf)-len(o.key) : len(buf) : len(buf)]
		if o.kind == opPut {
			buf = append(buf, o.value...)
			out[i].value = buf[len(buf)-len(o.value) : len(buf) : len(buf)]
		}
	}
	return out
}

// entryIter yields snapshot entries in strictly ascending key order. It stops
// when yield returns false. The slices passed to yield need only stay valid
// until yield returns.
type entryIter = func(yield func(key, value []byte, version uint64) bool)

// writeSnapshot writes a snapshot file of count entries at seq to w. entries
// must yield exactly count entries sorted strictly ascending by key, with
// 1 ≤ len(key) ≤ maxKeyLen and version ≤ seq; otherwise an error is returned
// (the output is then incomplete and must be discarded).
func writeSnapshot(w io.Writer, seq, count uint64, entries entryIter) error {
	hdr := append(make([]byte, 0, snapHeaderSize), snapshotMagic...)
	hdr = binary.LittleEndian.AppendUint64(hdr, seq)
	hdr = binary.LittleEndian.AppendUint64(hdr, count)
	hdr = binary.LittleEndian.AppendUint32(hdr, crc32.Checksum(hdr, castagnoli))
	bw := bufio.NewWriterSize(w, 64<<10)
	if _, err := bw.Write(hdr); err != nil {
		return err
	}

	var (
		sum     uint32
		buf     []byte
		prev    []byte
		written uint64
		err     error
	)
	entries(func(key, value []byte, version uint64) bool {
		switch {
		case written == count:
			err = errors.New("snapshot: more entries than count")
		case len(key) == 0 || len(key) > maxKeyLen:
			err = fmt.Errorf("snapshot: invalid key length %d", len(key))
		case written > 0 && bytes.Compare(prev, key) >= 0:
			err = errors.New("snapshot: keys not strictly ascending")
		case version > seq:
			err = fmt.Errorf("snapshot: version %d above seq %d", version, seq)
		}
		if err != nil {
			return false
		}
		buf = binary.AppendUvarint(buf[:0], uint64(len(key)))
		buf = append(buf, key...)
		buf = binary.AppendUvarint(buf, version)
		buf = binary.AppendUvarint(buf, uint64(len(value)))
		buf = append(buf, value...)
		sum = crc32.Update(sum, castagnoli, buf)
		if _, err = bw.Write(buf); err != nil {
			return false
		}
		prev = append(prev[:0], key...)
		written++
		return true
	})
	if err != nil {
		return err
	}
	if written != count {
		return fmt.Errorf("snapshot: %d entries yielded, want %d", written, count)
	}
	if _, err := bw.Write(binary.LittleEndian.AppendUint32(nil, sum)); err != nil {
		return err
	}
	return bw.Flush()
}

// snapshotView is a fully validated snapshot file held in memory.
type snapshotView struct {
	seq, count uint64
	body       []byte // entries, without header and trailer
}

// openSnapshot validates the snapshot file contents b: header and trailer
// checksums, exactly count entries, strictly ascending non-empty keys,
// versions ≤ seq and no trailing bytes. Errors wrap ErrCorrupt.
func openSnapshot(b []byte) (snapshotView, error) {
	if len(b) < snapHeaderSize+4 {
		return snapshotView{}, fmt.Errorf("%w: short snapshot", ErrCorrupt)
	}
	if string(b[:8]) != snapshotMagic {
		return snapshotView{}, fmt.Errorf("%w: bad snapshot magic", ErrCorrupt)
	}
	if crc32.Checksum(b[:snapHeaderSize-4], castagnoli) != binary.LittleEndian.Uint32(b[snapHeaderSize-4:]) {
		return snapshotView{}, fmt.Errorf("%w: bad snapshot header checksum", ErrCorrupt)
	}
	v := snapshotView{
		seq:   binary.LittleEndian.Uint64(b[8:]),
		count: binary.LittleEndian.Uint64(b[16:]),
		body:  b[snapHeaderSize : len(b)-4],
	}
	if crc32.Checksum(v.body, castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return snapshotView{}, fmt.Errorf("%w: bad snapshot checksum", ErrCorrupt)
	}
	if err := v.walk(nil); err != nil {
		return snapshotView{}, err
	}
	return v, nil
}

// each calls fn for every entry in ascending key order. The slices alias the
// file contents: fn must not modify them and must copy them to retain them
// independently of the file buffer.
func (v snapshotView) each(fn func(key, value []byte, version uint64)) {
	_ = v.walk(fn) // cannot fail: the view was validated
}

func (v snapshotView) walk(fn func(key, value []byte, version uint64)) error {
	body := v.body
	var prev []byte
	for i := uint64(0); i < v.count; i++ {
		var key, value []byte
		var ok bool
		if key, body, ok = cutBytes(body, maxKeyLen); !ok || len(key) == 0 {
			return fmt.Errorf("%w: bad snapshot key at entry %d", ErrCorrupt, i)
		}
		if i > 0 && bytes.Compare(prev, key) >= 0 {
			return fmt.Errorf("%w: snapshot keys not ascending at entry %d", ErrCorrupt, i)
		}
		version, n := binary.Uvarint(body)
		if n <= 0 || version > v.seq {
			return fmt.Errorf("%w: bad snapshot version at entry %d", ErrCorrupt, i)
		}
		if value, body, ok = cutBytes(body[n:], maxRecord); !ok {
			return fmt.Errorf("%w: bad snapshot value at entry %d", ErrCorrupt, i)
		}
		if fn != nil {
			fn(key, value, version)
		}
		prev = key
	}
	if len(body) != 0 {
		return fmt.Errorf("%w: %d trailing snapshot bytes", ErrCorrupt, len(body))
	}
	return nil
}
