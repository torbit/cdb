// Package cdb reads and writes cdb ("constant database") files.
//
// See the original cdb specification and C implementation by D. J. Bernstein
// at http://cr.yp.to/cdb.html.
package cdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"runtime"
)

const (
	headerSize = uint32(256 * 8)
)

type Cdb struct {
	r      io.ReaderAt
	closer io.Closer
}

type CdbIterator struct {
	db *Cdb
	// initErr is non-nil if an error happened when the iterator was created.
	initErr error
	// TODO: If iteration is exposed in the pkg interface, there needs to be a
	// note explaining that the CdbIterator keeps a references to the key slice.
	// If it is modified the iterator will stop working properly.
	key []byte
	// loop is the index of the next value for this iterator.
	loop uint32
	// khash is the hash of the key.
	khash uint32
	// kpos is the next file position in the hash to check for the key.
	kpos uint32
	// hpos is the file position of the hash table that this key is in.
	hpos uint32
	// hslots is the number of slots in the hash table.
	hslots uint32
	// dpos is the file position of the data. Only valid if the last call to next
	// returned nil.
	dpos uint32
	// dlen is the length of the data. Only valid if the last call to next
	// returned nil.
	dlen uint32
}

// Open opens the named file read-only and returns a new Cdb object.  The file
// should exist and be a cdb-format database file.
func Open(name string) (*Cdb, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	c := New(f)
	c.closer = f
	runtime.SetFinalizer(c, (*Cdb).Close)
	return c, nil
}

// Close closes the cdb for any further reads.
func (c *Cdb) Close() (err error) {
	if c.closer != nil {
		err = c.closer.Close()
		c.closer = nil
		runtime.SetFinalizer(c, nil)
	}
	return err
}

// New creates a new Cdb from the given ReaderAt, which should be a cdb format database.
func New(r io.ReaderAt) *Cdb {
	return &Cdb{r: r}
}

// Bytes returns the first value for this key as a []byte. Returns EOF when
// there is no value.
//
// Threadsafe.
func (c *Cdb) Bytes(key []byte) ([]byte, error) {
	return c.Iterate(key).NextBytes()
}

// Reader returns the first value for this key as an io.SectionReader. Returns
// EOF when there is no value.
//
// Threadsafe.
func (c *Cdb) Reader(key []byte) (*io.SectionReader, error) {
	return c.Iterate(key).NextReader()
}

// Iterate returns an iterator that can be used to access all of the values for
// a key. Always returns a non-nil value, even if the key has no values.
//
// Because the iterator keeps a reference to the byte slice, it shouldn't be
// modified until the iterator is no longer in use.
//
// Threadsafe.
func (c *Cdb) Iterate(key []byte) (iter *CdbIterator) {
	iter = new(CdbIterator)
	defer func() {
		if e := recover(); e != nil {
			iter.initErr = e.(error)
		}
	}()
	iter.db = c
	iter.key = key
	// Calculate the hash of the key.
	iter.khash = checksum(key)
	// Read in the position and size of the hash table for this key.
	iter.hpos, iter.hslots = c.readNums(iter.khash % 256 * 8)
	// If the hash table has no slots, there are no values.
	if iter.hslots == 0 {
		iter.initErr = io.EOF
		return
	}
	// Calculate first possible file position of key.
	hashslot := iter.khash / 256 % iter.hslots
	iter.kpos = iter.hpos + hashslot*8
	return
}

// NextBytes returns the next value for this iterator as a []byte. Returns EOF
// when there are no values left.
//
// Not threadsafe.
func (iter *CdbIterator) NextBytes() ([]byte, error) {
	if err := iter.next(); err != nil {
		return nil, err
	}
	data := make([]byte, iter.dlen)
	if _, err := iter.db.r.ReadAt(data, int64(iter.dpos)); err != nil {
		return nil, err
	}
	return data, nil
}

// NextReader returns the next value for this iterator as an io.SectionReader.
// Returns EOF when there are no values left.
//
// Not threadsafe.
func (iter *CdbIterator) NextReader() (*io.SectionReader, error) {
	if err := iter.next(); err != nil {
		return nil, err
	}
	return io.NewSectionReader(iter.db.r, int64(iter.dpos), int64(iter.dlen)), nil
}

// next iterates through the hash table until it finds the next match. If no
// matches are found, returns EOF.
//
// When a match is found dpos and dlen can be used to retreive the data.
func (iter *CdbIterator) next() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	if iter.initErr != nil {
		return iter.initErr
	}
	var khash, recPos uint32
	// Iterate through all of the hash slots until we find our key.
	for {
		// If we have seen every hash slot, we are done.
		if iter.loop >= iter.hslots {
			return io.EOF
		}
		khash, recPos = iter.db.readNums(iter.kpos)
		if recPos == 0 {
			return io.EOF
		}
		// Move the iterator to the next position.
		iter.loop++
		iter.kpos += 8
		// If the kpos goes past the end of the hash table, wrap around to the start.
		if iter.kpos == iter.hpos+(iter.hslots*8) {
			iter.kpos = iter.hpos
		}
		// If the key hash doesn't match, this hash slot isn't for our key. Keep iterating.
		if khash != iter.khash {
			continue
		}
		keyLen, dataLen := iter.db.readNums(recPos)
		// Check that the keys actually match in case of a hash collision.
		if keyLen != uint32(len(iter.key)) || iter.db.match(iter.key, recPos+8) == false {
			continue
		}
		iter.dpos = recPos + 8 + keyLen
		iter.dlen = dataLen
		return nil
	}
	panic("unreached")
}

// match returns true if the data at file position pos matches key.
func (c *Cdb) match(key []byte, pos uint32) bool {
	buf := make([]byte, 64)
	klen := len(key)
	for n := 0; n < klen; n += len(buf) {
		nleft := klen - n
		if len(buf) > nleft {
			buf = buf[:nleft]
		}
		if _, err := c.r.ReadAt(buf, int64(pos)); err != nil {
			panic(err)
		}
		if !bytes.Equal(buf, key[n:n+len(buf)]) {
			return false
		}
		pos += uint32(len(buf))
	}
	return true
}

func (c *Cdb) readNums(pos uint32) (uint32, uint32) {
	var buf [8]byte
	n, err := c.r.ReadAt(buf[:], int64(pos))
	// Ignore EOFs when we have read the full 8 bytes.
	if err != nil && (err != io.EOF || n < 8) {
		panic(err)
	}
	return binary.LittleEndian.Uint32(buf[:4]), binary.LittleEndian.Uint32(buf[4:])
}
