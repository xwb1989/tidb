package kv

import (
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// memDbBuffer implements MemBuffer interface, using memdb as storage engine
type memDbBuffer struct {
	db *memdb.DB
}

// MemDbIter abc
type memDbIter struct {
	iter iterator.Iterator
}

// NewMemDbBuffer creates a new memDbBuffer
func NewMemDbBuffer() MemBuffer {
	return &memDbBuffer{db: memdb.New(comparer.DefaultComparer, 1*1024*1024)}
}

// NewIterator creates an Iterator
func (m *memDbBuffer) NewIterator(param interface{}) Iterator {
	var i Iterator
	if param == nil {
		i = &memDbIter{iter: m.db.NewIterator(&util.Range{})}
	} else {
		i = &memDbIter{iter: m.db.NewIterator(&util.Range{Start: param.([]byte)})}
	}
	i.Next()
	return i
}

// Get returns the value associated with key
func (m *memDbBuffer) Get(k Key) ([]byte, error) {
	return m.db.Get(k)
}

// Set associates key with value
func (m *memDbBuffer) Set(k []byte, v []byte) error {
	return m.db.Put(k, v)
}

// Release reset the buffer
func (m *memDbBuffer) Release() {
	m.db.Reset()
}

func (i *memDbIter) Next() (Iterator, error) {
	i.iter.Next()
	return i, nil
}

func (i *memDbIter) Valid() bool {
	return i.iter.Valid()
}

func (i *memDbIter) Key() string {
	return string(i.iter.Key())
}

func (i *memDbIter) Value() []byte {
	return i.iter.Value()
}

func (i *memDbIter) Close() {
	i.iter.Release()
}
