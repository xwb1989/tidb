// Copyright 2015 PingCAP, Inc.
//
// Copyright 2015 Wenbin Xiao
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/util/types"
)

type btreeBuffer struct {
	tree *memkv.Tree
}

// NewBTreeBuffer returns a breeBuffer
func NewBTreeBuffer(asc bool) MemBuffer {
	return &btreeBuffer{
		tree: memkv.NewTree(types.Collators[asc]),
	}
}

func (b *btreeBuffer) Get(k Key) ([]byte, error) {
	v, ok := b.tree.Get(toItfc(k))
	if !ok {
		return nil, ErrNotExist
	}
	return fromItfc(v), nil
}

func (b *btreeBuffer) Set(k []byte, v []byte) error {
	if len(v) == 0 {
		if val, ok := b.tree.Get(toItfc(k)); ok && len(fromItfc(val)) == 0 {
			// delete a deleted key
			return ErrNotExist
		}
	}
	b.tree.Set(toItfc(k), toItfc(v))
	return nil
}

func (b *btreeBuffer) Release() {
	b.tree.Clear()
}

type btreeIter struct {
	e  *memkv.Enumerator
	k  string
	v  []byte
	ok bool
}

func (b *btreeBuffer) NewIterator(param interface{}) Iterator {
	var e *memkv.Enumerator
	var ok bool
	var err error
	if param == nil {
		e, err = b.tree.SeekFirst()
		ok = err == nil
	} else {
		k := param.([]byte)
		first, _ := b.tree.First()
		last, _ := b.tree.Last()
		// special case: if the key is smaller than the first element, we just SeekFirst
		switch {
		case bytes.Compare(k, fromItfc(first)) <= 0:
			// seek before first key
			e, err = b.tree.SeekFirst()
			ok = err == nil
		case bytes.Compare(k, fromItfc(last)) > 0:
			// seek beyond last key, error
			ok = false
		default:
			// seek within range
			log.Debugf("key: %s\n", string(k))
			key := toItfc(k)
			e, ok = b.tree.Seek(key)
		}
	}
	if ok {
		iter := &btreeIter{e: e, ok: ok}
		// the initial push...
		iter.Next()
		return iter
	}
	// something is wrong
	return &btreeIter{e: e, ok: ok}
}

// Close implements Iterator Close
func (i *btreeIter) Close() {
	//noop
}

// Key implements Iterator Key
func (i *btreeIter) Key() string {
	return i.k
}

// Value implements Iterator Value
func (i *btreeIter) Value() []byte {
	return i.v
}

// Next implements Iterator Next
func (i *btreeIter) Next() (Iterator, error) {
	k, v, err := i.e.Next()
	// find the first non-nil key
	for err == nil && len(fromItfc(k)) == 0 {
		k, v, err = i.e.Next()
	}
	i.k, i.v, i.ok = string(fromItfc(k)), fromItfc(v), err == nil
	return i, err
}

// Valid implements Iterator Valid
func (i *btreeIter) Valid() bool {
	return i.ok
}

func toItfc(v []byte) []interface{} {
	return []interface{}{v}
}
func fromItfc(v []interface{}) []byte {
	if v == nil {
		return nil
	}
	return v[0].([]byte)
}
