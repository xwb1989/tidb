package kv

import (
	"sort"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUnionIter{})

type testUnionIter struct {
	snapshotIt *mockIterator
	dirtyIt    iterator.Iterator
}

type mockKV struct {
	k string
	v string
}

type mockIterator struct {
	data []*mockKV
	cur  int
}

func newMockIterator(data []*mockKV) *mockIterator {
	ret := &mockIterator{data, 0}
	sort.Sort(ret)
	return ret
}

// sorter implement
func (it *mockIterator) Len() int {
	return len(it.data)
}

func (it *mockIterator) Swap(i, j int) {
	it.data[i], it.data[j] = it.data[j], it.data[i]
}

func (it *mockIterator) Less(i, j int) bool {
	return strings.Compare(it.data[i].k, it.data[j].k) < 0
}

// iterator interface implement
func (it *mockIterator) Next(_ FnKeyCmp) (Iterator, error) {
	it.cur++
	return it, nil
}

func (it *mockIterator) Value() []byte {
	return []byte(it.data[it.cur].v)
}

func (it *mockIterator) Key() string {
	return it.data[it.cur].k
}

func (it *mockIterator) Valid() bool {
	return it.cur < len(it.data)
}

func (it *mockIterator) Close() {}

// Test cases begin
func (t *testUnionIter) SetUpSuite(c *C) {
	t.snapshotIt = newMockIterator([]*mockKV{
		&mockKV{"z1", "1"},
		&mockKV{"z3", "3"},
		&mockKV{"z5", "5"},
		&mockKV{"z7", "7"},
		&mockKV{"aaa", "a"},
	})

	db := memdb.New(comparer.DefaultComparer, 1*1024*1024)
	db.Put([]byte("z2"), []byte("2"))
	db.Put([]byte("z4"), []byte("4"))
	db.Put([]byte("z6"), []byte("6"))
	t.dirtyIt = db.NewIterator(nil)
}

func (t *testUnionIter) TestBasicUnionIter(c *C) {
	it := Iterator(newUnionIter(t.dirtyIt, t.snapshotIt))
	cnt := 0
	for it.Valid() {
		cnt++
		it, _ = it.Next(nil)
	}
	c.Assert(cnt, Equals, 8)
}
