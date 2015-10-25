// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testKVSuite{})

type testKVSuite struct {
	b MemBuffer
}

func (s *testKVSuite) SetUpSuite(c *C) {
	s.b = NewBTreeBuffer(true)
}

func (s *testKVSuite) TearDownSuite(c *C) {
	s.b.Release()
}

func insertData(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := buffer.Set(val, val)
		c.Assert(err, IsNil)
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	fmt.Sscanf(string(s), "%010d", &n)
	return n
}

func valToStr(c *C, iter Iterator) string {
	val := iter.Value()
	return string(val)
}

func checkNewIterator(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		iter := buffer.NewIterator(val)
		c.Assert(iter.Key(), Equals, string(val))
		c.Assert(decodeInt([]byte(valToStr(c, iter))), Equals, i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter := buffer.NewIterator(val)
		c.Assert(iter.Key(), Equals, string(val))
		c.Assert(valToStr(c, iter), Equals, string(val))

		next, err := iter.Next()
		c.Assert(err, IsNil)
		c.Assert(next.Valid(), IsTrue)

		val = encodeInt((i + 1) * indexStep)
		c.Assert(next.Key(), Equals, string(val))
		c.Assert(valToStr(c, next), Equals, string(val))
		iter.Close()
	}

	// Non exist and beyond maximum seek test
	iter := buffer.NewIterator(encodeInt(testCount * indexStep))
	c.Assert(iter.Valid(), IsFalse)

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter = buffer.NewIterator(inBetween)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert(iter.Key(), Not(Equals), string(inBetween))
	c.Assert(iter.Key(), Equals, string(last))
	iter.Close()
}

func mustNotGet(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		_, err := buffer.Get(s)
		c.Assert(err, NotNil)
	}
}

func mustGet(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := buffer.Get(s)
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, string(s))
	}
}

func (s *testKVSuite) TestGetSet(c *C) {
	buffer := s.b
	insertData(c, buffer)
	mustGet(c, buffer)
	buffer.Release()
}

func (s *testKVSuite) TestNewIterator(c *C) {
	buffer := s.b
	defer buffer.Release()

	insertData(c, buffer)
	checkNewIterator(c, buffer)
}

func (s *testKVSuite) TestBasicNewIterator(c *C) {
	buffer := s.b
	defer buffer.Release()
	it := buffer.NewIterator([]byte("2"))
	c.Assert(it.Valid(), Equals, false)
}

func (s *testKVSuite) TestNewIteratorMin(c *C) {
	kvs := []struct {
		key   string
		value string
	}{
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"},
	}

	buffer := s.b
	for _, kv := range kvs {
		buffer.Set([]byte(kv.key), []byte(kv.value))
	}

	it := buffer.NewIterator(nil)
	for it.Valid() {
		fmt.Printf("%s, %s\n", it.Key(), it.Value())
		it, _ = it.Next()
	}

	it = buffer.NewIterator([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"))
	c.Assert(string(it.Key()), Equals, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")

	buffer.Release()
}
