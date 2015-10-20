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

package structure

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
)

// TypeFlag is for data structure meta/data flag.
type TypeFlag byte

const (
	StringMeta TypeFlag = 'S'
	StringData TypeFlag = 's'
	HashMeta   TypeFlag = 'H'
	HashData   TypeFlag = 'h'
	ListMeta   TypeFlag = 'L'
	ListData   TypeFlag = 'l'
)

func encodeStringDataKey(key []byte) []byte {
	ek := make([]byte, 0, len(key)+4)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(StringData))
}

func encodeHashMetaKey(key []byte) []byte {
	ek := make([]byte, 0, len(key)+4)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(HashMeta))
}

func encodeHashDataKey(key []byte, field []byte) []byte {
	ek := make([]byte, 0, len(key)+len(field)+6)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(HashData))
	return codec.EncodeBytes(ek, field)
}

func decodeHashDataKey(ek []byte) ([]byte, []byte, error) {
	var (
		key   []byte
		field []byte
		err   error
		tp    uint64
	)

	ek, key, err = codec.DecodeBytes(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if TypeFlag(tp) != HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}

	_, field, err = codec.DecodeBytes(ek)
	return key, field, errors.Trace(err)
}

func hashDataKeyPrefix(key []byte) []byte {
	ek := make([]byte, 0, len(key)+4)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(HashData))
}

func encodeListMetaKey(key []byte) []byte {
	ek := make([]byte, 0, len(key)+4)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(ListMeta))
}

func encodeListDataKey(key []byte, index int64) []byte {
	ek := make([]byte, 0, len(key)+13)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(ListData))
	return codec.EncodeInt(ek, index)
}