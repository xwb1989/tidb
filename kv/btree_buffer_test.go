package kv

import "testing"

func TestT(t *testing.T) {
	b := NewBTreeBuffer(true)
	data := [][]string{
		{"123", "abc"},
		{"124", "abd"},
		{"125", "abe"},
		{"126", "abf"},
	}
	for _, p := range data {
		b.Set([]byte(p[0]), []byte(p[1]))
	}

	check(t, data, 0, b.NewIterator(nil))
	check(t, data, 1, b.NewIterator([]byte("124")))
	check(t, data, 2, b.NewIterator([]byte("125")))
	check(t, data, 3, b.NewIterator([]byte("126")))
}

func check(t *testing.T, data [][]string, i int, it Iterator) {
	for ; it.Valid(); it.Next() {
		if data[i][0] != it.Key() {
			t.Errorf("key: %v != %v", data[i][0], it.Key())
		}
		if data[i][1] != string(it.Value()) {
			t.Errorf("value: %v != %v", data[i][1], it.Value())
		}
		i++
	}
}
