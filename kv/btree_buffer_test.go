package kv

import "testing"

func TestBasic(t *testing.T) {
	b := NewBTreeBuffer(true)
	keys := []string{"123", "124", "125", "126", "127", "128", "129"}
	for _, p := range keys {
		b.Set([]byte(p), []byte(p))
	}
	check(t, keys, 0, b.NewIterator(nil))
	for i := range keys {
		check(t, keys, i, b.NewIterator([]byte(keys[i])))
	}
	b.Release()

}

func TestSeek(t *testing.T) {
	b := NewBTreeBuffer(true)
	keys := []string{"123", "124", "125", "126", "137", "128", "129"}
	for _, p := range keys {
		b.Set([]byte(p), []byte(p))
	}
	it := b.NewIterator([]byte("111"))
	if !it.Valid() {
		t.Error("should be valid")
	}
	if it.Key() != "123" {
		t.Error("should be able to seek minimal")
	}
	it = b.NewIterator([]byte("130"))
	if !it.Valid() {
		t.Error("should be valid")
	}
	if it.Key() != "137" {
		t.Error("should be able to seek to 137")
	}
}

func check(t *testing.T, keys []string, i int, it Iterator) {
	for ; it.Valid(); it.Next() {
		if keys[i] != it.Key() {
			t.Errorf("key: %v != %v", keys[i], it.Key())
		}
		if keys[i] != string(it.Value()) {
			t.Errorf("value: %v != %v", keys[i], it.Value())
		}
		i++
	}
}
