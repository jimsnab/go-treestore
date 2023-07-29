package treestore

import "testing"

func TestIterateLevelEmpty(t *testing.T) {
	ts := NewTreeStore()

	keys := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty root")
	}

	ts.SetKey(MakeStoreKey())

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty root")
	}

	ts.SetKeyValue(MakeStoreKey(), 1)

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty root")
	}
}

func TestIterateLevelRoot(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("a")

	ts.SetKey(sk)

	keys := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "a" {
		t.Error("one node")
	}
	
	sk2 := MakeStoreKey("b", "c")

	ts.SetKey(sk2)

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "a" || string(keys[1].segment) != "b" {
		t.Error("two nodes")
	}
}