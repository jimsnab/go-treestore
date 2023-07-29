package treestore

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestIterateLevelEmpty(t *testing.T) {
	ts := NewTreeStore()

	keys, count := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty root")
	}

	ts.SetKey(MakeStoreKey())

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty root")
	}

	ts.SetKeyValue(MakeStoreKey(), 1)

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty root")
	}
}

func TestIterateLevelRoot(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("a")

	ts.SetKey(sk)

	keys, count := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "a" || count != 1 {
		t.Error("one node")
	}
	
	sk2 := MakeStoreKey("b", "c")

	ts.SetKey(sk2)

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "a" || string(keys[1].segment) != "b" || count != 2 {
		t.Error("two nodes")
	}

	sk3 := MakeStoreKey("d")

	ts.SetKey(sk3)

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 3 || string(keys[0].segment) != "a" || string(keys[1].segment) != "b" || string(keys[2].segment) != "d" || count != 3 {
		t.Error("three nodes")
	}
}

func TestIterateSecondLevel(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("a")

	keys, count := ts.GetLevelKeys(sk, "*", 0, 100)
	if keys != nil || count != 0 {
		t.Error("no second level")
	}

	ts.SetKey(sk)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty second level")
	}
	
	sk2 := MakeStoreKey("a", "cat")

	ts.SetKey(sk2)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || keys[0].hasChildren {
		t.Error("key only node")
	}

	sk3 := MakeStoreKey("a", "cat", "dog")

	ts.SetKey(sk3)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("key only node")
	}

	sk4 := MakeStoreKey("a", "dog")

	ts.SetKeyValue(sk4, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags")
	}

	if !keys[1].hasValue || keys[1].hasChildren {
		t.Error("dog node flags")
	}

	sk5 := MakeStoreKey("a", "dog", "fido")

	ts.SetKeyValue(sk5, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags 2")
	}

	if !keys[1].hasValue || !keys[1].hasChildren {
		t.Error("dog node flags 2")
	}
}

func TestIterateThirdLevel(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("data", "test")

	keys, count := ts.GetLevelKeys(sk, "*", 0, 100)
	if keys != nil || count != 0 {
		t.Error("no second level")
	}

	ts.SetKey(sk)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty second level")
	}
	
	sk2 := MakeStoreKey("data", "test", "cat")

	ts.SetKey(sk2)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || keys[0].hasChildren {
		t.Error("key only node")
	}

	sk3 := MakeStoreKey("data", "test", "cat", "dog")

	ts.SetKey(sk3)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("key only node")
	}

	sk4 := MakeStoreKey("data", "test", "dog")

	ts.SetKeyValue(sk4, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags")
	}

	if !keys[1].hasValue || keys[1].hasChildren {
		t.Error("dog node flags")
	}

	sk5 := MakeStoreKey("data", "test", "dog", "fido")

	ts.SetKeyValue(sk5, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags 2")
	}

	if !keys[1].hasValue || !keys[1].hasChildren {
		t.Error("dog node flags 2")
	}
}

func TestIterateLevelPages(t *testing.T) {
	ts := NewTreeStore()

	taken := map[int]struct{}{}
	values := make([]string, 0, 250)
	for i := 0 ; i < 250 ; i++ {
		for {
			n := rand.Intn(10000)
			_, exists := taken[n]
			if !exists {
				taken[n] = struct{}{}
				text := fmt.Sprintf("%d", n)
				sk := MakeStoreKey(text)
				values = append(values, text)
				ts.SetKey(sk)
				break
			}
		}
	}

	sort.Strings(values)

	root := MakeStoreKey()
	keys, count := ts.GetLevelKeys(root, "*", 0, 0)
	if keys == nil || len(keys) != 0 || count != 250 {
		t.Error("empty page")
	}

	for i := 0 ; i < 250 ; i++ {
		keys, count = ts.GetLevelKeys(root, "*", i, 0)	
		if keys == nil || len(keys) != 0 || count != 250 {
			t.Error("empty page with non zero start")
		}

		keys, count = ts.GetLevelKeys(root, "*", i, 1)	
		if keys == nil || len(keys) != 1 || count != 250 {
			t.Error("one item page")
		}

		if string(keys[0].segment) != values[i] {
			t.Error("value mismatch")
		}

		remaining := 250 - i
		remaining2 := 2
		if remaining < remaining2 {
			remaining2 = remaining
		}
		keys, count = ts.GetLevelKeys(root, "*", i, 2)	
		if keys == nil || len(keys) != remaining2 || count != 250 {
			t.Error("two item page")
		}

		if string(keys[0].segment) != values[i] {
			t.Error("first value mismatch")
		}
		if len(keys) == 2 {
			if string(keys[1].segment) != values[i+1] {
				t.Error("second value mismatch")
			}
		}
	
		keys, count = ts.GetLevelKeys(root, "*", i, remaining)	
		if keys == nil || len(keys) != remaining || count != 250 {
			t.Error("remaining items page")
		}

		for j := 0 ; j < remaining ; j++ {
			if string(keys[j].segment) != values[i + j] {
				t.Errorf("page value %d", i + j)
			}
		}

		keys, count = ts.GetLevelKeys(root, "*", i, 1000)	
		if keys == nil || len(keys) != remaining || count != 250 {
			t.Error("large page with start offset")
		}

		for j := 0 ; j < remaining ; j++ {
			if string(keys[j].segment) != values[i + j] {
				t.Errorf("page value %d", i + j)
			}
		}
	}
}