package treestore

import (
	"testing"
	"time"
)

func TestGetHistory(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("test")

	before := time.Now().UTC().UnixNano()
	ts.SetKeyValue(sk, 1)
	after1 := time.Now().UTC().UnixNano()
	ts.SetKeyValue(sk, 2)
	ts.SetKeyValue(sk, 3)

	val, exists := ts.GetKeyValueAtTime(sk, before)
	if val != nil || exists {
		t.Error("value before set")
	}

	val, exists = ts.GetKeyValueAtTime(sk, after1)
	if val != 1 || !exists {
		t.Error("first value")
	}

	val, exists = ts.GetKeyValueAtTime(sk, -1)
	if val != 3 || !exists {
		t.Error("last value")
	}

	val, exists = ts.GetKeyValueAtTime(sk, -9223372036854775808)
	if val != nil || exists {
		t.Error("invalid relative time")
	}

	val, exists = ts.GetKeyValueAtTime(MakeStoreKey("other"), -1)
	if val != nil || exists {
		t.Error("no value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestGetHistorySentinel(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey()

	before := time.Now().UTC().UnixNano()
	ts.SetKeyValue(sk, 1)
	ts.SetKeyValue(sk, 2)
	after2 := time.Now().UTC().UnixNano()
	ts.SetKeyValue(sk, 3)

	val, exists := ts.GetKeyValueAtTime(sk, before)
	if val != nil || exists {
		t.Error("value before set")
	}

	val, exists = ts.GetKeyValueAtTime(sk, after2)
	if val != 2 || !exists {
		t.Error("second value")
	}

	val, exists = ts.GetKeyValueAtTime(sk, -1)
	if val != 3 || !exists {
		t.Error("last value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
