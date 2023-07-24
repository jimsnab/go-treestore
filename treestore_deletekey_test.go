package treestore

import "testing"

func TestDeleteKeyWithValueOne(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, isFirst := ts.SetKeyValue(sk, 100)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	removed, val := ts.DeleteKeyWithValue(sk, true)
	if !removed || val != 100 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if verifyAddr != 0 || exists {
		t.Error("shouldn't exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteUnindexedKey(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("test", "abc")
	sk2 := MakeStoreKey("test")

	address, exists := ts.SetKey(sk1)
	if address == 0 || exists {
		t.Error("first set")
	}

	removed, val := ts.DeleteKeyWithValue(sk2, true)
	if removed || val != nil {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != address || !exists {
		t.Error("must exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteBaseKey(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "abc")

	firstAddr, isFirst := ts.SetKeyValue(sk1, 100)
	if firstAddr == 0 || !isFirst {
		t.Error("first set")
	}

	secondAddr, isFirst := ts.SetKeyValue(sk2, 200)
	if secondAddr == 0 || !isFirst {
		t.Error("second set")
	}

	removed, val := ts.DeleteKeyWithValue(sk1, true)
	if !removed || val != 100 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != firstAddr || !exists {
		t.Error("must exist")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != 0 || exists {
		t.Error("index must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != secondAddr || !exists {
		t.Error("second set must still exist")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != secondAddr || !exists {
		t.Error("second set index must still exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}

}
