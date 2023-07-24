package treestore

import "testing"

func TestSetKeyOne(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk)
	if address != verifyAddr || !exists {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOneTwoLevels(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test", "abc")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk)
	if address != verifyAddr || !exists {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOneThreeLevels(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test", "abc", "def")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk)
	if address != verifyAddr || !exists {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyTwoTwoLevels(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "abc")

	firstAddr, exists := ts.SetKey(sk1)
	if firstAddr == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	secondAddr, exists := ts.SetKey(sk2)
	if firstAddr == 0 || exists {
		t.Error("second set")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("set first again")
	}

	verifyAddr, exists = ts.SetKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("set second again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyTwoTwoLevelsFlip(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("test", "abc")
	sk2 := MakeStoreKey("test")

	firstAddr, exists := ts.SetKey(sk1)
	if firstAddr == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	secondAddr, exists := ts.SetKey(sk2)
	if secondAddr == 0 || !exists {
		t.Error("second set")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("set first again")
	}

	verifyAddr, exists = ts.SetKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("set second again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
