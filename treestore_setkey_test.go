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

func TestSetNotExist(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, nil)
	if address != 0 || !exists || orgVal != nil {
		t.Error("first set again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetNotExistValue(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, 400, SetExMustNotExist, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, nil)
	if address != 0 || !exists || orgVal != 400 {
		t.Error("first set again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetMustExist(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustExist, 0, nil)
	if address != 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set again")
	}

	address2, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustExist, 0, nil)
	if address2 != address || !exists || orgVal != nil {
		t.Error("set exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetMustExistValue(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, 400, SetExMustExist, 0, nil)
	if address != 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 401, 0, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set again")
	}

	address2, exists, orgVal := ts.SetKeyValueEx(sk, 402, SetExMustExist, 0, nil)
	if address2 != address || !exists || orgVal != 401 {
		t.Error("set exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetDbValue(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey()

	address, exists := ts.SetKey(sk)
	if address != 1 || !exists {
		t.Error("first db set")
	}

	address, exists = ts.SetKeyValue(sk, 25)
	if address != 1 || !exists {
		t.Error("first db value set")
	}

	address, exists, orgVal := ts.SetKeyValueEx(sk, 26, SetExMustExist, 0, nil)
	if address != 1 || !exists || orgVal != 25 {
		t.Error("first setex")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 27, 0, 0, nil)
	if address != 1 || !exists || orgVal != 26 {
		t.Error("first setex again")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 28, SetExMustNotExist, 0, nil)
	if address != 0 || !exists || orgVal != 27 {
		t.Error("must not exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
