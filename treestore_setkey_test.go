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

func TestSetExNoValue(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, nil)
	if address == 0 || exists || orgVal != nil {
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

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, nil)
	if address == 0 || !exists || orgVal != nil {
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

func TestSetRelationship(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("pet", "cat")
	sk2 := MakeStoreKey("sound", "meow")
	sk3 := MakeStoreKey("color", "multi")

	address2, exists := ts.SetKey(sk2)
	if address2 == 0 || exists {
		t.Error("first set")
	}

	address1, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address2})
	if address1 == 0 || exists || orgVal != nil {
		t.Error("second set")
	}

	// setting a relationship gives the key a value, even if nil
	verifyVal, keyExists, valueExists := ts.GetKeyValue(sk1)
	if verifyVal != nil || !keyExists || !valueExists {
		t.Error("value verify")
	}

	address3, exists, orgVal := ts.SetKeyValueEx(sk3, "calico", 0, 0, []StoreAddress{address1})
	if address3 == 0 || exists || orgVal != nil {
		t.Error("third set")
	}

	sk4 := MakeStoreKey("sound", "roar")
	address4, exists := ts.SetKey(sk4)
	if address4 == 0 || exists {
		t.Error("fourth set")
	}

	verifyAddr, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address4})
	if verifyAddr != address1 || !exists || orgVal != nil {
		t.Error("change relationship")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
