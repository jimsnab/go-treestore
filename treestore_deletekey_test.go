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

func TestDeleteCleanUnindexedKey(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("test", "abc")
	sk2 := MakeStoreKey("test")

	address, isFirst := ts.SetKeyValue(sk1, 250)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	removed, val := ts.DeleteKeyWithValue(sk1, true)
	if !removed || val != 250 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != 0 || exists {
		t.Error("unindexed must not exist")
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

func TestDeleteWithMiddleTwoNodes(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("a", "b1", "c")
	sk2 := MakeStoreKey("a", "b2", "c")
	sk3 := MakeStoreKey("a", "b2")

	address1, isFirst := ts.SetKeyValue(sk1, 250)
	if address1 == 0 || !isFirst {
		t.Error("first set")
	}

	address2, isFirst := ts.SetKeyValue(sk2, 333)
	if address2 == 0 || !isFirst {
		t.Error("second set")
	}

	removed, val := ts.DeleteKeyWithValue(sk1, true)
	if !removed || val != 250 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	_, exists = ts.LocateKey(sk3)
	if !exists {
		t.Error("middle key must exist")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != address2 || !exists {
		t.Error("other key must exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteWithMiddleTwoNodes2(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("a", "b1", "c")
	sk2 := MakeStoreKey("a", "b2", "c")
	sk3 := MakeStoreKey("a", "b2")

	address1, isFirst := ts.SetKeyValue(sk1, 250)
	if address1 == 0 || !isFirst {
		t.Error("first set")
	}

	address2, isFirst := ts.SetKeyValue(sk2, 333)
	if address2 == 0 || !isFirst {
		t.Error("second set")
	}

	removed, val := ts.DeleteKeyWithValue(sk2, true)
	if !removed || val != 333 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk2)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	_, exists = ts.LocateKey(sk3)
	if exists {
		t.Error("middle key must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk1)
	if verifyAddr != address1 || !exists {
		t.Error("other key must exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteWithMiddleTwoNodes3(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("a", "b1", "c")
	sk2 := MakeStoreKey("a", "b2", "c")
	sk3 := MakeStoreKey("a")

	address1, isFirst := ts.SetKeyValue(sk1, 250)
	if address1 == 0 || !isFirst {
		t.Error("first set")
	}

	address2, isFirst := ts.SetKeyValue(sk2, 333)
	if address2 == 0 || !isFirst {
		t.Error("second set")
	}

	removed, val := ts.DeleteKeyWithValue(sk1, true)
	if !removed || val != 250 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != address2 || !exists {
		t.Error("second must exist")
	}

	removed, val = ts.DeleteKeyWithValue(sk2, true)
	if !removed || val != 333 {
		t.Error("delete")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second must not exist")
	}

	_, exists = ts.LocateKey(sk3)
	if exists {
		t.Error("first key node must not exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteWithMiddleTwoNodesDontClean(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("a", "b1", "c")
	sk2 := MakeStoreKey("a", "b2", "c")
	sk3 := MakeStoreKey("a", "b2")

	address1, isFirst := ts.SetKeyValue(sk1, 250)
	if address1 == 0 || !isFirst {
		t.Error("first set")
	}

	address2, isFirst := ts.SetKeyValue(sk2, 333)
	if address2 == 0 || !isFirst {
		t.Error("second set")
	}

	removed, val := ts.DeleteKeyWithValue(sk2, false)
	if !removed || val != 333 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk2)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	_, exists = ts.LocateKey(sk3)
	if !exists {
		t.Error("middle key must exist")
	}

	verifyAddr, exists = ts.LocateKey(sk1)
	if verifyAddr != address1 || !exists {
		t.Error("other key must exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteEmpty(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("a")
	sk2 := MakeStoreKey("a", "b")
	sk3 := MakeStoreKey("a", "b", "c")

	removed, val := ts.DeleteKeyWithValue(sk1, false)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(sk1, true)
	if removed || val != nil {
		t.Error("delete a clean")
	}

	removed, val = ts.DeleteKeyWithValue(sk2, false)
	if removed || val != nil {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(sk2, true)
	if removed || val != nil {
		t.Error("delete b clean")
	}

	removed, val = ts.DeleteKeyWithValue(sk3, false)
	if removed || val != nil {
		t.Error("delete c")
	}

	removed, val = ts.DeleteKeyWithValue(sk3, true)
	if removed || val != nil {
		t.Error("delete c clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteNull(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("")

	removed, val := ts.DeleteKeyWithValue(sk1, false)
	if removed || val != nil {
		t.Error("delete empty")
	}

	removed, val = ts.DeleteKeyWithValue(sk1, true)
	if removed || val != nil {
		t.Error("delete empty clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteDbSentinel(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey()

	removed, val := ts.DeleteKeyWithValue(sk1, false)
	if removed || val != nil {
		t.Error("delete sentinel")
	}

	removed, val = ts.DeleteKeyWithValue(sk1, true)
	if removed || val != nil {
		t.Error("delete sentinel clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteNullPopulated(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("")

	a := MakeStoreKey("a")
	b := MakeStoreKey("a", "b")
	c := MakeStoreKey("a", "b", "c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	removed, val := ts.DeleteKeyWithValue(sk1, false)
	if removed || val != nil {
		t.Error("delete empty")
	}

	removed, val = ts.DeleteKeyWithValue(sk1, true)
	if removed || val != nil {
		t.Error("delete empty clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteRoot(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("a")
	b := MakeStoreKey("b")
	c := MakeStoreKey("c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	keyRemoved, valueRemoved, val := ts.DeleteKey(a)
	if !keyRemoved || valueRemoved || val != nil {
		t.Error("delete a")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(a)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete a twice")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(b)
	if !keyRemoved || !valueRemoved || val != 100 {
		t.Error("delete b")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(b)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete b twice")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(c)
	if !keyRemoved || valueRemoved || val != nil {
		t.Error("delete c")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(c)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete c twice")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSecondLevel(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("base", "a")
	b := MakeStoreKey("base", "b")
	c := MakeStoreKey("base", "c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	keyRemoved, valueRemoved, val := ts.DeleteKey(a)
	if !keyRemoved || valueRemoved || val != nil {
		t.Error("delete a")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(a)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete a twice")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(b)
	if !keyRemoved || !valueRemoved || val != 100 {
		t.Error("delete b")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(b)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete b twice")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(c)
	if !keyRemoved || valueRemoved || val != nil {
		t.Error("delete c")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(c)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete c twice")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSecondLevelWithChildren(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("base", "a")
	b := MakeStoreKey("base", "b")
	c := MakeStoreKey("base", "c")
	childA := MakeStoreKey("base", "a", "x")
	childB := MakeStoreKey("base", "b", "x")
	childC := MakeStoreKey("base", "c", "x")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)
	ts.SetKeyValue(childA, 200)
	ts.SetKey(childB)
	ts.SetKey(childC)

	keyRemoved, valueRemoved, val := ts.DeleteKey(a)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete a")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(a)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete a twice")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(b)
	if keyRemoved || !valueRemoved || val != 100 {
		t.Error("delete b")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(b)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete b twice")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(c)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete c")
	}

	keyRemoved, valueRemoved, val = ts.DeleteKey(c)
	if keyRemoved || valueRemoved || val != nil {
		t.Error("delete c twice")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteRootWithValue(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("a")
	b := MakeStoreKey("b")
	c := MakeStoreKey("c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	removed, val := ts.DeleteKeyWithValue(a, false)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(b, false)
	if !removed|| val != 100 {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(b, false)
	if removed || val != nil {
		t.Error("delete b twice")
	}

	removed, val = ts.DeleteKeyWithValue(c, false)
	if removed || val != nil {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSecondLevelWithValue(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("base", "a")
	b := MakeStoreKey("base", "b")
	c := MakeStoreKey("base", "c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	removed, val := ts.DeleteKeyWithValue(a, false)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(b, false)
	if !removed || val != 100 {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(b, false)
	if removed || val != nil {
		t.Error("delete b twice")
	}

	removed, val = ts.DeleteKeyWithValue(c, false)
	if removed || val != nil {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSecondLevelWithChildrenWithValue(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("base", "a")
	b := MakeStoreKey("base", "b")
	c := MakeStoreKey("base", "c")
	childA := MakeStoreKey("base", "a", "x")
	childB := MakeStoreKey("base", "b", "x")
	childC := MakeStoreKey("base", "c", "x")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)
	ts.SetKeyValue(childA, 200)
	ts.SetKey(childB)
	ts.SetKey(childC)

	removed, val := ts.DeleteKeyWithValue(a, false)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(b, false)
	if !removed || val != 100 {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(b, false)
	if removed || val != nil {
		t.Error("delete b twice")
	}

	removed, val = ts.DeleteKeyWithValue(c, false)
	if removed || val != nil {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteRootWithValueClean(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("a")
	b := MakeStoreKey("b")
	c := MakeStoreKey("c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	removed, val := ts.DeleteKeyWithValue(a, true)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(b, true)
	if !removed || val != 100 {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(b, true)
	if removed || val != nil {
		t.Error("delete b twice")
	}

	removed, val = ts.DeleteKeyWithValue(c, true)
	if removed || val != nil {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSecondLevelWithValueClean(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("base", "a")
	b := MakeStoreKey("base", "b")
	c := MakeStoreKey("base", "c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	removed, val := ts.DeleteKeyWithValue(a, true)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(b, true)
	if !removed || val != 100 {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(b, true)
	if removed || val != nil {
		t.Error("delete b twice")
	}

	removed, val = ts.DeleteKeyWithValue(c, true)
	if removed || val != nil {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSecondLevelWithChildrenWithValueClean(t *testing.T) {
	ts := NewTreeStore()

	a := MakeStoreKey("base", "a")
	b := MakeStoreKey("base", "b")
	c := MakeStoreKey("base", "c")
	childA := MakeStoreKey("base", "a", "x")
	childB := MakeStoreKey("base", "b", "x")
	childC := MakeStoreKey("base", "c", "x")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)
	ts.SetKeyValue(childA, 200)
	ts.SetKey(childB)
	ts.SetKey(childC)

	removed, val := ts.DeleteKeyWithValue(a, true)
	if removed || val != nil {
		t.Error("delete a")
	}

	removed, val = ts.DeleteKeyWithValue(b, true)
	if !removed || val != 100 {
		t.Error("delete b")
	}

	removed, val = ts.DeleteKeyWithValue(b, true)
	if removed || val != nil {
		t.Error("delete b twice")
	}

	removed, val = ts.DeleteKeyWithValue(c, true)
	if removed || val != nil {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteDbValue(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey()

	address, exists := ts.SetKeyValue(sk, 25)
	if address != 1 || !exists {
		t.Error("first db value set")
	}

	removed, originalValue := ts.DeleteKeyWithValue(sk, true)
	if !removed || originalValue != 25 {
		t.Error("delete first db value")
	}

	verifyValue, keyExists, valueExists := ts.GetKeyValue(sk)
	if !keyExists || valueExists || verifyValue != nil {
		t.Error("get after first delete")
	}

	address, exists = ts.SetKeyValue(sk, 26)
	if address != 1 || !exists {
		t.Error("second db value set")
	}

	verifyValue, keyExists, valueExists = ts.GetKeyValue(sk)
	if !keyExists || !valueExists || verifyValue != 26 {
		t.Error("get after second delete")
	}

	removed, originalValue = ts.DeleteKeyWithValue(sk, true)
	if !removed || originalValue != 26 {
		t.Error("delete second db value")
	}

	verifyValue, keyExists, valueExists = ts.GetKeyValue(sk)
	if !keyExists || valueExists || verifyValue != nil {
		t.Error("get after second delete")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteDbValueEmpty(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey()

	removed, originalValue := ts.DeleteKeyWithValue(sk, true)
	if removed || originalValue != nil {
		t.Error("delete db value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteKeyCleanToGrandparent(t *testing.T) {
	ts := NewTreeStore()
	sk1 := MakeStoreKey("test", "data")
	sk2 := MakeStoreKey("test", "data", "cat", "dog")

	address, isFirst := ts.SetKeyValue(sk1, 100)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	address2, isFirst := ts.SetKeyValue(sk2, 200)
	if address2 == 0 || !isFirst {
		t.Error("second set")
	}

	removed, val := ts.DeleteKeyWithValue(sk2, true)
	if !removed || val != 200 {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != address || !exists {
		t.Error("must exist")
	}

	sk3 := MakeStoreKey("test", "data", "cat")
	_, exists = ts.LocateKey(sk3)
	if exists {
		t.Error("must be cleaned")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteSentinel(t *testing.T) {
	ts := NewTreeStore()
	sk := MakeStoreKey()

	keyRemoved, valueRemoved, orgVal := ts.DeleteKey(sk)
	if keyRemoved || valueRemoved || orgVal != nil {
		t.Error("empty sentinel")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if verifyAddr != 1 || !exists {
		t.Error("sentinel must exist")
	}

	removed, val := ts.DeleteKeyWithValue(sk, true)
	if removed || val != nil {
		t.Error("delete sentinel value")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if verifyAddr != 1 || !exists {
		t.Error("sentinel must still exist")
	}

	ts.SetKeyValue(sk, 123)

	keyRemoved, valueRemoved, orgVal = ts.DeleteKey(sk)
	if keyRemoved || !valueRemoved || orgVal != 123 {
		t.Error("sentinel key delete with value")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if verifyAddr != 1 || !exists {
		t.Error("sentinel key must exist")
	}

	verifyVal, keyExists, valExists := ts.GetKeyValue(sk)
	if verifyVal != nil || !keyExists || valExists {
		t.Error("sentinel key must not have value")
	}

	ts.SetKeyValue(sk, 456)

	removed, orgVal = ts.DeleteKeyWithValue(sk, true)
	if !removed || orgVal != 456 {
		t.Error("delete sentinel value 456")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if verifyAddr != 1 || !exists {
		t.Error("sentinel must still exist after 456 delete")
	}

	verifyVal, keyExists, valExists = ts.GetKeyValue(sk)
	if verifyVal != nil || !keyExists || valExists {
		t.Error("sentinel key must not have value after 456 delete")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

