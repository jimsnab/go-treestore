package treestore

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestDeleteKeyTreeOne(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	address, isFirst := ts.SetKeyValue(sk, 100)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	removed := ts.DeleteKeyTree(sk)
	if !removed {
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

func TestDeleteTreeUnindexedKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey("test", "abc")
	sk2 := MakeStoreKey("test")

	address, exists := ts.SetKey(sk1)
	if address == 0 || exists {
		t.Error("first set")
	}

	removed := ts.DeleteKeyTree(sk2)
	if !removed {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeBaseKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
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

	removed := ts.DeleteKeyTree(sk1)
	if !removed {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != 0 || exists {
		t.Error("index must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second set must not exist")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second set index must not exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}

}

func TestDeleteTreeWithMiddleTwoNodes(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
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

	removed := ts.DeleteKeyTree(sk1)
	if !removed {
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

func TestDeleteTreeWithMiddleTwoNodes2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
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

	removed := ts.DeleteKeyTree(sk2)
	if !removed {
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

func TestDeleteTreeWithMiddleTwoNodes3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
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

	removed := ts.DeleteKeyTree(sk3)
	if !removed {
		t.Error("delete")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if verifyAddr != 0 || exists {
		t.Error("must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second must not exist")
	}

	verifyAddr, exists = ts.LocateKey(sk3)
	if verifyAddr != 0 || exists {
		t.Error("third must not exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey("a")
	sk2 := MakeStoreKey("a", "b")
	sk3 := MakeStoreKey("a", "b", "c")

	removed := ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete a")
	}

	removed = ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete a clean")
	}

	removed = ts.DeleteKeyTree(sk2)
	if removed {
		t.Error("delete b")
	}

	removed = ts.DeleteKeyTree(sk3)
	if removed {
		t.Error("delete c")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeNull(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey("")

	removed := ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete empty")
	}

	removed = ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete empty clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeDbSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey()

	removed := ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete sentinel")
	}

	removed = ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete sentinel clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeNullPopulated(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey("")

	a := MakeStoreKey("a")
	b := MakeStoreKey("a", "b")
	c := MakeStoreKey("a", "b", "c")

	ts.SetKey(a)
	ts.SetKeyValue(b, 100)
	ts.SetKey(c)

	removed := ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete empty")
	}

	removed = ts.DeleteKeyTree(sk1)
	if removed {
		t.Error("delete empty clean")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeSecondLevelWithChildren(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	removed := ts.DeleteKeyTree(a)
	if !removed {
		t.Error("delete a")
	}

	removed = ts.DeleteKeyTree(a)
	if removed {
		t.Error("delete a twice")
	}

	removed = ts.DeleteKeyTree(b)
	if !removed {
		t.Error("delete b")
	}

	removed = ts.DeleteKeyTree(b)
	if removed {
		t.Error("delete b twice")
	}

	removed = ts.DeleteKeyTree(c)
	if !removed {
		t.Error("delete c")
	}

	removed = ts.DeleteKeyTree(c)
	if removed {
		t.Error("delete c twice")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeDbValueEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey()

	removed := ts.DeleteKeyTree(sk)
	if removed {
		t.Error("delete db value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestDeleteTreeSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey()

	removed := ts.DeleteKeyTree(sk)
	if removed {
		t.Error("empty sentinel")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if verifyAddr != 1 || !exists {
		t.Error("sentinel must exist")
	}

	ts.SetKeyValue(sk, 123)

	removed = ts.DeleteKeyTree(sk)
	if !removed {
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

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
