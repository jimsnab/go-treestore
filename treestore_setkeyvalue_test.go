package treestore

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestSetKeyValueOne(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	address, isFirst := ts.SetKeyValue(sk, 10)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != address || !exists {
		t.Error("first set indexed")
	}

	address2, isFirst := ts.SetKeyValue(sk, 20)
	if address2 != address || isFirst {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address2 != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != address2 || !exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyValueOneTwoLevels(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test", "abc")

	address, isFirst := ts.SetKeyValue(sk, 10)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != address || !exists {
		t.Error("first set indexed")
	}

	address2, isFirst := ts.SetKeyValue(sk, 20)
	if address != address2 || isFirst {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address2 != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != address2 || !exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyValueOneThreeLevels(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test", "abc", "def")

	address, isFirst := ts.SetKeyValue(sk, 10)
	if address == 0 || !isFirst {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != address || !exists {
		t.Error("first set indexed")
	}

	verifyAddr, isFirst = ts.SetKeyValue(sk, 20)
	if address != verifyAddr || isFirst {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != address || !exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyValueTwoTwoLevels(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "abc")

	firstAddr, isFirst := ts.SetKeyValue(sk1, 10)
	if firstAddr == 0 || !isFirst {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != firstAddr || !exists {
		t.Error("first set indexed")
	}

	secondAddr, isFirst := ts.SetKeyValue(sk2, 22)
	if firstAddr == 0 || !isFirst {
		t.Error("second set")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != secondAddr || !exists {
		t.Error("second set indexed")
	}

	verifyAddr, isFirst = ts.SetKeyValue(sk1, 10)
	if firstAddr != verifyAddr || isFirst {
		t.Error("set first again")
	}

	verifyAddr, isFirst = ts.SetKeyValue(sk2, 22)
	if secondAddr != verifyAddr || isFirst {
		t.Error("set second again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyValueTwoTwoLevelsFlip(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk1 := MakeStoreKey("test", "abc")
	sk2 := MakeStoreKey("test")

	firstAddr, isFirst := ts.SetKeyValue(sk1, 10)
	if firstAddr == 0 || !isFirst {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != firstAddr || !exists {
		t.Error("first set indexed")
	}

	secondAddr, isFirst := ts.SetKeyValue(sk2, 22)
	if secondAddr == 0 || !isFirst {
		t.Error("second set")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != secondAddr || !exists {
		t.Error("second set indexed")
	}

	verifyAddr, isFirst = ts.SetKeyValue(sk1, 10)
	if firstAddr != verifyAddr || isFirst {
		t.Error("set first again")
	}

	verifyAddr, isFirst = ts.SetKeyValue(sk2, 22)
	if secondAddr != verifyAddr || isFirst {
		t.Error("set second again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
