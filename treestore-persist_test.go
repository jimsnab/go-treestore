package treestore

import (
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/spf13/afero"
)

func TestSaveLoadEmpty(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	err := ts.Save(ts.l, "/test/empty.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test/empty.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadOneKey(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("cat")
	addr, _ := ts.SetKey(sk)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	verifyAddr, exists := ts2.LocateKey(sk)
	if verifyAddr != addr || !exists {
		t.Error("locate key")
	}

	_, exists = ts2.IsKeyIndexed(sk)
	if exists {
		t.Error("value not indexed")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadOneValue(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("cat")

	addr, _ := ts.SetKeyValue(sk, 262)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	verifyAddr, exists := ts2.LocateKey(sk)
	if verifyAddr != addr || !exists {
		t.Error("locate key")
	}

	verifyAddr, exists = ts2.IsKeyIndexed(sk)
	if verifyAddr != addr || !exists {
		t.Error("value not indexed")
	}

	val, keyExists, valueExists := ts2.GetKeyValue(sk)
	if val != 262 || !keyExists || !valueExists {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadTwoLevels(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk1 := MakeStoreKey("cat")
	sk2 := MakeStoreKey("cat", "test")

	addr1, _ := ts.SetKeyValue(sk1, 262)
	addr2, _ := ts.SetKey(sk2)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	verifyAddr, exists := ts2.LocateKey(sk1)
	if verifyAddr != addr1 || !exists {
		t.Error("locate key")
	}

	verifyAddr, exists = ts2.IsKeyIndexed(sk1)
	if verifyAddr != addr1 || !exists {
		t.Error("value not indexed")
	}

	val, keyExists, valueExists := ts2.GetKeyValue(sk1)
	if val != 262 || !keyExists || !valueExists {
		t.Error("value verify")
	}

	verifyAddr, exists = ts2.LocateKey(sk2)
	if verifyAddr != addr2 || !exists {
		t.Error("locate key 2")
	}

	verifyAddr, exists = ts2.IsKeyIndexed(sk2)
	if verifyAddr != 0 || exists {
		t.Error("value not indexed 2")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadExpired(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("cat")

	ts.SetKeyValueEx(sk, 551, 0, 100, nil)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	verifyAddr, exists := ts2.LocateKey(sk)
	if verifyAddr != 0 || exists {
		t.Error("locate expired key")
	}

	verifyAddr, exists = ts2.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("value not indexed")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadNotExpired(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("cat")

	tick := time.Now().UTC().UnixNano()
	tick += 24 * 60 * 60 * 1000 * 1000 * 1000
	addr, _, _ := ts.SetKeyValueEx(sk, 551, 0, tick, nil)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	verifyAddr, exists := ts2.LocateKey(sk)
	if verifyAddr != addr || !exists {
		t.Error("locate unexpired key")
	}

	verifyAddr, exists = ts2.IsKeyIndexed(sk)
	if verifyAddr != addr || !exists {
		t.Error("value indexed")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadRelationships(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk1 := MakeStoreKey("cat")
	sk2 := MakeStoreKey("pet")

	addr1, _ := ts.SetKey(sk1)
	ts.SetKeyValueEx(sk2, 900, 0, 0, []StoreAddress{addr1})

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	keys := ts2.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if len(keys) != 2 {
		t.Error("get both keys")
	}

	if keys[0].relationships != nil || keys[0].sk.path != "/cat" {
		t.Error("first key no relationships")
	}

	if len(keys[1].relationships) != 1 || keys[1].relationships[0] != addr1 || keys[1].sk.path != "/pet" {
		t.Error("second key one relationship")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadHistory(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("testing")

	before := time.Now().UTC().UnixNano()
	ts.SetKeyValue(sk, 1)
	after1 := time.Now().UTC().UnixNano()
	ts.SetKeyValue(sk, 2)
	ts.SetKeyValue(sk, 3)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	val, exists := ts2.GetKeyValueAtTime(sk, before)
	if val != nil || exists {
		t.Error("value before set")
	}

	val, exists = ts2.GetKeyValueAtTime(sk, after1)
	if val != 1 || !exists {
		t.Error("first value")
	}

	val, exists = ts2.GetKeyValueAtTime(sk, -1)
	if val != 3 || !exists {
		t.Error("last value")
	}

	val, exists = ts2.GetKeyValueAtTime(sk, -9223372036854775808)
	if val != nil || exists {
		t.Error("invalid relative time")
	}

	val, exists = ts2.GetKeyValueAtTime(MakeStoreKey("other"), -1)
	if val != nil || exists {
		t.Error("no value")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadMetadata(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("cat")
	ts.SetKey(sk)
	ts.SetMetdataAttribute(sk, "abc", "123")

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	keys := ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("key loaded")
	}

	attributes := ts.GetMetadataAttributes(sk)
	if len(attributes) != 1 || attributes[0] != "abc" {
		t.Error("attribute missing")
	}

	_, val := ts.GetMetadataAttribute(sk, "abc")
	if val != "123" {
		t.Error("attribute value missing")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}
