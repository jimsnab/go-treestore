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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	err := ts.Save(ts.l, "/test/empty.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")
	addr, _ := ts.SetKey(sk)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")

	addr, _ := ts.SetKeyValue(sk, 262)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("cat")
	sk2 := MakeStoreKey("cat", "test")

	addr1, _ := ts.SetKeyValue(sk1, 262)
	addr2, _ := ts.SetKey(sk2)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")

	ts.SetKeyValueEx(sk, 551, 0, 100, nil)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")

	tick := time.Now().UTC().UnixNano()
	tick += 24 * 60 * 60 * nsPerSec
	addr, _, _ := ts.SetKeyValueEx(sk, 551, 0, tick, nil)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("cat")
	sk2 := MakeStoreKey("pet")

	addr1, _ := ts.SetKey(sk1)
	ts.SetKeyValueEx(sk2, 900, 0, 0, []StoreAddress{addr1})

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	keys := ts2.GetMatchingKeys(MakeStoreKey("**"), 0, 100, false)
	if len(keys) != 2 {
		t.Error("get both keys")
	}

	if keys[0].Relationships != nil || keys[0].Key != "/cat" {
		t.Error("first key no relationships")
	}

	if len(keys[1].Relationships) != 1 || keys[1].Relationships[0] != addr1 || keys[1].Key != "/pet" {
		t.Error("second key one relationship")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadHistory(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

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

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")
	ts.SetKey(sk)
	ts.SetMetadataAttribute(sk, "abc", "123")

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	keys := ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100, false)
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

func TestSaveLoadTwoLevelValue(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat", "test")

	addr, _ := ts.SetKeyValue(sk, 55)

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
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
	if val != 55 || !keyExists || !valueExists {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadSentinel(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()
	sk2 := MakeStoreKey("foo")

	addr, _ := ts.SetKey(sk2)

	ts.SetKeyValueEx(sk, 230, 0, -1000*(1000*1000*1000), []StoreAddress{addr})
	ttl := ts.GetKeyTtl(sk)
	checkpoint := time.Now().UTC().UnixNano()
	if ttl < checkpoint {
		t.Error("relative ttl")
	}
	ts.SetKeyValueEx(sk, 630, 0, -1, []StoreAddress{addr})
	ts.SetMetadataAttribute(sk, "test", "cat")

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	v, keyExists, valueExists := ts2.GetKeyValue(sk)
	if v != 630 || !keyExists || !valueExists {
		t.Error("latest value verify")
	}

	v, valueExists = ts2.GetKeyValueAtTime(sk, checkpoint)
	if v != 230 || !valueExists {
		t.Error("first value verify")
	}

	ttl2 := ts2.GetKeyTtl(sk)
	if ttl2 != ttl {
		t.Error("ttl verify")
	}

	exists, md := ts2.GetMetadataAttribute(sk, "test")
	if !exists || md != "cat" {
		t.Error("metadata verify")
	}

	hasLink, rv := ts2.GetRelationshipValue(sk, 0)
	if !hasLink || rv == nil || rv.CurrentValue != nil || rv.Sk.Path != "/foo" {
		t.Error("follow relationship")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadThreeLevels(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("first", "second", "third")
	sk2 := MakeStoreKey("foo", "bar")

	addr, _ := ts.SetKey(sk2)

	ts.SetKeyValueEx(sk, 230, 0, -1000*(1000*1000*1000), []StoreAddress{addr})
	ttl := ts.GetKeyTtl(sk)
	checkpoint := time.Now().UTC().UnixNano()
	if ttl < checkpoint {
		t.Error("relative ttl")
	}
	ts.SetKeyValueEx(sk, 630, 0, -1, []StoreAddress{addr})
	ts.SetMetadataAttribute(sk, "test", "cat")

	err := ts.Save(ts.l, "/test.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	err = ts2.Load(ts2.l, "/test.db")
	if err != nil {
		t.Errorf("load error %s", err.Error())
	}

	v, keyExists, valueExists := ts2.GetKeyValue(sk)
	if v != 630 || !keyExists || !valueExists {
		t.Error("latest value verify")
	}

	v, valueExists = ts2.GetKeyValueAtTime(sk, checkpoint)
	if v != 230 || !valueExists {
		t.Error("first value verify")
	}

	ttl2 := ts2.GetKeyTtl(sk)
	if ttl2 != ttl {
		t.Error("ttl verify")
	}

	exists, md := ts2.GetMetadataAttribute(sk, "test")
	if !exists || md != "cat" {
		t.Error("metadata verify")
	}

	hasLink, rv := ts2.GetRelationshipValue(sk, 0)
	if !hasLink || rv == nil || rv.CurrentValue != nil || rv.Sk.Path != "/foo/bar" {
		t.Error("follow relationship")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadAppVersionChange(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	err := ts.Save(ts.l, "/test/empty.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 1)
	err = ts2.Load(ts2.l, "/test/empty.db")
	if err == nil {
		t.Errorf("expected load error")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadIndexedValue(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 1)

	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.CreateAutoLink(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("index key count before")
	}

	err := ts.Save(ts.l, "/test/empty.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 1)
	err = ts2.Load(ts2.l, "/test/empty.db")
	if err != nil {
		t.Errorf("unexpected load error")
	}

	if countSubKeys(ts2, isk) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts2.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	vsk2 := MakeStoreKey("tree1", "source", "552")
	ts2.SetKey(vsk2)

	if countSubKeys(ts2, isk) != 2 {
		t.Error("index key count 2")
	}

	hasLink, rv = ts2.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "552"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/552" {
		t.Error("link verify 2")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSaveLoadIndexedValue2(t *testing.T) {
	fs = afero.NewMemMapFs()
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 1)

	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123", "user", "Joe")

	re, ic := ts.CreateAutoLink(dsk, isk, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("index key count before")
	}

	err := ts.Save(ts.l, "/test/empty.db")
	if err != nil {
		t.Errorf("save error %s", err.Error())
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}

	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 1)
	err = ts2.Load(ts2.l, "/test/empty.db")
	if err != nil {
		t.Errorf("unexpected load error")
	}

	if countSubKeys(ts2, isk) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts2.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Joe"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	vsk2 := MakeStoreKey("tree1", "source", "552", "user", "Mary")
	ts2.SetKey(vsk2)

	if countSubKeys(ts2, isk) != 2 {
		t.Error("index key count 2")
	}

	hasLink, rv = ts2.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/552" {
		t.Error("link verify 2")
	}

	if !ts2.DiagDump() {
		t.Error("final diag dump")
	}
}
