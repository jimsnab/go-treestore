package treestore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestImportExportEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data, err := ts.Export(MakeStoreKey())
	if data == nil || err != nil {
		t.Fatal("export empty")
	}

	err = ts2.Import(MakeStoreKey(), data)
	if err != nil {
		t.Error("import empty")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	addr, _, _ := ts.SetKeyValueEx(MakeStoreKey("dead"), nil, SetExNoValueUpdate, 1, nil)
	if addr != 2 {
		t.Error("create expired")
	}

	data, err := ts.Export(MakeStoreKey())
	if data == nil || err != nil {
		t.Fatal("export expired")
	}

	err = ts2.Import(MakeStoreKey(), data)
	if err != nil {
		t.Error("import expired")
	}

	addr, _, _ = ts2.SetKeyValueEx(MakeStoreKey("dead"), nil, SetExNoValueUpdate, 1, nil)
	if addr != 2 {
		t.Error("create again")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	ts.SetKeyValue(sk, "test")

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export sentinel")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import sentinel")
	}

	v, ke, ve := ts.GetKeyValue(sk)
	if v != "test" || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueBytes(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	ts.SetKeyValue(sk, []byte("test"))

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export value bytes")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import value bytes")
	}

	v, ke, ve := ts.GetKeyValue(sk)
	if !bytes.Equal(v.([]byte), []byte("test")) || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueJson(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := []string{"test"}
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export json")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import json")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.([]string)
	if len(val) != 1 || val[0] != "test" || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueBase64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := []byte{1, 2, 3}
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export base64")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import base64")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.([]byte)
	if !bytes.Equal(v, val) || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := 123
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.(int)
	if v != val || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueInt64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := int64(123)
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.(int64)
	if v != val || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueUint64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := uint64(123)
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.(uint64)
	if v != val || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueBool(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := true
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.(bool)
	if v != val || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelValueFloat64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	v := float64(123)
	ts.SetKeyValue(sk, v)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import")
	}

	v2, ke, ve := ts.GetKeyValue(sk)
	val, _ := v2.(float64)
	if v != val || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	ts.SetKeyValueEx(sk, "test", 0, 0, []StoreAddress{1})

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export relationship")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import relationship")
	}

	hasLink, rv := ts.GetRelationshipValue(sk, 0)
	if !hasLink || rv.CurrentValue != "test" || len(rv.Sk.Tokens) != 0 {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSentinelExpiration(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	now := time.Now().UTC().Add(time.Hour)
	ttl := now.UnixNano()
	ts.SetKeyValueEx(sk, "test", 0, ttl, nil)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export expiration")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import expiration")
	}

	ttl2 := ts.GetKeyValueTtl(sk)
	if ttl2 != ttl {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportRootValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")

	ts.SetKeyValue(sk, "meow")

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export root")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import root")
	}

	v, ke, ve := ts.GetKeyValue(sk)
	if v != "meow" || !ke || !ve {
		t.Error("value verify")
	}
}

func TestImportExportRootValueImportChild(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("cat")
	sk2 := MakeStoreKey("feline")

	ts.SetKeyValue(sk, "meow")

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export change child")
	}

	err = ts2.Import(sk2, data)
	if err != nil {
		t.Error("import change child")
	}

	v, ke, ve := ts2.GetKeyValue(sk2)
	if v != "meow" || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportRootValueImportChild2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	oldRoot := MakeStoreKey()
	sk := MakeStoreKey("cat")
	newRoot := MakeStoreKey("animals")
	sk2 := MakeStoreKey("animals", "cat")

	ts.SetKeyValue(sk, "meow")

	data, err := ts.Export(oldRoot)
	if data == nil || err != nil {
		t.Fatal("export move")
	}

	err = ts2.Import(newRoot, data)
	if err != nil {
		t.Error("import move")
	}

	v, ke, ve := ts2.GetKeyValue(sk2)
	if v != "meow" || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportRootValueImportChild3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	oldRoot := MakeStoreKey()
	sk := MakeStoreKey("cat")
	newRoot := MakeStoreKey("animals")
	sk2 := MakeStoreKey("animals", "cat")

	ts.SetKeyValue(sk, "meow")

	data, err := ts.Export(oldRoot)
	if data == nil || err != nil {
		t.Fatal("export replace")
	}

	ts2.SetKeyValue(sk2, "hiss")

	err = ts2.Import(newRoot, data)
	if err != nil {
		t.Error("import replace")
	}

	v, ke, ve := ts2.GetKeyValue(sk2)
	if v != "meow" || !ke || !ve {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportRootValueImportChild4(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	oldRoot := MakeStoreKey()
	sk := MakeStoreKey("cat")
	newRoot := MakeStoreKey("animals")
	sk2 := MakeStoreKey("animals", "cat")
	sk3 := MakeStoreKey("animals", "cat", "name")

	ts.SetKeyValue(sk, "meow")

	data, err := ts.Export(oldRoot)
	if data == nil || err != nil {
		t.Fatal("export discard")
	}

	ts2.SetKeyValue(sk3, "mittens")

	err = ts2.Import(newRoot, data)
	if err != nil {
		t.Error("import discard")
	}

	v, ke, ve := ts2.GetKeyValue(sk2)
	if v != "meow" || !ke || !ve {
		t.Error("value verify")
	}

	v, ke, ve = ts2.GetKeyValue(sk3)
	if v != nil || ke || ve {
		t.Error("removed value")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportInvalidRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	ts.SetKeyValueEx(sk, "test", 0, 0, []StoreAddress{100})

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export invalid")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import invalid")
	}

	hasLink, rv := ts.GetRelationshipValue(sk, 0)
	if !hasLink || rv != nil {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportReferenceSelf(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("testing")

	addr, _ := ts.SetKey(sk)
	ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, []StoreAddress{addr})

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export self")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import self")
	}

	hasLink, rv := ts.GetRelationshipValue(sk, 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/testing" {
		t.Error("value verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportReferenceRelativeInvalid(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("farm", "animals", "pig")
	sk2 := MakeStoreKey("farm", "animals", "horse")
	sk3 := MakeStoreKey("farm", "animals", "cow")
	sk4 := MakeStoreKey("livestock")

	addr1, _ := ts.SetKey(sk1)
	addr2, _ := ts.SetKey(sk2)
	addr3, _ := ts.SetKey(sk3)

	ts.SetKeyValueEx(sk4, nil, SetExNoValueUpdate, 0, []StoreAddress{addr1, addr2, addr3})

	data, err := ts.Export(MakeStoreKeyFromPath("/livestock"))
	if data == nil || err != nil {
		t.Fatal("export relative")
	}

	err = ts2.Import(MakeStoreKeyFromPath("/test"), data)
	if err != nil {
		t.Error("import relative invalid")
	}

	hasLink, rv := ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test"), 0)
	if !hasLink || rv != nil {
		t.Error("relationship verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportReferenceRelative(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("farm", "animals", "pig")
	sk2 := MakeStoreKey("farm", "animals", "horse")
	sk3 := MakeStoreKey("farm", "animals", "cow")
	sk4 := MakeStoreKey("farm", "livestock")

	addr1, _ := ts.SetKey(sk1)
	addr2, _ := ts.SetKey(sk2)
	addr3, _ := ts.SetKey(sk3)

	ts.SetKeyValueEx(sk4, nil, SetExNoValueUpdate, 0, []StoreAddress{addr1, addr2, addr3})

	data, err := ts.Export(MakeStoreKeyFromPath("/farm"))
	if data == nil || err != nil {
		t.Fatal("export relative")
	}

	err = ts2.Import(MakeStoreKeyFromPath("/test"), data)
	if err != nil {
		t.Error("import relative invalid")
	}

	hasLink, rv := ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/livestock"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/animals/pig" {
		if rv == nil {
			t.Error("relationship verify /test/livestock not followed")
		} else {
			t.Error("relationship verify " + rv.Sk.Path)
		}
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportReferenceAbs(t *testing.T) {
	oldHook := invalidAddrHook
	defer func() {
		invalidAddrHook = oldHook
	}()
	invalidAddrHook = func() { panic("invalid relationship address") }

	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("farm", "animals", "pig")
	sk2 := MakeStoreKey("farm", "animals", "horse")
	sk3 := MakeStoreKey("farm", "animals", "cow")
	sk4 := MakeStoreKey("livestock")
	sk5 := MakeStoreKey("abc")

	addr1, _ := ts.SetKey(sk1)
	addr2, _ := ts.SetKey(sk2)
	addr3, _ := ts.SetKey(sk3)

	if addr1 != 4 || addr2 != 5 || addr3 != 6 {
		t.Fatal("unexpected addresses")
	}

	addr, exists, orgVal := ts.SetKeyValueEx(sk4, nil, SetExNoValueUpdate, 0, []StoreAddress{addr1, addr2, addr3})
	if addr != 7 || exists || orgVal != nil {
		t.Fatal("setex sk4")
	}

	addr, exists, orgVal = ts.SetKeyValueEx(sk5, nil, SetExNoValueUpdate, 0, []StoreAddress{addr1, addr2, addr3})
	if addr != 8 || exists || orgVal != nil {
		t.Fatal("setex sk5")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}

	data, err := ts.Export(MakeStoreKey())
	if data == nil || err != nil {
		t.Fatal("export relative")
	}

	err = ts2.Import(MakeStoreKeyFromPath("/test"), data)
	if err != nil {
		t.Error("import relative invalid")
	}

	hasLink, rv := ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/livestock"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/farm/animals/pig" {
		t.Error("relationship verify")
	}

	hasLink, rv = ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/livestock"), 1)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/farm/animals/horse" {
		t.Error("relationship verify")
	}

	hasLink, rv = ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/livestock"), 2)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/farm/animals/cow" {
		t.Error("relationship verify")
	}

	hasLink, rv = ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/abc"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/farm/animals/pig" {
		t.Error("relationship verify")
	}

	hasLink, rv = ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/abc"), 1)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/farm/animals/horse" {
		t.Error("relationship verify")
	}

	hasLink, rv = ts2.GetRelationshipValue(MakeStoreKeyFromPath("/test/abc"), 2)
	if !hasLink || rv == nil || rv.Sk.Path != "/test/farm/animals/cow" {
		t.Error("relationship verify")
	}

	if !ts2.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportSaveRecoverInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	raw := []byte(`{"children":{"test":{"history":[{"timestamp":0,"value":"123","type":"int"}]}}}`)

	err := ts.Import(sk, raw)
	if err != nil {
		t.Error("import")
	}

	sk2 := MakeStoreKey("test")
	v2, ke, ve := ts.GetKeyValue(sk2)
	val, _ := v2.(int)
	if val != 123 || !ke || !ve {
		t.Error("value verify")
	}

	serialized, err := ts.Export(sk)
	if err != nil {
		t.Error("export")
	}
	if !bytes.Equal(raw, serialized) {
		t.Error("round trip")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestImportExportIndexedValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export root")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import root")
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

func TestImportExportIndexedValue2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123", "user", "Joe")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	data, err := ts.Export(sk)
	if data == nil || err != nil {
		t.Fatal("export root")
	}

	err = ts2.Import(sk, data)
	if err != nil {
		t.Error("import root")
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
