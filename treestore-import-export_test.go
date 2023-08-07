package treestore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestImportExportEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

	data, err := ts.Export(MakeStoreKey())
	if data == nil || err != nil {
		t.Fatal("export empty")
	}

	err = ts2.Import(MakeStoreKey(), data)
	if err != nil {
		t.Error("import empty")
	}

	ts2.DiagDump()
}

func TestImportExportSentinelValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueBytes(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueJson(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueBase64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueInt64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueUint64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueBool(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelValueFloat64(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportSentinelExpiration(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportRootValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportRootValueImportChild2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportRootValueImportChild3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportRootValueImportChild4(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportInvalidRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportReferenceSelf(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportReferenceRelativeInvalid(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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

	ts2.DiagDump()
}

func TestImportExportReferenceRelative(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

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
		t.Error("relationship verify")
	}

	ts2.DiagDump()
}

func TestImportExportReferenceAbs(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ts2 := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk1 := MakeStoreKey("farm", "animals", "pig")
	sk2 := MakeStoreKey("farm", "animals", "horse")
	sk3 := MakeStoreKey("farm", "animals", "cow")
	sk4 := MakeStoreKey("livestock")

	addr1, _ := ts.SetKey(sk1)
	addr2, _ := ts.SetKey(sk2)
	addr3, _ := ts.SetKey(sk3)

	ts.SetKeyValueEx(sk4, nil, SetExNoValueUpdate, 0, []StoreAddress{addr1, addr2, addr3})

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

	ts2.DiagDump()
}
