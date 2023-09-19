package treestore

import (
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestMoveKeyBasic(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyCrossTree(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("tree1", "source")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("tree2", "target")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeySelf(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("tree1", "source")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("tree1", "source")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || moved {
		t.Error("shouldn't move")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeySelfOverwrite(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("tree1", "source")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("tree1", "source")
	exists, moved := ts.MoveKey(ssk, dsk, true)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyDestExists(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")
	address, firstValue = ts.SetKeyValue(dsk, 345)
	if address == 0 || !firstValue {
		t.Error("second set")
	}

	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || moved {
		t.Error("moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyDestExistsOverwrite(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")
	address, firstValue = ts.SetKeyValue(dsk, 345)
	if address == 0 || !firstValue {
		t.Error("second set")
	}

	exists, moved := ts.MoveKey(ssk, dsk, true)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyDestExistsDiscard(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("src set")
	}

	osk := MakeStoreKey("target", "other")
	address, firstValue = ts.SetKeyValue(osk, 123)
	if address == 0 || !firstValue {
		t.Error("other set")
	}

	dsk := MakeStoreKey("target")
	address, firstValue = ts.SetKeyValue(dsk, 345)
	if address == 0 || !firstValue {
		t.Error("second set")
	}

	exists, moved := ts.MoveKey(ssk, dsk, true)
	if !exists || !moved {
		t.Error("not moved")
	}

	address, exists = ts.LocateKey(osk)
	if address != 0 || exists {
		t.Error("other key should have been lost")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyDestNested(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	dsk := MakeStoreKey("test", "target")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyDestNestedOverwite(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("src set")
	}

	osk := MakeStoreKey("test", "other")

	address, firstValue = ts.SetKeyValue(osk, 555)
	if address == 0 || !firstValue {
		t.Error("other set")
	}

	dsk := MakeStoreKey("test", "target")
	address, firstValue = ts.SetKeyValue(dsk, 345)
	if address == 0 || !firstValue {
		t.Error("dest set")
	}

	exists, moved := ts.MoveKey(ssk, dsk, true)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeySentinelIn(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("staged")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("src set")
	}

	dsk := MakeStoreKey()
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || moved {
		t.Error("must always overwrite sentinel")
	}

	exists, moved = ts.MoveKey(ssk, dsk, true)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeySentinelIn2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("staged")

	address, exists, ov := ts.SetKeyValueEx(ssk, 123, 0, time.Now().Add(time.Millisecond*30).UnixNano(), []StoreAddress{1, 2})
	if address != 2 || exists || ov != nil {
		t.Error("src set")
	}

	ts.SetMetadataAttribute(ssk, "test", "abc")

	dsk := MakeStoreKey()
	exists, moved := ts.MoveKey(ssk, dsk, true)
	if !exists || !moved {
		t.Error("not moved")
	}

	hasLink, rv := ts.GetRelationshipValue(dsk, 0)
	if !hasLink || rv == nil || rv.Sk.Path != "" {
		t.Error("relationship to sentinel")
	}

	hasLink, rv = ts.GetRelationshipValue(dsk, 1)
	if !hasLink || rv == nil || rv.Sk.Path != "" {
		t.Error("relationship to node")
	}

	time.Sleep(time.Millisecond * 31) // sentinel does not expire
	v, ke, ve := ts.GetKeyValue(dsk)
	if !ke || !ve || v != 123 {
		t.Error("fetch sentinel value")
	}

	ae, mv := ts.GetMetadataAttribute(dsk, "test")
	if !ae || mv != "abc" {
		t.Error("fetch metadata")
	}

	ae, mv = ts.GetMetadataAttribute(ssk, "test")
	if ae || mv != "" {
		t.Error("src metadata should be gone")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeySentinelOut(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey()

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("src set")
	}

	dsk := MakeStoreKey("sentinel")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || !moved {
		t.Error("not moved")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyExtended(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test", "staged")

	address, exists, ov := ts.SetKeyValueEx(ssk, 123, 0, time.Now().Add(time.Millisecond*30).UnixNano(), []StoreAddress{1, 3})
	if address != 3 || exists || ov != nil {
		t.Error("src set")
	}

	ts.SetMetadataAttribute(ssk, "test", "abc")

	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if !exists || !moved {
		t.Error("not moved")
	}

	hasLink, rv := ts.GetRelationshipValue(dsk, 0)
	if !hasLink || rv == nil || rv.Sk.Path != "" {
		t.Error("relationship to sentinel")
	}

	hasLink, rv = ts.GetRelationshipValue(dsk, 1)
	if !hasLink || rv == nil || rv.Sk.Path != "/target" {
		t.Error("relationship to node")
	}

	ae, mv := ts.GetMetadataAttribute(dsk, "test")
	if !ae || mv != "abc" {
		t.Error("fetch metadata")
	}

	time.Sleep(time.Millisecond * 31)
	v, ke, ve := ts.GetKeyValue(dsk)
	if ke || ve || v != nil {
		t.Error("fetch expired value")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, exists, _ := ts.SetKeyValueEx(ssk, 123, 0, 1, nil)
	if address == 0 || exists {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveKey(ssk, dsk, false)
	if exists || moved {
		t.Error("shouldn't exist")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyReferenced(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")
	rsk := MakeStoreKey("index")

	address, firstValue := ts.SetKeyValue(ssk, 123)
	if address == 0 || !firstValue {
		t.Error("first set")
	}

	raddr, _, _ := ts.SetKeyValueEx(rsk, nil, SetExNoValueUpdate, 0, []StoreAddress{address})
	if raddr == 0 {
		t.Error("relationship set")
	}

	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, -1, []StoreKey{rsk}, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	hasLink, rv := ts.GetRelationshipValue(rsk, 0)
	if !hasLink || rv.Sk.Path != "/target" {
		t.Error("relationship wrong")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyTempTtl(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	expiration := time.Now().Add(time.Minute).UnixNano()
	address, _, _ := ts.SetKeyValueEx(ssk, 123, 0, expiration, nil)
	if address == 0 {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, 0, nil, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	ttl := ts.GetKeyTtl(dsk)
	if ttl != 0 {
		t.Error("ttl not cleared")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyTempTtl2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")

	address, _, _ := ts.SetKeyValueEx(ssk, 123, 0, 0, nil)
	if address == 0 {
		t.Error("first set")
	}

	expiration := time.Now().Add(-time.Minute).UnixNano()
	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, expiration, nil, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	ttl := ts.GetKeyTtl(dsk)
	if ttl != -1 {
		t.Error("key not expired")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyTempTtl3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")
	rsk1 := MakeStoreKey("index1")
	rsk2 := MakeStoreKey("index2")

	expiration := time.Now().Add(time.Minute).UnixNano()
	address, _, _ := ts.SetKeyValueEx(ssk, 123, 0, expiration, nil)
	if address == 0 {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, 0, []StoreKey{rsk1, rsk2}, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	ttl := ts.GetKeyTtl(dsk)
	if ttl != 0 {
		t.Error("dsk ttl not cleared")
	}

	ttl = ts.GetKeyTtl(rsk1)
	if ttl != 0 {
		t.Error("rsk1 ttl not zero")
	}

	ttl = ts.GetKeyTtl(rsk2)
	if ttl != 0 {
		t.Error("rsk2 ttl not zero")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyTempTtl4(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")
	rsk1 := MakeStoreKey("index1")
	rsk2 := MakeStoreKey("index2")

	address, _ := ts.SetKey(ssk)
	if address == 0 {
		t.Error("first set")
	}

	dsk := MakeStoreKey("target")

	expiration := time.Now().Add(time.Minute).UnixNano()
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, expiration, []StoreKey{rsk1, rsk2}, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	ttl := ts.GetKeyTtl(dsk)
	if ttl != expiration {
		t.Error("dsk ttl not set")
	}

	ttl = ts.GetKeyTtl(rsk1)
	if ttl != expiration {
		t.Error("rsk1 ttl not set")
	}

	ttl = ts.GetKeyTtl(rsk2)
	if ttl != expiration {
		t.Error("rsk2 ttl not set")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyTempTtl5(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")
	rsk1 := MakeStoreKey("index1")
	rsk2 := MakeStoreKey("index2")

	address, _ := ts.SetKey(ssk)
	if address == 0 {
		t.Error("first set")
	}

	raddr1, _ := ts.SetKey(rsk1)
	if raddr1 == 0 {
		t.Error("raddr1 set")
	}

	raddr2, _, _ := ts.SetKeyValueEx(rsk1, nil, SetExNoValueUpdate, 1000, nil)
	if raddr2 == 0 {
		t.Error("raddr2 set")
	}

	dsk := MakeStoreKey("target")

	expiration := time.Now().Add(time.Minute).UnixNano()
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, expiration, []StoreKey{rsk1, rsk2}, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	ttl := ts.GetKeyTtl(dsk)
	if ttl != expiration {
		t.Error("dsk ttl not set")
	}

	ttl = ts.GetKeyTtl(rsk1)
	if ttl != expiration {
		t.Error("rsk1 ttl not set")
	}

	ttl = ts.GetKeyTtl(rsk2)
	if ttl != expiration {
		t.Error("rsk2 ttl not set")
	}

	hasLink, rv := ts.GetRelationshipValue(rsk1, 0)
	if !hasLink || rv.Sk.Path != "/target" {
		t.Error("relationship 1 wrong")
	}

	hasLink, rv = ts.GetRelationshipValue(rsk2, 0)
	if !hasLink || rv.Sk.Path != "/target" {
		t.Error("relationship 2 wrong")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveKeyTempTtl6(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")
	rsk1 := MakeStoreKey("index1")
	rsk2 := MakeStoreKey("index2")

	expiration := time.Now().Add(time.Minute).UnixNano()

	address, _ := ts.SetKey(ssk)
	if address == 0 {
		t.Error("first set")
	}

	raddr1, _ := ts.SetKey(rsk1)
	if raddr1 == 0 {
		t.Error("raddr1 set")
	}

	raddr2, _, _ := ts.SetKeyValueEx(rsk2, nil, SetExNoValueUpdate, expiration, []StoreAddress{address})
	if raddr2 == 0 {
		t.Error("raddr2 set")
	}

	dsk := MakeStoreKey("target")

	exp2 := expiration + time.Minute.Nanoseconds()
	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, exp2, []StoreKey{rsk1, rsk2}, nil)
	if !exists || !moved {
		t.Error("not moved")
	}

	ttl := ts.GetKeyTtl(dsk)
	if ttl != exp2 {
		t.Error("dsk ttl not set")
	}

	ttl = ts.GetKeyTtl(rsk1)
	if ttl != exp2 {
		t.Error("rsk1 ttl not set")
	}

	ttl = ts.GetKeyTtl(rsk2)
	if ttl != exp2 {
		t.Error("rsk2 ttl not set")
	}

	hasLink, rv := ts.GetRelationshipValue(rsk1, 0)
	if hasLink || rv != nil {
		t.Error("relationship 1 wrong")
	}

	hasLink, rv = ts.GetRelationshipValue(rsk2, 0)
	if !hasLink || rv.Sk.Path != "/target" {
		t.Error("relationship 2 wrong")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestMoveIndex(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	ssk := MakeStoreKey("test")
	rsk1 := MakeStoreKey("index1")
	rsk2 := MakeStoreKey("index2")

	address, _ := ts.SetKey(ssk)
	if address == 0 {
		t.Error("first set")
	}

	raddr1, _, _ := ts.SetKeyValueEx(rsk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address})
	if raddr1 == 0 {
		t.Error("raddr1 set")
	}

	dsk := MakeStoreKey("target")

	exists, moved := ts.MoveReferencedKey(ssk, dsk, false, -1, []StoreKey{rsk2}, []StoreKey{rsk1})
	if !exists || !moved {
		t.Error("not moved")
	}

	hasLink, rv := ts.GetRelationshipValue(rsk1, 0)
	if hasLink || rv != nil {
		t.Error("relationship 1 wrong")
	}

	hasLink, rv = ts.GetRelationshipValue(rsk2, 0)
	if !hasLink || rv.Sk.Path != "/target" {
		t.Error("relationship 2 wrong")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}
