package treestore

import (
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestExpireKeyLong(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl missing key")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value missing key")
	}

	expireNs := int64(0x7FFFFFFFFFFFFFFF)
	addr, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, expireNs, nil)

	if addr == 0 || exists || orgVal != nil {
		t.Error("long expire set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireKeyValueLong(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	expireNs := int64(0x7FFFFFFFFFFFFFFF)
	addr, exists, orgVal := ts.SetKeyValueEx(sk, 100, 0, expireNs, nil)

	if addr == 0 || exists || orgVal != nil {
		t.Error("long expire set")
	}

	ttl := ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != expireNs {
		t.Error("ttl value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyExpireLong(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	expireNs := int64(0x7FFFFFFFFFFFFFFF)

	exists := ts.SetKeyTtl(sk, expireNs)
	if exists {
		t.Error("key exists")
	}

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl")
	}

	ts.SetKey(sk)

	ttl = ts.GetKeyTtl(sk)
	if ttl != 0 {
		t.Error("no ttl yet")
	}

	exists = ts.SetKeyTtl(sk, expireNs)
	if !exists {
		t.Error("key ttl set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("valid ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyExpireZero(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	expireNs := int64(0)

	ts.SetKey(sk)

	exists := ts.SetKeyTtl(sk, expireNs)
	if !exists {
		t.Error("key ttl set")
	}

	ttl := ts.GetKeyTtl(sk)
	if ttl != 0 {
		t.Error("no ttl should be set")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyExpireNegative(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	expireNs := int64(-1)

	ts.SetKey(sk)

	exists := ts.SetKeyTtl(sk, expireNs)
	if !exists {
		t.Error("key ttl set")
	}

	ttl := ts.GetKeyTtl(sk)
	if ttl != 0 {
		t.Error("no ttl should be set")
	}

	expireNs = int64(-100)

	exists = ts.SetKeyTtl(sk, expireNs)
	if !exists {
		t.Error("key ttl set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != 0 {
		t.Error("no ttl should be set")
	}

	expireNs = int64(0x7FFFFFFFFFFFFFFF)

	exists = ts.SetKeyTtl(sk, expireNs)
	if !exists {
		t.Error("key ttl set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("ttl should be set")
	}

	exists = ts.SetKeyTtl(sk, -100)
	if !exists {
		t.Error("key ttl set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("ttl should not have changed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyValueExpireLong(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	expireNs := int64(0x7FFFFFFFFFFFFFFF)

	exists := ts.SetKeyValueTtl(sk, expireNs)
	if exists {
		t.Error("key exists")
	}

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl")
	}

	ts.SetKeyValue(sk, 123)

	ttl = ts.GetKeyTtl(sk)
	if ttl != 0 {
		t.Error("no ttl yet")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != 0 {
		t.Error("no value ttl yet")
	}

	exists = ts.SetKeyValueTtl(sk, expireNs)
	if !exists {
		t.Error("key value ttl set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("valid ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != expireNs {
		t.Error("ttl value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireKeyShort(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl missing key")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value missing key")
	}

	expireNs := time.Now().UTC().UnixNano() + (15 * nsPerSec)
	addr, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, expireNs, nil)

	if addr == 0 || exists || orgVal != nil {
		t.Error("short expire set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value")
	}

	addr, exists = ts.LocateKey(sk)
	if addr != 2 || !exists {
		t.Error("locate expired")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireKeyInstant(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl missing key")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value missing key")
	}

	expireNs := time.Now().UTC().UnixNano() + 1
	addr, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, expireNs, nil)

	if addr == 0 || exists || orgVal != nil {
		t.Error("instant expire set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value")
	}

	addr, exists = ts.LocateKey(sk)
	if addr != 0 || exists {
		t.Error("locate expired")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireKeyValueShort(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl missing key")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value missing key")
	}

	expireNs := time.Now().UTC().UnixNano() + (15 * nsPerSec)
	addr, exists, orgVal := ts.SetKeyValueEx(sk, 100, 0, expireNs, nil)

	if addr == 0 || exists || orgVal != nil {
		t.Error("short expire set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
		t.Error("get ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != expireNs {
		t.Error("get ttl value")
	}

	addr, exists = ts.LocateKey(sk)
	if addr != 2 || !exists {
		t.Error("locate expired")
	}

	addr, exists = ts.IsKeyIndexed(sk)
	if addr != 2 || !exists {
		t.Error("index expired")
	}

	value, keyExists, valueExists := ts.GetKeyValue(sk)
	if value != 100 || !keyExists || !valueExists {
		t.Error("get value expired")
	}

	exists = ts.SetKeyValueTtl(sk, 1)
	if !exists {
		t.Error("set ttl")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireKeyValueInstant(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	ttl := ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("ttl missing key")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("ttl value missing key")
	}

	expireNs := time.Now().UTC().UnixNano() + 1
	addr, exists, orgVal := ts.SetKeyValueEx(sk, 100, 0, expireNs, nil)

	if addr == 0 || exists || orgVal != nil {
		t.Error("instant expire set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != -1 {
		t.Error("get ttl")
	}

	ttl = ts.GetKeyValueTtl(sk)
	if ttl != -1 {
		t.Error("get ttl value")
	}

	addr, exists = ts.LocateKey(sk)
	if addr != 0 || exists {
		t.Error("locate expired")
	}

	addr, exists = ts.IsKeyIndexed(sk)
	if addr != 0 || exists {
		t.Error("index expired")
	}

	value, keyExists, valueExists := ts.GetKeyValue(sk)
	if value != nil || keyExists || valueExists {
		t.Error("get value expired")
	}

	exists = ts.SetKeyValueTtl(sk, 1)
	if exists {
		t.Error("set ttl")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireSetTtlNoOp(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	ts.SetKey(sk)

	exists := ts.SetKeyTtl(sk, -1)
	if !exists {
		t.Error("set negative ttl")
	}

	ttl := ts.GetKeyTtl(sk)
	if ttl != 0 {
		t.Error("verify no ttl change")
	}

	exists = ts.SetKeyTtl(MakeStoreKey(), 10)
	if !exists {
		t.Error("set sentinel ttl ignored")
	}

	ttl = ts.GetKeyTtl(MakeStoreKey())
	if ttl != 0 {
		t.Error("verify no sentinel ttl")
	}
}

func TestSetExOnExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	now := time.Now().UTC().UnixNano()

	address, exists, orgVal := ts.SetKeyValueEx(sk, 972, 0, now+(10*nsPerSec), nil)
	if address != 2 || exists || orgVal != nil {
		t.Error("value set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 729, 0, now, nil)
	if address != 2 || !exists || orgVal != 972 {
		t.Error("value set again")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 279, 0, now, nil)
	if address != 3 || exists || orgVal != nil {
		t.Error("expired value set")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOnExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	addr1, _ := ts.SetKey(sk)
	verifyAddr, _ := ts.LocateKey(sk)
	if verifyAddr != addr1 {
		t.Error("verify addr1")
	}

	now := time.Now().UTC().UnixNano()

	exists := ts.SetKeyTtl(sk, now)
	if !exists {
		t.Error("set ttl")
	}

	addr2, _ := ts.SetKey(sk)

	verifyAddr, _ = ts.LocateKey(sk)
	if verifyAddr != addr2 {
		t.Error("verify addr2")
	}

	if addr1 == addr2 {
		t.Error("repurpose check")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyValueOnExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	addr1, _ := ts.SetKeyValue(sk, 40)
	verifyAddr, _ := ts.LocateKey(sk)
	if verifyAddr != addr1 {
		t.Error("verify addr1")
	}

	now := time.Now().UTC().UnixNano()

	exists := ts.SetKeyTtl(sk, now)
	if !exists {
		t.Error("set ttl")
	}

	addr2, _ := ts.SetKeyValue(sk, 80)

	verifyAddr, _ = ts.LocateKey(sk)
	if verifyAddr != addr2 {
		t.Error("verify addr2")
	}

	if addr1 == addr2 {
		t.Error("repurpose check")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetMetadataOnExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	addr1, _ := ts.SetKeyValue(sk, 40)
	verifyAddr, _ := ts.LocateKey(sk)
	if verifyAddr != addr1 {
		t.Error("verify addr1")
	}

	now := time.Now().UTC().UnixNano()

	exists := ts.SetKeyTtl(sk, now)
	if !exists {
		t.Error("set ttl")
	}

	exists, _ = ts.SetMetadataAttribute(sk, "attr", "test")

	if exists {
		t.Error("expired set")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestIterateLevelExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")
	root := MakeStoreKey()

	addr1, _ := ts.SetKeyValue(sk, 40)
	verifyAddr, _ := ts.LocateKey(sk)
	if verifyAddr != addr1 {
		t.Error("verify addr1")
	}

	keys := ts.GetLevelKeys(root, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].Segment) != "test" {
		t.Error("one node")
	}

	now := time.Now().UTC().UnixNano()

	exists := ts.SetKeyTtl(sk, now)
	if !exists {
		t.Error("set ttl")
	}

	keys = ts.GetLevelKeys(root, "*", 0, 100)
	if len(keys) != 0 {
		t.Error("zero nodes")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMatchingKeysExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")
	pattern := MakeStoreKey("**")

	ts.SetKeyValue(sk, 40)

	keys := ts.GetMatchingKeys(pattern, 0, 10)
	if keys == nil || len(keys) != 1 {
		t.Error("before ttl")
	}

	now := time.Now().UTC().UnixNano()

	exists := ts.SetKeyTtl(sk, now)
	if !exists {
		t.Error("set ttl")
	}

	keys = ts.GetMatchingKeys(pattern, 0, 10)
	if keys == nil || len(keys) != 0 {
		t.Error("after ttl")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMatchingKeyValuesExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")
	pattern := MakeStoreKey("**")

	ts.SetKeyValue(sk, 40)

	values := ts.GetMatchingKeyValues(pattern, 0, 10)
	if values == nil || len(values) != 1 {
		t.Error("before ttl")
	}

	now := time.Now().UTC().UnixNano()

	exists := ts.SetKeyTtl(sk, now)
	if !exists {
		t.Error("set ttl")
	}

	values = ts.GetMatchingKeyValues(pattern, 0, 10)
	if values == nil || len(values) != 0 {
		t.Error("after ttl")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
