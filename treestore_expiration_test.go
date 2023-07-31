package treestore

import (
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestExpireKeyLong(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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

func TestSetKeyValueExpireLong(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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
	if addr != 0 || exists {
		t.Error("locate expired")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireKeyValueShort(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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
		t.Error("short expire set")
	}

	ttl = ts.GetKeyTtl(sk)
	if ttl != expireNs {
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

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
