package treestore

import (
	"testing"
	"time"
)

func TestExpireKeyLong(t *testing.T) {
	ts := NewTreeStore()

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
}

func TestExpireKeyValueLong(t *testing.T) {
	ts := NewTreeStore()

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
}

func TestSetKeyExpireLong(t *testing.T) {
	ts := NewTreeStore()

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
}

func TestSetKeyValueExpireLong(t *testing.T) {
	ts := NewTreeStore()

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
}

func TestExpireKeyShort(t *testing.T) {
	ts := NewTreeStore()

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
}

func TestExpireKeyValueShort(t *testing.T) {
	ts := NewTreeStore()

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

	addr, exists = ts.IsKeyIndexed(sk)
	if addr != 0 || exists {
		t.Error("index expired")
	}
}
