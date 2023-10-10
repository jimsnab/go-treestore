package treestore

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestReuseKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists := ts.SetKey(sk)
	if address != 2 || exists {
		t.Error("first set")
	}

	kr, vr, ov := ts.DeleteKey(sk)
	if !kr || vr || ov != nil {
		t.Error("delete")
	}

	address, exists = ts.SetKey(sk)
	if address != 3 || exists {
		t.Error("second set")
	}

	sk2, exists := ts.KeyFromAddress(2)
	if exists || len(sk2.Tokens) != 0 {
		t.Error("first exists")
	}

	sk2, exists = ts.KeyFromAddress(3)
	if !exists || sk2.Path != "/test" {
		t.Error("second !exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireReuseKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists := ts.SetKey(sk)
	if address != 2 || exists {
		t.Error("first set")
	}

	exists = ts.SetKeyTtl(sk, 1)
	if !exists {
		t.Error("expire")
	}

	sk2, exists := ts.KeyFromAddress(2)
	if exists || len(sk2.Tokens) != 0 {
		t.Error("ttl check")
	}

	address, exists = ts.SetKey(sk)
	if address != 3 || exists {
		t.Error("second set")
	}

	sk2, exists = ts.KeyFromAddress(2)
	if exists || len(sk2.Tokens) != 0 {
		t.Error("first exists")
	}

	sk2, exists = ts.KeyFromAddress(3)
	if !exists || sk2.Path != "/test" {
		t.Error("second !exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestExpireReuseValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, firstValue := ts.SetKeyValue(sk, 123)
	if address != 2 || !firstValue {
		t.Error("first set")
	}

	exists := ts.SetKeyTtl(sk, 1)
	if !exists {
		t.Error("expire")
	}

	sk2, exists := ts.KeyFromAddress(2)
	if exists || len(sk2.Tokens) != 0 {
		t.Error("ttl check")
	}

	address, firstValue = ts.SetKeyValue(sk, 456)
	if address != 3 || !firstValue {
		t.Error("second set")
	}

	sk2, exists = ts.KeyFromAddress(2)
	if exists || len(sk2.Tokens) != 0 {
		t.Error("first exists")
	}

	sk2, exists = ts.KeyFromAddress(3)
	if !exists || sk2.Path != "/test" {
		t.Error("second !exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
