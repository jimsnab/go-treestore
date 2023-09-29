package treestore

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestPurgeEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	doc, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	ts.Purge()

	doc2, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(doc, doc2) {
		t.Error("not empty doc")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestPurgeSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	doc, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	ts.SetKeyValueEx(MakeStoreKey(), 123, 0, 0, []StoreAddress{1})

	doc2, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(doc, doc2) {
		t.Error("doc should change")
	}

	ts.Purge()

	doc3, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(doc, doc3) {
		t.Error("not empty doc")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestPurgeKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	doc, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	sk := MakeStoreKey("cat", "dog", "mouse")

	ts.SetKeyValueEx(sk, 123, 0, 0, []StoreAddress{1})

	doc2, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(doc, doc2) || !strings.Contains(string(doc2), "mouse") {
		t.Error("doc should change")
	}

	ts.Purge()

	doc3, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(doc, doc3) {
		t.Error("not empty doc")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestPurgeMetadata(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	doc, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	sk := MakeStoreKey("cat", "dog", "mouse")

	ts.SetKeyValueEx(sk, 123, 0, 0, []StoreAddress{1})

	ts.SetMetadataAttribute(sk, "fox", "true")

	doc2, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(doc, doc2) || !strings.Contains(string(doc2), "fox") {
		t.Error("doc should change")
	}

	ts.Purge()

	doc3, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(doc, doc3) {
		t.Error("not empty doc")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestPurgeHistory(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	doc, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	sk := MakeStoreKey("cat", "dog", "mouse")

	ts.SetKeyValueEx(sk, "frog", 0, 0, []StoreAddress{1})
	ts.SetKeyValueEx(sk, "cow", 0, 0, []StoreAddress{1})

	doc2, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(doc, doc2) || !strings.Contains(string(doc2), "frog") || !strings.Contains(string(doc2), "cow") {
		t.Error("doc should change")
	}

	ts.Purge()

	doc3, err := ts.Export(MakeStoreKey())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(doc, doc3) {
		t.Error("not empty doc")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}
