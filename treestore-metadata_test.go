package treestore

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestMetadataSet(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("test")

	ts.SetKey(sk)
	exists, orgVal := ts.SetMetadataAttribute(sk, "abc", "123")
	if !exists || orgVal != "" {
		t.Error("first attribute set")
	}

	exists, verifyVal := ts.GetMetadataAttribute(sk, "abc")
	if !exists || verifyVal != "123" {
		t.Error("verify first attribute")
	}

	exists, orgVal = ts.SetMetadataAttribute(sk, "abc", "456")
	if !exists || orgVal != "123" {
		t.Error("second attribute set")
	}

	exists, verifyVal = ts.GetMetadataAttribute(sk, "abc")
	if !exists || verifyVal != "456" {
		t.Error("verify second attribute")
	}

	exists, orgVal = ts.SetMetadataAttribute(MakeStoreKey("foo"), "abc", "890")
	if exists || orgVal != "" {
		t.Error("attribute attempt on missing key")
	}

	exists, verifyVal = ts.GetMetadataAttribute(MakeStoreKey("bar"), "abc")
	if exists || verifyVal != "" {
		t.Error("read attempt on missing key")
	}
}

func TestMetadataClear(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("test")

	ts.SetKey(sk)
	exists, orgVal := ts.SetMetadataAttribute(sk, "abc", "123")
	if !exists || orgVal != "" {
		t.Error("first attribute set")
	}

	exists, verifyVal := ts.GetMetadataAttribute(sk, "abc")
	if !exists || verifyVal != "123" {
		t.Error("verify first attribute")
	}

	attributeExists, orgVal := ts.ClearMetadataAttribute(sk, "nope")
	if attributeExists || orgVal != "" {
		t.Error("clear missing attribute")
	}

	attributeExists, orgVal = ts.ClearMetadataAttribute(sk, "abc")
	if !attributeExists || orgVal != "123" {
		t.Error("clear missing attribute")
	}

	exists, orgVal = ts.SetMetadataAttribute(sk, "abc", "456")
	if !exists || orgVal != "" {
		t.Error("second attribute set")
	}

	exists, verifyVal = ts.GetMetadataAttribute(sk, "abc")
	if !exists || verifyVal != "456" {
		t.Error("verify second attribute")
	}

	exists, orgVal = ts.SetMetadataAttribute(sk, "def", "100")
	if !exists || orgVal != "" {
		t.Error("third attribute set")
	}

	attributeExists, orgVal = ts.ClearMetadataAttribute(sk, "def")
	if !attributeExists || orgVal != "100" {
		t.Error("clear missing attribute")
	}

	exists, orgVal = ts.SetMetadataAttribute(sk, "def", "100")
	if !exists || orgVal != "" {
		t.Error("third attribute set")
	}

	ts.ClearKeyMetadata(MakeStoreKey("missing"))

	exists, verifyVal = ts.GetMetadataAttribute(sk, "abc")
	if !exists || verifyVal != "456" {
		t.Error("verify abc=456")
	}

	exists, verifyVal = ts.GetMetadataAttribute(sk, "def")
	if !exists || verifyVal != "100" {
		t.Error("verify def=100")
	}

	attribs := ts.GetMetadataAttributes(sk)
	if len(attribs) != 2 || attribs[0] != "abc" || attribs[1] != "def" {
		t.Error("verify retrieve attributes")
	}

	ts.ClearKeyMetadata(sk)

	attribs = ts.GetMetadataAttributes(sk)
	if len(attribs) != 0 || attribs == nil {
		t.Error("verify after clear key")
	}
}

func TestMetadataMissing(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey("missing")

	attribs := ts.GetMetadataAttributes(sk)
	if attribs != nil {
		t.Error("no attribs on missing key")
	}

	attribExists, value := ts.GetMetadataAttribute(sk, "something")
	if attribExists || value != "" {
		t.Error("no attribute value on missing key")
	}

	attribExists, value = ts.ClearMetadataAttribute(sk, "something")
	if attribExists || value != "" {
		t.Error("no attribute to clear on missing key")
	}
}

func TestMetadataSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))

	sk := MakeStoreKey()

	exists, orgVal := ts.SetMetadataAttribute(sk, "abc", "123")
	if !exists || orgVal != "" {
		t.Error("first attribute set")
	}

	exists, orgVal = ts.SetMetadataAttribute(sk, "abc", "321")
	if !exists || orgVal != "123" {
		t.Error("second attribute set")
	}

	attribs := ts.GetMetadataAttributes(sk)
	if len(attribs) != 1 || attribs[0] != "abc" {
		t.Error("verify sentinel metadata")
	}

	ts.ClearKeyMetadata(sk)

	attribs = ts.GetMetadataAttributes(sk)
	if len(attribs) != 0 || attribs == nil {
		t.Error("cleared sentinel metadata")
	}
}
