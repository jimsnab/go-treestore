package treestore

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestSetJsonSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	replaced, err := ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("set first")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "cat" {
		t.Error("first val verify")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	replaced, err = ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("set second")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "dog" {
		t.Error("second val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestSetJsonValueTypes(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": 123}`)

	replaced, err := ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("set first")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != float64(123) {
		t.Error("first val verify")
	}

	jsonData = []byte(`{"pet": true}`)

	replaced, err = ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("set second")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != true {
		t.Error("second val verify")
	}

	jsonData = []byte(`{"pet": null}`)

	replaced, err = ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("set third")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != nil {
		t.Error("third val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestSetJsonValueTypesMerge(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": 123}`)

	err := ts.MergeKeyJson(sk, jsonData)
	if err != nil {
		t.Error("merge first")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != float64(123) {
		t.Error("first val verify")
	}

	jsonData = []byte(`{"pet": true}`)

	err = ts.MergeKeyJson(sk, jsonData)
	if err != nil {
		t.Error("merge second")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != true {
		t.Error("second val verify")
	}

	jsonData = []byte(`{"pet": null}`)

	err = ts.MergeKeyJson(sk, jsonData)
	if err != nil {
		t.Error("merge third")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != nil {
		t.Error("third val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestSetJsonValueTypesMergeOverwrite(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "pet")

	ts.SetKeyValueEx(sk2, 500, 0, 0, []StoreAddress{1})

	jsonData := []byte(`{"pet": "cat"}`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("first json")
	}

	val, ke, ve := ts.GetKeyValue(sk2)
	if !ke || !ve || val != "cat" {
		t.Error("merge overwrite val")
	}

	hasLink, rv := ts.GetRelationshipValue(sk2, 0)
	if hasLink || rv != nil {
		t.Error("relationship")
	}

	tm := time.Now().UTC().Add(time.Hour)
	ts.SetKeyValueEx(sk2, 500, 0, tm.UnixNano(), nil)

	ttl := ts.GetKeyTtl(sk2)
	if ttl == 0 {
		t.Error("set ttl")
	}

	replaced, err = ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("second json")
	}

	ttl = ts.GetKeyTtl(sk2)
	if ttl != 0 {
		t.Error("set ttl")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoJsons(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey()

	jsonData := []byte(`{"pet": "cat"}`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`{"pet": { "cat": "meow" }}`)
	replaced, err = ts.SetKeyJson(sk, jsonData2)
	if !replaced || err != nil {
		t.Error("second json")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeSentinelValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey()

	jsonData := []byte(`100`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("first json")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(100) {
		t.Error("first verify")
	}

	jsonData2 := []byte(`null`)
	replaced, err = ts.SetKeyJson(sk, jsonData2)
	if !replaced || err != nil {
		t.Error("second json")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != nil {
		t.Error("second verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeSentinelArray(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey()

	jsonData := []byte(`["test", "123"]`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("first json")
	}

	index0 := []byte{0, 0, 0, 0}
	index1 := []byte{0, 0, 0, 1}

	sk1 := MakeStoreKeyFromTokenSegments(index0)
	sk2 := MakeStoreKeyFromTokenSegments(index1)

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val != "test" {
		t.Error("first verify")
	}

	val, ke, ve = ts.GetKeyValue(sk2)
	if !ke || !ve || val != "123" {
		t.Error("second verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestJsonSetTree(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey()

	jsonData := []byte(`{"test":{"animals":[{"type":"cat"},{"type":"dog","food":"purina"}]}}`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("first json")
	}

	verifyData, err := ts.GetKeyAsJson(sk)
	if verifyData == nil || err != nil {
		t.Error("return json")
	}

	var parsed any
	json.Unmarshal(jsonData, &parsed)
	canonicalOrg, _ := json.Marshal(parsed)

	json.Unmarshal(verifyData, &parsed)
	canonicalGet, _ := json.Marshal(parsed)

	if !bytes.Equal(canonicalOrg, canonicalGet) {
		t.Error("round trip")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoArrays(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`["cow", "pig"]`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`["horse", "duck", "cow"]`)
	err = ts.MergeKeyJson(sk, jsonData2)
	if err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00`)
	sk2 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x01`)
	sk3 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x02`)

	val1, _, _ := ts.GetKeyValue(sk1)
	val2, _, _ := ts.GetKeyValue(sk2)
	val3, _, _ := ts.GetKeyValue(sk3)
	if val1 != "horse" || val2 != "duck" || val3 != "cow" {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoArrays2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`["cow", "pig"]`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`["horse"]`)
	err = ts.MergeKeyJson(sk, jsonData2)
	if err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00`)

	val1, _, _ := ts.GetKeyValue(sk1)
	if val1 != "horse" {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoArrays3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`[{"animals": {"cow": true, "pig": true}}]`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`[{"animals": {"horse": true}}]`)
	err = ts.MergeKeyJson(sk, jsonData2)
	if err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00/animals/horse`)
	sk2 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00/animals/cow`)
	sk3 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00/animals/pig`)

	val1, _, _ := ts.GetKeyValue(sk1)
	val2, _, _ := ts.GetKeyValue(sk2)
	val3, _, _ := ts.GetKeyValue(sk3)
	if val1 != true || val2 != true || val3 != true {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoMaps(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`{"animals": {"cow": true, "pig": true}}`)
	replaced, err := ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`{"animals": {"horse": true}}`)
	err = ts.MergeKeyJson(sk, jsonData2)
	if err != nil {
		t.Error("second json")
	}

	ts.DiagDump()

	sk1 := MakeStoreKeyFromPath(`/test/farm/animals/horse`)
	sk2 := MakeStoreKeyFromPath(`/test/farm/animals/cow`)
	sk3 := MakeStoreKeyFromPath(`/test/farm/animals/pig`)

	val1, _, _ := ts.GetKeyValue(sk1)
	val2, _, _ := ts.GetKeyValue(sk2)
	val3, _, _ := ts.GetKeyValue(sk3)
	if val1 != true || val2 != true || val3 != true {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestReplaceJsonSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	replaced, err := ts.ReplaceKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("replace before exists")
	}

	replaced, err = ts.SetKeyJson(sk, jsonData)
	if replaced || err != nil {
		t.Error("set first")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	replaced, err = ts.ReplaceKeyJson(sk, jsonData)
	if !replaced || err != nil {
		t.Error("replace after exists")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "dog" {
		t.Error("second val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestCreateJsonSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	created, err := ts.CreateKeyJson(sk, jsonData)
	if !created || err != nil {
		t.Error("create first")
	}

	created, err = ts.CreateKeyJson(sk, jsonData)
	if created || err != nil {
		t.Error("create second")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "cat" {
		t.Error("val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestCreateJsonExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()))
	sk := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "pet")

	jsonData := []byte(`{"pet": "cat"}`)

	created, err := ts.CreateKeyJson(sk, jsonData)
	if !created || err != nil {
		t.Error("create first")
	}

	ts.SetKeyTtl(sk, 1)

	created, err = ts.CreateKeyJson(sk, jsonData)
	if created || err != nil {
		t.Error("create second")
	}

	ts.SetKeyTtl(sk2, 1)

	created, err = ts.CreateKeyJson(sk, jsonData)
	if !created || err != nil {
		t.Error("create third")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "cat" {
		t.Error("val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}
