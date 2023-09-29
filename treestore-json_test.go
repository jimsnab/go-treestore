package treestore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestSetJsonSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("set first")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "cat" {
		t.Error("first val verify")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
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

func TestSetJsonSimpleStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	replaced, addr, err := ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set first")
	}

	addr, ke := ts.LocateKey(MakeStoreKey("test", "pet", "cat"))
	if !ke || addr != 3 {
		t.Error("first addr verify")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("set second")
	}

	addr, ke = ts.LocateKey(MakeStoreKey("test", "pet", "dog"))
	if !ke || addr != 6 {
		t.Error("second addr verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeJsonSimpleStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	addr, err := ts.MergeKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if addr == 0 || err != nil {
		t.Error("merge first")
	}

	addr, ke := ts.LocateKey(MakeStoreKey("test", "pet", "cat"))
	if !ke || addr != 4 {
		t.Error("first addr verify")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	addr, err = ts.MergeKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if addr == 0 || err != nil {
		t.Error("set second")
	}

	addr, ke = ts.LocateKey(MakeStoreKey("test", "pet", "cat"))
	if ke || addr != 0 {
		t.Error("first addr verify again")
	}

	addr, ke = ts.LocateKey(MakeStoreKey("test", "pet", "dog"))
	if !ke || addr != 5 {
		t.Error("second addr verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestSetJsonValueTypes(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": 123}`)

	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("set first")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != float64(123) {
		t.Error("first val verify")
	}

	jsonData = []byte(`{"pet": true}`)

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("set second")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != true {
		t.Error("second val verify")
	}

	jsonData = []byte(`{"pet": null}`)

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": 123}`)

	addr, err := ts.MergeKeyJson(sk, jsonData, 0)
	if addr == 0 || err != nil {
		t.Error("merge first")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != float64(123) {
		t.Error("first val verify")
	}

	jsonData = []byte(`{"pet": true}`)

	addr, err = ts.MergeKeyJson(sk, jsonData, 0)
	if addr == 0 || err != nil {
		t.Error("merge second")
	}

	val, ke, ve = ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != true {
		t.Error("second val verify")
	}

	jsonData = []byte(`{"pet": null}`)

	addr, err = ts.MergeKeyJson(sk, jsonData, 0)
	if addr == 0 || err != nil {
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "pet")

	ts.SetKeyValueEx(sk2, 500, 0, 0, []StoreAddress{1})

	jsonData := []byte(`{"pet": "cat"}`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
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

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	jsonData := []byte(`{"pet": "cat"}`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`{"pet": { "cat": "meow" }}`)
	replaced, addr, err = ts.SetKeyJson(sk, jsonData2, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("second json")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoJsonsStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	jsonData := []byte(`{"pet": "cat"}`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`{"pet": { "cat": "meow" }}`)
	replaced, addr, err = ts.SetKeyJson(sk, jsonData2, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("second json")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeSentinelValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	jsonData := []byte(`100`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(100) {
		t.Error("first verify")
	}

	jsonData2 := []byte(`null`)
	replaced, addr, err = ts.SetKeyJson(sk, jsonData2, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("second json")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != nil {
		t.Error("second verify")
	}

	jsonData3 := []byte(`{"test": 123}`)
	replaced, addr, err = ts.SetKeyJson(sk, jsonData3, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("third json")
	}

	jsonData4 := []byte(`["test"]`)
	replaced, addr, err = ts.SetKeyJson(sk, jsonData4, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("fourth json")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeSentinelValueStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	jsonData := `"text"`
	replaced, addr, err := ts.SetKeyJson(sk, []byte(jsonData), JsonStringValuesAsKeys)
	if !replaced || addr != 1 || err != nil {
		t.Error("first json")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || ve || val != nil {
		t.Error("verify")
	}

	sk2 := MakeStoreKey("text")
	_, ke = ts.LocateKey(sk2)
	if !ke {
		t.Error("sentinel subkey collision")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeSentinelArray(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	jsonData := []byte(`["test", "123"]`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	jsonData := []byte(`{"test":{"animals":[{"type":"cat"},{"type":"dog","food":"purina"}]}}`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	verifyData, err := ts.GetKeyAsJson(sk, 0)
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`["cow", "pig"]`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`["horse", "duck", "cow"]`)
	addr, err = ts.MergeKeyJson(sk, jsonData2, 0)
	if addr == 0 || err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00`)
	sk2 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x01`)
	sk3 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x02`)
	sk4 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x03`)
	sk5 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x04`)

	val1, _, _ := ts.GetKeyValue(sk1)
	val2, _, _ := ts.GetKeyValue(sk2)
	val3, _, _ := ts.GetKeyValue(sk3)
	val4, _, _ := ts.GetKeyValue(sk4)
	val5, _, _ := ts.GetKeyValue(sk5)
	if val1 != "cow" || val2 != "pig" || val3 != "horse" || val4 != "duck" || val5 != "cow" {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoArrays2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`["cow", "pig"]`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`["horse"]`)
	addr, err = ts.MergeKeyJson(sk, jsonData2, 0)
	if addr == 0 || err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00`)
	sk2 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x01`)
	sk3 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x02`)

	val1, _, _ := ts.GetKeyValue(sk1)
	val2, _, _ := ts.GetKeyValue(sk2)
	val3, _, _ := ts.GetKeyValue(sk3)
	if val1 != "cow" || val2 != "pig" || val3 != "horse" {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoArrays3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`[{"animals": {"cow": true, "pig": true}}]`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`[{"animals": {"horse": true}}]`)
	addr, err = ts.MergeKeyJson(sk, jsonData2, 0)
	if addr == 0 || err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x01/animals/horse`)
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

func TestMergeTwoArraysStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`["cow", "pig"]`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`["horse", "duck", "cow"]`)
	addr, err = ts.MergeKeyJson(sk, jsonData2, JsonStringValuesAsKeys)
	if addr == 0 || err != nil {
		t.Error("second json")
	}

	sk1 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x00/cow`)
	sk2 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x01/pig`)
	sk3 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x02/horse`)
	sk4 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x03/duck`)
	sk5 := MakeStoreKeyFromPath(`/test/farm/\x00\x00\x00\x04/cow`)

	_, ke1 := ts.LocateKey(sk1)
	_, ke2 := ts.LocateKey(sk2)
	_, ke3 := ts.LocateKey(sk3)
	_, ke4 := ts.LocateKey(sk4)
	_, ke5 := ts.LocateKey(sk5)
	if !ke1 || !ke2 || !ke3 || !ke4 || !ke5 {
		t.Error("verify vals")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestMergeTwoMaps(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "farm")

	jsonData := []byte(`{"animals": {"cow": true, "pig": true}}`)
	replaced, addr, err := ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("first json")
	}

	jsonData2 := []byte(`{"animals": {"horse": true}}`)
	addr, err = ts.MergeKeyJson(sk, jsonData2, 0)
	if addr == 0 || err != nil {
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
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	replaced, addr, err := ts.ReplaceKeyJson(sk, jsonData, 0)
	if replaced || addr != 0 || err != nil {
		t.Error("replace before exists")
	}

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, 0)
	if replaced || addr == 0 || err != nil {
		t.Error("set first")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	replaced, addr, err = ts.ReplaceKeyJson(sk, jsonData, 0)
	if !replaced || addr == 0 || err != nil {
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

func TestReplaceJsonSimpleStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	replaced, addr, err := ts.ReplaceKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr != 0 || err != nil {
		t.Error("replace before exists")
	}

	replaced, addr, err = ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set first")
	}

	jsonData = []byte(`{"pet": "dog"}`)

	replaced, addr, err = ts.ReplaceKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !replaced || addr == 0 || err != nil {
		t.Error("replace after exists")
	}

	_, ke := ts.LocateKey(MakeStoreKey("test", "pet", "cat"))
	if ke {
		t.Error("first val verify again")
	}

	_, ke = ts.LocateKey(MakeStoreKey("test", "pet", "dog"))
	if !ke {
		t.Error("second val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestCreateJsonSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	created, addr, err := ts.CreateKeyJson(sk, jsonData, 0)
	if !created || addr == 0 || err != nil {
		t.Error("create first")
	}

	created, addr, err = ts.CreateKeyJson(sk, jsonData, 0)
	if created || addr != 0 || err != nil {
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

func TestCreateJsonSimpleStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	created, addr, err := ts.CreateKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if !created || addr == 0 || err != nil {
		t.Error("create first")
	}

	created, addr, err = ts.CreateKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if created || addr != 0 || err != nil {
		t.Error("create second")
	}

	_, ke := ts.LocateKey(MakeStoreKey("test", "pet", "cat"))
	if !ke {
		t.Error("val again")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestCreateJsonExpired(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "pet")

	jsonData := []byte(`{"pet": "cat"}`)

	created, addr, err := ts.CreateKeyJson(sk, jsonData, 0)
	if !created || addr == 0 || err != nil {
		t.Error("create first")
	}

	ts.SetKeyTtl(sk, 1)

	created, addr, err = ts.CreateKeyJson(sk, jsonData, 0)
	if created || addr != 0 || err != nil {
		t.Error("create second")
	}

	ts.SetKeyTtl(sk2, 1)

	created, addr, err = ts.CreateKeyJson(sk, jsonData, 0)
	if !created || addr == 0 || err != nil {
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

func TestJsonIndexUseCase(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": {"1": {"type": "cat", "sound": "meow"}, "2": {"type": "dog", "sound": "bark"} }}`)

	replaced, addr, err := ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set")
	}

	// find the id of cat
	keys := ts.GetMatchingKeys(MakeStoreKeyFromPath("/test/pet/*/type/cat"), 0, 100)

	if len(keys) != 1 || keys[0].Key != "/test/pet/1/type/cat" {
		t.Error("find cat index")
	}

	sk2 := MakeStoreKeyFromPath(keys[0].Key)
	tc := len(sk2.Tokens)
	sk2 = MakeStoreKeyFromTokenSegments(sk2.Tokens[:tc-2]...)

	jd, err := ts.GetKeyAsJson(sk2, JsonStringValuesAsKeys)
	if err != nil {
		t.Fatal("get json fail")
	}

	var fields map[string]string
	err = json.Unmarshal(jd, &fields)
	if err != nil {
		t.Fatal("json parse fail")
	}

	if fields["sound"] != "meow" {
		t.Error("peer field verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestJsonRetrieveLeafs(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"animals": {"cat": {"sound": "meow"}, "dog": {"sound": "bark", "breeds": 360}}}`)

	replaced, addr, err := ts.SetKeyJson(sk, jsonData, JsonStringValuesAsKeys)
	if replaced || addr == 0 || err != nil {
		t.Error("set")
	}

	retrieved, err := ts.GetKeyAsJson(sk, JsonStringValuesAsKeys)
	if err != nil {
		t.Fatal("get json fail")
	}

	var m map[string]any
	err = json.Unmarshal(retrieved, &m)
	if err != nil {
		t.Fatal("json parse")
	}

	m2, exists := m["animals"].(map[string]any)
	if !exists {
		t.Error("animals")
	}

	if len(m2) != 2 {
		t.Error("anmials length")
	}

	cat, exists := m2["cat"].(map[string]any)
	if !exists {
		t.Error("cat")
	}

	dog, exists := m2["dog"].(map[string]any)
	if !exists {
		t.Error("dog")
	}

	if cat["sound"].(string) != "meow" || dog["sound"].(string) != "bark" || dog["breeds"].(float64) != 360 {
		t.Error("details")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestSetJsonStagedSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	stageSk := MakeStoreKey("staging")
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	tempSk, addr, err := ts.StageKeyJson(stageSk, jsonData, 0)
	expectedPath := fmt.Sprintf("%s/%d", stageSk.Path, addr)
	if tempSk.Path != TokenPath(expectedPath) || err != nil {
		t.Error("stage")
	}

	exists, moved := ts.MoveKey(tempSk, sk, false)
	if !exists || !moved {
		t.Error("move fail")
	}

	val, ke, ve := ts.GetKeyValue(MakeStoreKey("test", "pet"))
	if !ke || !ve || val != "cat" {
		t.Error("val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}

func TestSetJsonStagedSimpleStrAsKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	stageSk := MakeStoreKey("staging")
	sk := MakeStoreKey("test")

	jsonData := []byte(`{"pet": "cat"}`)

	tempSk, addr, err := ts.StageKeyJson(stageSk, jsonData, JsonStringValuesAsKeys)
	expectedPath := fmt.Sprintf("%s/%d", stageSk.Path, addr)
	if tempSk.Path != TokenPath(expectedPath) || err != nil {
		t.Error("stage")
	}

	exists, moved := ts.MoveKey(tempSk, sk, false)
	if !exists || !moved {
		t.Error("move fail")
	}

	ttl := ts.GetKeyTtl(MakeStoreKey("test", "pet", "cat"))
	if ttl != 0 {
		t.Error("val verify")
	}

	if !ts.DiagDump() {
		t.Fatal("dump error")
	}
}
