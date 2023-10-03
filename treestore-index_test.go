package treestore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestIndexEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.CreateIndex(dsk, isk, nil)
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("index key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexEmpty2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("index key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexUserNameSingle(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Joe"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 1 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/2" {
		t.Error("link 2 verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexUserNameMulti(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "1", "user", "Mary")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Joe"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 1 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 2 verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexUserNameDelete(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)
	ts.DeleteKey(usk1)

	if countSubKeys(ts, isk) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Joe"), 0)
	if hasLink || rv != nil {
		t.Error("link 1 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/2" {
		t.Error("link 2 verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexTwoValues(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1a := MakeStoreKey("records", "1", "user", "Joe")
	usk1b := MakeStoreKey("records", "1", "status", "active")
	usk2a := MakeStoreKey("records", "2", "user", "Mary")
	usk2b := MakeStoreKey("records", "2", "status", "suspended")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user"), MakeRecordSubPath("status")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1a)
	ts.SetKey(usk1b)
	ts.SetKey(usk2a)
	ts.SetKey(usk2b)

	if countSubKeys(ts, isk) != 4 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Joe", "active"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 1 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary", "suspended"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/2" {
		t.Error("link 2 verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexTwoValuesMove(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1a := MakeStoreKey("records", "1", "user", "Joe")
	usk1b := MakeStoreKey("records", "1", "status", "active")
	usk2a := MakeStoreKey("records", "2", "user", "Mary")
	usk2b1 := MakeStoreKey("records", "2", "status", "suspended")
	usk2b2 := MakeStoreKey("records", "2", "status", "active")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user"), MakeRecordSubPath("status")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1a)
	ts.SetKey(usk1b)
	ts.SetKey(usk2a)
	ts.SetKey(usk2b1)
	ts.MoveKey(usk2b1, usk2b2, false)

	if countSubKeys(ts, isk) != 4 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Joe", "active"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 1 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary", "suspended"), 0)
	if hasLink || rv != nil {
		t.Error("original link 2 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "Mary", "active"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/2" {
		t.Error("link 2 verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexTwoValuesTwoIndexes(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk1 := MakeStoreKey("user-index")
	isk2 := MakeStoreKey("status-index")
	usk1a := MakeStoreKey("records", "1", "user", "Joe")
	usk1b := MakeStoreKey("records", "1", "status", "active")
	usk2a := MakeStoreKey("records", "2", "user", "Mary")
	usk2b := MakeStoreKey("records", "2", "status", "active")

	re, ic := ts.CreateIndex(dsk, isk1, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created 1")
	}

	re, ic = ts.CreateIndex(dsk, isk2, []RecordSubPath{MakeRecordSubPath("status"), MakeRecordSubPath()})
	if !re || !ic {
		t.Errorf("created 2")
	}

	ts.SetKey(usk1a)
	ts.SetKey(usk1b)
	ts.SetKey(usk2a)
	ts.SetKey(usk2b)

	if countSubKeys(ts, isk1) != 2 {
		t.Error("index key count 1")
	}
	if countSubKeys(ts, isk2) != 3 {
		t.Error("index key count 2")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "Joe"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 1 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "Mary"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/2" {
		t.Error("link 2 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "active", "1"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/1" {
		t.Error("link 3 verify")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "active", "2"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/records/2" {
		t.Error("link 4 verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexRepeat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)
	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func countSubKeys(ts *TreeStore, sk StoreKey) int {
	keys := ts.GetLevelKeys(sk, "*", 0, 1000)
	count := len(keys)
	for _, key := range keys {
		count += countSubKeys(ts, AppendStoreKeySegments(sk, key.Segment))
	}
	return count
}

func TestIndexRepeat2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")
	vsk1 := MakeStoreKey("tree1", "source", "123")
	vsk2 := MakeStoreKey("tree1", "source", "123", "more")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{{}})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKey(vsk1)
	ts.SetKey(vsk2)

	if countSubKeys(ts, isk) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexRepeat3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk1 := MakeStoreKey("tree1-index")
	isk2 := MakeStoreKey("tree1-index2")
	vsk1 := MakeStoreKey("tree1", "source", "123")
	vsk2 := MakeStoreKey("tree1", "source", "123", "more")

	re, ic := ts.CreateIndex(dsk, isk1, []RecordSubPath{{}})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.CreateIndex(dsk, isk2, []RecordSubPath{{}})
	if !re || !ic {
		t.Error("not created 2")
	}

	ts.SetKey(vsk1)
	ts.SetKey(vsk2)

	if countSubKeys(ts, isk1) != 1 {
		t.Error("index key count")
	}

	if countSubKeys(ts, isk2) != 1 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func toJson(obj any) []byte {
	bytes, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return bytes
}

func TestIndexJsonArray(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data := map[string]any{
		"names": []string{"fido", "rover"},
	}

	dsk := MakeStoreKey("source")
	isk := MakeStoreKey("index-names")

	// second token is nil for the array index
	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPathFromSegments(TokenSegment("names"), nil)})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Error("index key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexJsonMixed(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data1 := map[string]any{
		"pet":   "cat",
		"sound": "meow",
		"toys": map[string]any{
			"string": []string{"zebra rope", "wand"},
		},
	}
	data2 := map[string]any{
		"pet":   "dog",
		"sound": "woof",
		"names": []string{"fido", "rover"},
	}

	dsk := MakeStoreKey("source")
	isk1 := MakeStoreKey("index-pet-types")
	isk2 := MakeStoreKey("index-names")

	re, ic := ts.CreateIndex(dsk, isk1, []RecordSubPath{MakeRecordSubPath("pet")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.CreateIndex(dsk, isk2, []RecordSubPath{MakeRecordSubPathFromSegments(TokenSegment("names"), nil)})
	if !re || !ic {
		t.Error("not created 2")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "2"), toJson(data2), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk1) != 2 {
		t.Error("index key count 1")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("index key count 2")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "cat"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "dog"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 4")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexJsonStaged(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data1 := map[string]any{
		"pet":   "cat",
		"sound": "meow",
		"toys": map[string]any{
			"string": []string{"zebra rope", "wand"},
		},
		"names": []string{"mittens"},
	}
	data2 := map[string]any{
		"pet":   "dog",
		"sound": "woof",
		"names": []string{"fido", "rover"},
	}

	stagingSk := MakeStoreKey("staging")
	dataSk := MakeStoreKey("source")
	isk1 := MakeStoreKey("index-pet-types")
	isk2 := MakeStoreKey("index-names")

	re, ic := ts.CreateIndex(dataSk, isk1, []RecordSubPath{MakeRecordSubPath("pet")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.CreateIndex(dataSk, isk2, []RecordSubPath{MakeRecordSubPathFromSegments(TokenSegment("names"), nil)})
	if !re || !ic {
		t.Error("not created 2")
	}

	tempSk1, _, _ := ts.StageKeyJson(stagingSk, toJson(data1), JsonStringValuesAsKeys)
	tempSk2, _, _ := ts.StageKeyJson(stagingSk, toJson(data2), JsonStringValuesAsKeys)

	dsk1 := AppendStoreKeySegmentStrings(dataSk, "1")
	dsk2 := AppendStoreKeySegmentStrings(dataSk, "2")

	ts.MoveReferencedKey(tempSk1, dsk1, false, 0, nil, nil)
	ts.MoveReferencedKey(tempSk2, dsk2, false, 0, nil, nil)

	if countSubKeys(ts, isk1) != 2 {
		t.Error("index key count 1")
	}

	if countSubKeys(ts, isk2) != 3 {
		t.Error("index key count 2")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "cat"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "dog"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 4")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "mittens"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 5")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexJsonStagedDeep(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data1 := map[string]any{
		"pet":   "cat",
		"sound": "meow",
		"toys": map[string]any{
			"string": []string{"zebra rope", "wand"},
		},
		"names": []string{"mittens"},
	}
	data2 := map[string]any{
		"pet":   "dog",
		"sound": "woof",
		"names": []string{"fido", "rover"},
	}

	stagingSk := MakeStoreKey("staging")
	dataSk := MakeStoreKey("v1", "data")
	isk1 := MakeStoreKey("v1", "index-pet-types")
	isk2 := MakeStoreKey("v1", "index-names")

	re, ic := ts.CreateIndex(dataSk, isk1, []RecordSubPath{MakeRecordSubPath("pet"), MakeRecordSubPath("sound")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.CreateIndex(dataSk, isk2, []RecordSubPath{MakeRecordSubPathFromSegments(TokenSegment("names"), nil)})
	if !re || !ic {
		t.Error("not created 2")
	}

	tempSk1, _, _ := ts.StageKeyJson(stagingSk, toJson(data1), JsonStringValuesAsKeys)
	tempSk2, _, _ := ts.StageKeyJson(stagingSk, toJson(data2), JsonStringValuesAsKeys)

	dsk1 := AppendStoreKeySegmentStrings(dataSk, "1")
	dsk2 := AppendStoreKeySegmentStrings(dataSk, "2")

	ts.MoveReferencedKey(tempSk1, dsk1, false, 0, nil, nil)
	ts.MoveReferencedKey(tempSk2, dsk2, false, 0, nil, nil)

	if countSubKeys(ts, isk1) != 4 {
		t.Error("index key count 1")
	}

	if countSubKeys(ts, isk2) != 3 {
		t.Error("index key count 2")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "cat", "meow"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/v1/data/1" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "dog", "woof"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/v1/data/2" {
		t.Error("link verify 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/v1/data/2" {
		t.Error("link verify 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/v1/data/2" {
		t.Error("link verify 4")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "mittens"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/v1/data/1" {
		t.Error("link verify 5")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}


func TestIndexLate(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data1 := map[string]any{
		"pet":   "cat",
		"sound": "meow",
		"toys": map[string]any{
			"string": []string{"zebra rope", "wand"},
		},
	}
	data2 := map[string]any{
		"pet":   "dog",
		"sound": "woof",
		"names": []string{"fido", "rover"},
	}

	dsk := MakeStoreKey("source")
	isk1 := MakeStoreKey("index-pet-types")
	isk2 := MakeStoreKey("index-names")

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "2"), toJson(data2), JsonStringValuesAsKeys)

	re, ic := ts.CreateIndex(dsk, isk1, []RecordSubPath{MakeRecordSubPath("pet")})
	if !re || !ic {
		t.Error("not created")
	}

	re, ic = ts.CreateIndex(dsk, isk2, []RecordSubPath{MakeRecordSubPathFromSegments(TokenSegment("names"), nil)})
	if !re || !ic {
		t.Error("not created 2")
	}

	if countSubKeys(ts, isk1) != 2 {
		t.Error("index key count 1")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("index key count 2")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "cat"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "dog"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 4")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexAddDeleteAdd(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data1 := map[string]any{
		"pet":   "cat",
		"sound": "meow",
		"toys": map[string]any{
			"string": []string{"zebra rope", "wand"},
		},
	}
	data2 := map[string]any{
		"pet":   "dog",
		"sound": "woof",
		"names": []string{"fido", "rover"},
	}

	dsk := MakeStoreKey("source")
	isk1 := MakeStoreKey("index-pet-types")
	isk2 := MakeStoreKey("index-names")

	re, ic := ts.CreateIndex(dsk, isk1, []RecordSubPath{MakeRecordSubPath("pet")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.CreateIndex(dsk, isk2, []RecordSubPath{MakeRecordSubPathFromSegments(TokenSegment("names"), nil)})
	if !re || !ic {
		t.Error("not created 2")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "2"), toJson(data2), JsonStringValuesAsKeys)

	re, ir := ts.DeleteIndex(dsk, isk1)
	if !re || !ir {
		t.Error("not deleted")
	}

	re, ir = ts.DeleteIndex(dsk, isk1)
	if !re || ir {
		t.Error("not deleted 2")
	}

	if countSubKeys(ts, isk1) != 0 {
		t.Error("index key count 1")
	}

	re, ic = ts.CreateIndex(dsk, isk1, []RecordSubPath{MakeRecordSubPath("pet")})
	if !re || !ic {
		t.Error("not created again")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("index key count 2")
	}

	if countSubKeys(ts, isk1) != 2 {
		t.Error("index key count 1 again")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("index key count 2 again")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "cat"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/1" {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk1, "dog"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk2, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/source/2" {
		t.Error("link verify 4")
	}

	re, ic = ts.CreateIndex(dsk, isk1, []RecordSubPath{MakeRecordSubPath("pet")})
	if re || ic {
		t.Error("create overwite not blocked")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexRedefineIndex(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-index")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	re, ic = ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("test")})
	if !re || ic {
		t.Errorf("created")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexIndexMissingValues(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("function")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 0 {
		t.Error("index key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexIndexMissingValues2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "function")
	usk2 := MakeStoreKey("records", "2", "user", "service")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user", "function")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 0 {
		t.Error("index key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexTakeRecordAway(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("index key count")
	}

	ts.DeleteKeyTree(dsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("index key count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestIndexTakeRecordAway2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-index")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.CreateIndex(dsk, isk, []RecordSubPath{MakeRecordSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("index key count")
	}

	ts.DeleteKey(usk1)
	ts.DeleteKey(usk2)
	ts.DeleteKey(dsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("index key count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}
