package treestore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestAutoLinkEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, nil)
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkEmpty2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkSimple(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkUserNameSingle(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
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

func TestAutoLinkUserNameMulti(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "1", "user", "Mary")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
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

func TestAutoLinkUserNameDelete(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)
	ts.DeleteKey(usk1)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
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

func TestAutoLinkTwoValues(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1a := MakeStoreKey("records", "1", "user", "Joe")
	usk1b := MakeStoreKey("records", "1", "status", "active")
	usk2a := MakeStoreKey("records", "2", "user", "Mary")
	usk2b := MakeStoreKey("records", "2", "status", "suspended")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user"), MakeSubPath("status")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1a)
	ts.SetKey(usk1b)
	ts.SetKey(usk2a)
	ts.SetKey(usk2b)

	if countSubKeys(ts, isk) != 4 {
		t.Error("auto-link key count")
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

func TestAutoLinkTwoValuesMove(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1a := MakeStoreKey("records", "1", "user", "Joe")
	usk1b := MakeStoreKey("records", "1", "status", "active")
	usk2a := MakeStoreKey("records", "2", "user", "Mary")
	usk2b1 := MakeStoreKey("records", "2", "status", "suspended")
	usk2b2 := MakeStoreKey("records", "2", "status", "active")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user"), MakeSubPath("status")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1a)
	ts.SetKey(usk1b)
	ts.SetKey(usk2a)
	ts.SetKey(usk2b1)
	ts.MoveKey(usk2b1, usk2b2, false)

	if countSubKeys(ts, isk) != 4 {
		t.Error("auto-link key count")
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

func TestAutoLinkTwoValuesTwoAlKeys(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk1 := MakeStoreKey("user-links")
	isk2 := MakeStoreKey("status-links")
	usk1a := MakeStoreKey("records", "1", "user", "Joe")
	usk1b := MakeStoreKey("records", "1", "status", "active")
	usk2a := MakeStoreKey("records", "2", "user", "Mary")
	usk2b := MakeStoreKey("records", "2", "status", "active")

	re, ic := ts.DefineAutoLinkKey(dsk, isk1, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created 1")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk2, []SubPath{MakeSubPath("status"), MakeSubPath()})
	if !re || !ic {
		t.Errorf("created 2")
	}

	ts.SetKey(usk1a)
	ts.SetKey(usk1b)
	ts.SetKey(usk2a)
	ts.SetKey(usk2b)

	if countSubKeys(ts, isk1) != 2 {
		t.Error("auto-link key count 1")
	}
	if countSubKeys(ts, isk2) != 3 {
		t.Error("auto-link key count 2")
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

func TestAutoLinkRepeat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)
	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
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

func TestAutoLinkRepeat2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk1 := MakeStoreKey("tree1", "source", "123")
	vsk2 := MakeStoreKey("tree1", "source", "123", "more")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKey(vsk1)
	ts.SetKey(vsk2)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkRepeat3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk1 := MakeStoreKey("tree1-links")
	isk2 := MakeStoreKey("tree1-links2")
	vsk1 := MakeStoreKey("tree1", "source", "123")
	vsk2 := MakeStoreKey("tree1", "source", "123", "more")

	re, ic := ts.DefineAutoLinkKey(dsk, isk1, []SubPath{{}})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk2, []SubPath{{}})
	if !re || !ic {
		t.Error("not created 2")
	}

	ts.SetKey(vsk1)
	ts.SetKey(vsk2)

	if countSubKeys(ts, isk1) != 1 {
		t.Error("auto-link key count")
	}

	if countSubKeys(ts, isk2) != 1 {
		t.Error("auto-link key count")
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

func TestAutoLinkJsonArray(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	data := map[string]any{
		"names": []string{"fido", "rover"},
	}

	dsk := MakeStoreKey("source")
	isk := MakeStoreKey("link-names")

	// second token is nil for the array index
	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("names", `\N`)})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
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

func TestAutoLinkJsonMixed(t *testing.T) {
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
	isk1 := MakeStoreKey("link-pet-types")
	isk2 := MakeStoreKey("link-names")

	re, ic := ts.DefineAutoLinkKey(dsk, isk1, []SubPath{MakeSubPath("pet")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk2, []SubPath{MakeSubPath("names", `\N`)})
	if !re || !ic {
		t.Error("not created 2")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "2"), toJson(data2), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk1) != 2 {
		t.Error("auto-link key count 1")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("auto-link key count 2")
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

func TestAutoLinkJsonStaged(t *testing.T) {
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
	isk1 := MakeStoreKey("link-pet-types")
	isk2 := MakeStoreKey("link-names")

	re, ic := ts.DefineAutoLinkKey(dataSk, isk1, []SubPath{MakeSubPath("pet")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dataSk, isk2, []SubPath{MakeSubPath("names", `\N`)})
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
		t.Error("auto-link key count 1")
	}

	if countSubKeys(ts, isk2) != 3 {
		t.Error("auto-link key count 2")
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

func TestAutoLinkJsonStagedDeep(t *testing.T) {
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
	isk1 := MakeStoreKey("v1", "link-pet-types")
	isk2 := MakeStoreKey("v1", "link-names")

	re, ic := ts.DefineAutoLinkKey(dataSk, isk1, []SubPath{MakeSubPath("pet"), MakeSubPath("sound")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dataSk, isk2, []SubPath{MakeSubPath("names", `\N`)})
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
		t.Error("auto-link key count 1")
	}

	if countSubKeys(ts, isk2) != 3 {
		t.Error("auto-link key count 2")
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

func TestAutoLinkLate(t *testing.T) {
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
	isk1 := MakeStoreKey("link-pet-types")
	isk2 := MakeStoreKey("link-names")

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "2"), toJson(data2), JsonStringValuesAsKeys)

	re, ic := ts.DefineAutoLinkKey(dsk, isk1, []SubPath{MakeSubPath("pet")})
	if !re || !ic {
		t.Error("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk2, []SubPath{MakeSubPath("names", `\N`)})
	if !re || !ic {
		t.Error("not created 2")
	}

	if countSubKeys(ts, isk1) != 2 {
		t.Error("auto-link key count 1")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("auto-link key count 2")
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

func TestAutoLinkAddDeleteAdd(t *testing.T) {
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
	isk1 := MakeStoreKey("link-pet-types")
	isk2 := MakeStoreKey("link-names")

	re, ic := ts.DefineAutoLinkKey(dsk, isk1, []SubPath{MakeSubPath("pet")})
	if re || !ic {
		t.Error("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk2, []SubPath{MakeSubPath("names", `\N`)})
	if !re || !ic {
		t.Error("not created 2")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "1"), toJson(data1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, "2"), toJson(data2), JsonStringValuesAsKeys)

	re, ir := ts.RemoveAutoLinkKey(dsk, isk1)
	if !re || !ir {
		t.Error("not deleted")
	}

	re, ir = ts.RemoveAutoLinkKey(dsk, isk1)
	if !re || ir {
		t.Error("not deleted 2")
	}

	if countSubKeys(ts, isk1) != 0 {
		t.Error("auto-link key count 1")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk1, []SubPath{MakeSubPath("pet")})
	if !re || !ic {
		t.Error("not created again")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("auto-link key count 2")
	}

	if countSubKeys(ts, isk1) != 2 {
		t.Error("auto-link key count 1 again")
	}

	if countSubKeys(ts, isk2) != 2 {
		t.Error("auto-link key count 2 again")
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

	re, ic = ts.DefineAutoLinkKey(dsk, isk1, []SubPath{MakeSubPath("pet")})
	if re || ic {
		t.Error("create overwite not blocked")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkRedefineAutoLink(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("test")})
	if !re || ic {
		t.Errorf("created")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkDefMissingValues(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("function")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkDefMissingValues2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "function")
	usk2 := MakeStoreKey("records", "2", "user", "service")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user", "function")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkTakeRecordAway(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
	}

	ts.DeleteKeyTree(dsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkTakeRecordAway2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("records")
	isk := MakeStoreKey("user-links")
	usk1 := MakeStoreKey("records", "1", "user", "Joe")
	usk2 := MakeStoreKey("records", "2", "user", "Mary")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("user")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(usk1)
	ts.SetKey(usk2)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
	}

	ts.DeleteKey(usk1)
	ts.DeleteKey(usk2)
	ts.DeleteKey(dsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkGet(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	isk2 := MakeStoreKey("tree1-links2")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}

	id := ts.GetAutoLinkDefinition(MakeStoreKey())
	if id != nil {
		t.Error("expected no auto-link def")
	}

	id = ts.GetAutoLinkDefinition(dsk)
	if len(id) != 1 {
		t.Error("expected auto-link def")
	}
	if id[0].AutoLinkSk.Path != isk.Path || len(id[0].Fields) != 1 {
		t.Error("bad auto-link response 1")
	}

	re, ic = ts.DefineAutoLinkKey(dsk, isk2, []SubPath{MakeSubPath("test"), MakeSubPath("link", "deeper")})
	if !re || !ic {
		t.Errorf("not created 2")
	}

	id = ts.GetAutoLinkDefinition(dsk)
	if len(id) != 2 {
		t.Error("expected auto-link def")
	}
	if id[0].AutoLinkSk.Path != isk.Path || len(id[0].Fields) != 1 {
		t.Errorf("bad auto-link response 2a: %s %d", id[0].AutoLinkSk.Path, len(id[0].Fields))
	}
	if id[1].AutoLinkSk.Path != isk2.Path || len(id[1].Fields) != 2 {
		t.Errorf("bad auto-link response 2b: %s %d", id[1].AutoLinkSk.Path, len(id[1].Fields))
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonUpdateArray(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store names under /names, and link them under /link-names
	//
	// - add one name
	// - verify
	// - add a second name
	// - verify
	// - remove the first name
	// - verify
	// - remove the second name
	// - verify
	//

	data := map[string]any{
		"names": []string{"fido"},
	}

	dsk := MakeStoreKey("source")
	isk := MakeStoreKey("link-names")
	id := "ID1"

	// second token is nil for the array index
	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("names", `\N`)})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count 1")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1")
	}

	// add the second name
	data["names"] = []string{"fido", "rover"}
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1 again")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 2")
	}

	// remove the first name
	data["names"] = []string{"rover"}
	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if hasLink || rv != nil {
		t.Error("link verify deletion")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 2 again")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDelTree(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store names under /names, and link them under /link-names
	//
	// - store some data
	// - verify
	// - deltree the data
	// - verify
	//

	data := map[string]any{
		"names": []string{"fido", "rover"},
	}

	dsk := MakeStoreKey("source")
	isk := MakeStoreKey("link-names")
	id := "ID1"

	// second token is nil for the array index
	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("names", `\N`)})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(dsk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 2")
	}

	// deltree the data
	ts.DeleteKeyTree(AppendStoreKeySegmentStrings(dsk, id))

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if hasLink || rv != nil {
		t.Error("link verify 1 again")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "rover"), 0)
	if hasLink || rv != nil {
		t.Error("link verify 2 again")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store names under /source/ID/records/names, and link them under /link-names,
	// place auto-link on /source
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify

	data := map[string]any{
		"records": map[string]any{
			"names": []string{"fido", "rover"},
		},
	}

	ssk := MakeStoreKey("source")
	isk := MakeStoreKey("link-names")
	id := "ID1"
	dsk := MakeStoreKey("source", id, "records", "names")

	// second token is nil for the array index
	re, ic := ts.DefineAutoLinkKey(ssk, isk, []SubPath{MakeSubPath("records", "names", `\N`)})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(ssk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 2")
	}

	// deltree the names
	ts.DeleteKeyTree(dsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count")
	}

	// store new data
	data = map[string]any{
		"names": []string{"mittens"},
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(ssk, id, "records"), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "mittens"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1 again")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace2(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store structure under /source/ID/records/data, and link them under /link-type-name,
	// place auto-link on /source and pick two fields out of the data
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	data := map[string]any{
		"records": map[string]any{
			"data": []any{
				map[string]any{
					"type": "dog",
					"name": "fido",
				},
				map[string]any{
					"type": "cat",
					"name": "muffy",
				},
			},
		},
	}

	ssk := MakeStoreKey("source")
	isk := MakeStoreKey("link-type-name")
	id := "ID1"
	dsk := MakeStoreKey("source", id, "records", "data")

	// second token is nil for the array index
	re, ic := ts.DefineAutoLinkKey(ssk, isk, []SubPath{MakeSubPath("records", "data", `\N`, "type"), MakeSubPath("records", "data", `\N`, "name")})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(ssk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 6 {
		t.Error("auto-link key count 1")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "dog", "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "cat", "muffy"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 2")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "dog", "muffy"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 3")
	}
	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "cat", "fido"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 4")
	}

	// deltree the names
	ts.DeleteKeyTree(dsk)

	if countSubKeys(ts, isk) != 0 {
		t.Error("auto-link key count 2")
	}

	// store new data
	data = map[string]any{
		"data": []any{
			map[string]any{
				"type": "dog",
				"name": "rover",
			},
		},
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(ssk, id, "records"), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Error("auto-link key count 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "dog", "rover"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1 again")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace3(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store structure under /source/ID/records/data, and link them under /link-service-id,
	// place auto-link on /source and pick two fields out of the data that don't get deleted.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	data := map[string]any{
		"records": map[string]any{
			"pets": []any{
				map[string]any{
					"type": "dog",
					"name": "fido",
				},
				map[string]any{
					"type": "cat",
					"name": "muffy",
				},
			},
			"info": map[string]any{
				"service": "vet",
				"id":      "35",
			},
		},
	}

	ssk := MakeStoreKey("source")
	isk := MakeStoreKey("link-service-id")
	id := "ID1"
	dsk := MakeStoreKey("source", id, "records", "data")

	// second token is nil for the array index
	re, ic := ts.DefineAutoLinkKey(ssk, isk, []SubPath{MakeSubPath("records", "info", "service"), MakeSubPath("records", "info", "id")})
	if re || !ic {
		t.Error("not created")
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(ssk, id), toJson(data), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Fatal("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "vet", "35"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1")
	}

	// deltree the names
	ts.DeleteKeyTree(dsk)

	if countSubKeys(ts, isk) != 2 {
		t.Fatal("auto-link key count 2")
	}

	// store new data
	pets := []any{
		map[string]any{
			"type": "dog",
			"name": "rover",
		},
	}

	ts.SetKeyJson(AppendStoreKeySegmentStrings(ssk, id, "records", "pets"), toJson(pets), JsonStringValuesAsKeys)

	if countSubKeys(ts, isk) != 2 {
		t.Fatal("auto-link key count 3")
	}

	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "vet", "35"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/source/"+id) {
		t.Error("link verify 1 again")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace4(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/email-org-name,
	// place auto-link on /users/profiles and pick two fields out of the data that don't get deleted.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
	"email": "dog@gmail.com",
	"name": "Testy",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	usersSk := MakeStoreKey("users", "profiles")
	ts.DefineAutoLinkKey(usersSk, MakeStoreKey("users", "email-org-name"), []SubPath{MakeSubPath("email"), MakeSubPath("organization_id"), MakeSubPath("name")})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, MakeStoreKey("users", "email-org-name"))
	if count != 6 {
		t.Error("link count 1")
	}

	dsk := AppendStoreKeySegmentStrings(usersSk, userId1, "permissions")
	ts.DeleteKeyTree(dsk)

	count = countSubKeys(ts, MakeStoreKey("users", "email-org-name"))
	if count != 6 {
		t.Error("link count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace5(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/orgs,
	// place auto-link on /users/profiles and pick two fields out of the data that don't get deleted.
	// One of the fields must be the record ID.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
	"email": "dog@gmail.com",
	"name": "Testy",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	usersSk := MakeStoreKey("users", "profiles")
	ts.DefineAutoLinkKey(usersSk, MakeStoreKey("users", "orgs"), []SubPath{MakeSubPath("organization_id"), nil})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, MakeStoreKey("users", "orgs"))
	if count != 3 {
		t.Error("link count 1")
	}

	dsk := AppendStoreKeySegmentStrings(usersSk, userId1, "permissions")
	ts.DeleteKeyTree(dsk)

	count = countSubKeys(ts, MakeStoreKey("users", "orgs"))
	if count != 3 {
		t.Error("link count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace6(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/roles,
	// place auto-link on /users/profiles and auto-link the role_id field within the array.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
	"email": "dog@gmail.com",
	"name": "Testy",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	usersSk := MakeStoreKey("users", "profiles")
	kRoleLinksSk := MakeStoreKey("users", "roles")
	ts.DefineAutoLinkKey(usersSk, kRoleLinksSk, []SubPath{MakeSubPath("permissions", `\N`, "role_id"), nil})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, kRoleLinksSk)
	if count != 3 {
		t.Error("link count 1")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace7(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/roles,
	// place auto-link on /users/profiles and auto-link the role_id field within the array.
	// Set a permission using MergeKeyJson and partial data.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
	"email": "dog@gmail.com",
	"name": "Testy",
	"organization_id": "ORG2",
	"permissions": []
}`

	usersSk := MakeStoreKey("users", "profiles")
	u2Sk := MakeStoreKey("users", "profiles", "USER2")
	kRoleLinksSk := MakeStoreKey("users", "roles")
	ts.DefineAutoLinkKey(usersSk, kRoleLinksSk, []SubPath{MakeSubPath("permissions", `\N`, "role_id"), nil})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, kRoleLinksSk)
	if count != 2 {
		t.Error("link count 1")
	}

	partialJson := `{
	"login": "today",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`
	ts.MergeKeyJson(u2Sk, []byte(partialJson), JsonStringValuesAsKeys)

	count = countSubKeys(ts, kRoleLinksSk)
	if count != 3 {
		t.Error("link count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace8(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/roles,
	// place auto-link on /users/profiles and auto-link the role_id field within the array.
	// Set a permission using SetKeyJson and partial data.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
	"email": "dog@gmail.com",
	"name": "Testy",
	"organization_id": "ORG2",
	"permissions": []
}`

	usersSk := MakeStoreKey("users", "profiles")
	u2Sk := MakeStoreKey("users", "profiles", "USER2")
	kRoleLinksSk := MakeStoreKey("users", "roles")
	ts.DefineAutoLinkKey(usersSk, kRoleLinksSk, []SubPath{MakeSubPath("permissions", `\N`, "role_id"), nil})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, kRoleLinksSk)
	if count != 2 {
		t.Error("link count 1")
	}

	partialJson := `{
	"login": "today",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`
	ts.SetKeyJson(u2Sk, []byte(partialJson), JsonStringValuesAsKeys)

	count = countSubKeys(ts, kRoleLinksSk)
	if count != 3 {
		t.Error("link count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace9(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/roles,
	// place auto-link on /users/profiles and auto-link the role_id field within the array.
	// Set a permission using CreateKeyJson and partial data.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
"email": "dog@gmail.com",
"name": "Testy",
"organization_id": "ORG2"
}`

	usersSk := MakeStoreKey("users", "profiles")
	u2Sk := MakeStoreKey("users", "profiles", "USER2", "permissions")
	kRoleLinksSk := MakeStoreKey("users", "roles")
	ts.DefineAutoLinkKey(usersSk, kRoleLinksSk, []SubPath{MakeSubPath("permissions", `\N`, "role_id"), nil})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, kRoleLinksSk)
	if count != 2 {
		t.Error("link count 1")
	}

	partialJson := `[
	{
		"context": "CONTEXT1",
		"role_id": "ROLE1"
	}
]`
	ts.CreateKeyJson(u2Sk, []byte(partialJson), JsonStringValuesAsKeys)

	count = countSubKeys(ts, kRoleLinksSk)
	if count != 3 {
		t.Error("link count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkJsonDeepReplace10(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	//
	// Store a user structure under /users/profiles/ID, and link them under /users/roles,
	// place auto-link on /users/profiles and auto-link the role_id field within the array.
	// Set a permission using ReplaceKeyJson and partial data.
	//
	// - store full record
	// - verify
	// - deltree the names
	// - verify
	// - store replacement names
	// - verify
	//

	userId1 := "USER1"
	userProfile1 := `{
	"email": "cat@gmail.com",
	"name": "Test User",
	"organization_id": "ORG2",
	"permissions": [
		{
			"context": "CONTEXT1",
			"role_id": "ROLE1"
		}
	]
}`

	userId2 := "USER2"
	userProfile2 := `{
"email": "dog@gmail.com",
"name": "Testy",
"organization_id": "ORG2",
"permissions": []
}`

	usersSk := MakeStoreKey("users", "profiles")
	u2Sk := MakeStoreKey("users", "profiles", "USER2", "permissions")
	kRoleLinksSk := MakeStoreKey("users", "roles")
	ts.DefineAutoLinkKey(usersSk, kRoleLinksSk, []SubPath{MakeSubPath("permissions", `\N`, "role_id"), nil})
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId1), []byte(userProfile1), JsonStringValuesAsKeys)
	ts.SetKeyJson(AppendStoreKeySegmentStrings(usersSk, userId2), []byte(userProfile2), JsonStringValuesAsKeys)

	count := countSubKeys(ts, kRoleLinksSk)
	if count != 2 {
		t.Error("link count 1")
	}

	partialJson := `[
	{
		"context": "CONTEXT1",
		"role_id": "ROLE1"
	}
]`
	ts.ReplaceKeyJson(u2Sk, []byte(partialJson), JsonStringValuesAsKeys)

	count = countSubKeys(ts, kRoleLinksSk)
	if count != 3 {
		t.Error("link count 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkExpireFieldData(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk := MakeStoreKey("tree1", "source", "123")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
	}

	ts.SetKeyTtl(vsk, 1)

	// DESIGN ISSUE - really would be nice if the key expiration resulted in immediate auto-link adjustment.
	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count 2")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "123"), 0)
	if !hasLink || rv != nil {
		t.Error("link verify")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkExpireFieldDataDeep(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk1 := MakeStoreKey("tree1", "source", "123")
	vsk2 := MakeStoreKey("tree1", "source", "123", "data", "abc")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{MakeSubPath("data")})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk1)
	ts.SetKeyValueEx(vsk2, nil, SetExNoValueUpdate, time.Now().Add(time.Millisecond*10).UnixNano(), nil)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "abc"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/tree1/source/123") {
		t.Error("link verify")
	}

	time.Sleep(20 * time.Millisecond)

	// DESIGN ISSUE - really would be nice if the key expiration resulted in immediate auto-link adjustment.
	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count 2")
	}

	// DESIGN BUG - even though the inner data member has expired, we still find the record.
	// This will have to be fixed another day.
	hasLink, rv = ts.GetRelationshipValue(AppendStoreKeySegmentStrings(isk, "abc"), 0)
	if !hasLink || rv == nil || rv.Sk.Path != TokenPath("/tree1/source/123") {
		t.Error("link verify 2")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}

func TestAutoLinkAddRemove(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	dsk := MakeStoreKey("tree1", "source")
	isk := MakeStoreKey("tree1-links")
	vsk := MakeStoreKey("tree1", "source", "123")
	linkSk := AppendStoreKeySegmentStrings(isk, "123")

	re, ic := ts.DefineAutoLinkKey(dsk, isk, []SubPath{{}})
	if re || !ic {
		t.Errorf("not created")
	}

	ts.SetKey(vsk)

	if countSubKeys(ts, isk) != 1 {
		t.Error("auto-link key count")
	}

	hasLink, rv := ts.GetRelationshipValue(linkSk, 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/tree1/source/123" {
		t.Error("link verify")
	}

	ts.DeleteKey(vsk)

	ttl := ts.GetKeyTtl(linkSk)
	if ttl != -1 {
		t.Error("id should not be indexed")
	}

	ttl = ts.GetKeyTtl(isk)
	if ttl != 0 {
		t.Error("isk should still exist")
	}

	if !ts.DiagDump() {
		t.Error("final dump")
	}
}
