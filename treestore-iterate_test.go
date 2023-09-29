package treestore

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestIterateLevelEmpty(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	keys := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty root")
	}

	ts.SetKey(MakeStoreKey())

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty root with sentinel")
	}

	ts.SetKeyValue(MakeStoreKey(), 1)

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty root with sentinel value")
	}

	keys = ts.GetLevelKeys(MakeStoreKey("not there"), "*", 0, 100)
	if keys != nil {
		t.Error("empty root no key")
	}
}

func TestIterateLevelRoot(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("a")

	ts.SetKey(sk)

	keys := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 1 || string(keys[0].Segment) != "a" {
		t.Error("one node")
	}

	sk2 := MakeStoreKey("b", "c")

	ts.SetKey(sk2)

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 2 || string(keys[0].Segment) != "a" || string(keys[1].Segment) != "b" {
		t.Error("two nodes")
	}

	sk3 := MakeStoreKey("d")

	ts.SetKey(sk3)

	keys = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 3 || string(keys[0].Segment) != "a" || string(keys[1].Segment) != "b" || string(keys[2].Segment) != "d" {
		t.Error("three nodes")
	}
}

func TestIterateLevelNoBase(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("a", "b", "c")

	ts.SetKey(sk)

	keys := ts.GetLevelKeys(MakeStoreKey("d"), "*", 0, 100)
	if keys != nil {
		t.Error("no base match")
	}
}

func TestIterateSecondLevel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("a")

	keys := ts.GetLevelKeys(sk, "*", 0, 100)
	if keys != nil {
		t.Error("no second level")
	}

	ts.SetKey(sk)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty second level")
	}

	sk2 := MakeStoreKey("a", "cat")

	ts.SetKey(sk2)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].Segment) != "cat" {
		t.Error("one node")
	}

	if keys[0].HasValue || keys[0].HasChildren {
		t.Error("key only node")
	}

	sk3 := MakeStoreKey("a", "cat", "dog")

	ts.SetKey(sk3)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].Segment) != "cat" {
		t.Error("one node")
	}

	if keys[0].HasValue || !keys[0].HasChildren {
		t.Error("key only node")
	}

	sk4 := MakeStoreKey("a", "dog")

	ts.SetKeyValue(sk4, 80)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].Segment) != "cat" || string(keys[1].Segment) != "dog" {
		t.Error("two nodes")
	}

	if keys[0].HasValue || !keys[0].HasChildren {
		t.Error("cat node flags")
	}

	if !keys[1].HasValue || keys[1].HasChildren {
		t.Error("dog node flags")
	}

	sk5 := MakeStoreKey("a", "dog", "fido")

	ts.SetKeyValue(sk5, 80)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].Segment) != "cat" || string(keys[1].Segment) != "dog" {
		t.Error("two nodes")
	}

	if keys[0].HasValue || !keys[0].HasChildren {
		t.Error("cat node flags 2")
	}

	if !keys[1].HasValue || !keys[1].HasChildren {
		t.Error("dog node flags 2")
	}
}

func TestIterateThirdLevel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("data", "test")

	keys := ts.GetLevelKeys(sk, "*", 0, 100)
	if keys != nil {
		t.Error("no second level")
	}

	ts.SetKey(sk)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty second level")
	}

	sk2 := MakeStoreKey("data", "test", "cat")

	ts.SetKey(sk2)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].Segment) != "cat" {
		t.Error("one node")
	}

	if keys[0].HasValue || keys[0].HasChildren {
		t.Error("key only node")
	}

	sk3 := MakeStoreKey("data", "test", "cat", "dog")

	ts.SetKey(sk3)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].Segment) != "cat" {
		t.Error("one node")
	}

	if keys[0].HasValue || !keys[0].HasChildren {
		t.Error("key only node")
	}

	sk4 := MakeStoreKey("data", "test", "dog")

	ts.SetKeyValue(sk4, 80)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].Segment) != "cat" || string(keys[1].Segment) != "dog" {
		t.Error("two nodes")
	}

	if keys[0].HasValue || !keys[0].HasChildren {
		t.Error("cat node flags")
	}

	if !keys[1].HasValue || keys[1].HasChildren {
		t.Error("dog node flags")
	}

	sk5 := MakeStoreKey("data", "test", "dog", "fido")

	ts.SetKeyValue(sk5, 80)

	keys = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].Segment) != "cat" || string(keys[1].Segment) != "dog" {
		t.Error("two nodes")
	}

	if keys[0].HasValue || !keys[0].HasChildren {
		t.Error("cat node flags 2")
	}

	if !keys[1].HasValue || !keys[1].HasChildren {
		t.Error("dog node flags 2")
	}
}

func TestIterateLevelPages(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	taken := map[int]struct{}{}
	values := make([]string, 0, 250)
	for i := 0; i < 250; i++ {
		for {
			n := rand.Intn(10000)
			_, exists := taken[n]
			if !exists {
				taken[n] = struct{}{}
				text := fmt.Sprintf("%d", n)
				sk := MakeStoreKey(text)
				values = append(values, text)
				ts.SetKey(sk)
				break
			}
		}
	}

	sort.Strings(values)

	root := MakeStoreKey()
	keys := ts.GetLevelKeys(root, "*", 0, 0)
	if keys == nil || len(keys) != 0 {
		t.Error("empty page")
	}

	for i := 0; i < 250; i++ {
		keys = ts.GetLevelKeys(root, "*", i, 0)
		if keys == nil || len(keys) != 0 {
			t.Error("empty page with non zero start")
		}

		keys = ts.GetLevelKeys(root, "*", i, 1)
		if keys == nil || len(keys) != 1 {
			t.Error("one item page")
		}

		if string(keys[0].Segment) != values[i] {
			t.Error("value mismatch")
		}

		remaining := 250 - i
		remaining2 := 2
		if remaining < remaining2 {
			remaining2 = remaining
		}
		keys = ts.GetLevelKeys(root, "*", i, 2)
		if keys == nil || len(keys) != remaining2 {
			t.Error("two item page")
		}

		if string(keys[0].Segment) != values[i] {
			t.Error("first value mismatch")
		}
		if len(keys) == 2 {
			if string(keys[1].Segment) != values[i+1] {
				t.Error("second value mismatch")
			}
		}

		keys = ts.GetLevelKeys(root, "*", i, remaining)
		if keys == nil || len(keys) != remaining {
			t.Error("remaining items page")
		}

		for j := 0; j < remaining; j++ {
			if string(keys[j].Segment) != values[i+j] {
				t.Errorf("page value %d", i+j)
			}
		}

		keys = ts.GetLevelKeys(root, "*", i, 1000)
		if keys == nil || len(keys) != remaining {
			t.Error("large page with start offset")
		}

		for j := 0; j < remaining; j++ {
			if string(keys[j].Segment) != values[i+j] {
				t.Errorf("page value %d", i+j)
			}
		}
	}
}

func TestIterateLevelPattern(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	taken := map[int]struct{}{}
	values := make([]string, 0, 250)
	for i := 0; i < 250; i++ {
		for {
			n := rand.Intn(10000)
			_, exists := taken[n]
			if !exists {
				taken[n] = struct{}{}
				text := fmt.Sprintf("%d", n)
				sk := MakeStoreKey(text)
				values = append(values, text)
				ts.SetKey(sk)
				break
			}
		}
	}

	sort.Strings(values)

	root := MakeStoreKey()
	keys := ts.GetLevelKeys(root, "", 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty page")
	}

	keys = ts.GetLevelKeys(root, values[0], 0, 100)
	if len(keys) != 1 {
		t.Error("exact match")
	}

	ones := 0
	for _, val := range values {
		if strings.HasPrefix(val, "1") {
			ones++
		}
	}

	keys = ts.GetLevelKeys(root, "1*", 0, 250)
	if len(keys) != ones {
		t.Error("match ones")
	}
}

func TestFullIterateSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	keys := ts.GetMatchingKeys(MakeStoreKey(), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("sentinel match")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("default sentinel")
	}

	ts.SetKeyValue(MakeStoreKey(), 320)

	keys = ts.GetMatchingKeys(MakeStoreKey(), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("sentinel match")
	}

	if keys[0].CurrentValue != 320 || keys[0].HasChildren || !keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("sentinel has value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateOneKey(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	keys := ts.GetMatchingKeys(sk, 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty match")
	}

	ts.SetKey(sk)

	keys = ts.GetMatchingKeys(sk, 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test key")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match *")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test key *")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("t*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match t*")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test key t*")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match **")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test key **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match **/**")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test key **/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**"), 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Fatal("test key match test/**")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateOneValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test")

	keys := ts.GetMatchingKeys(sk, 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty match")
	}

	ts.SetKeyValue(sk, 330)

	keys = ts.GetMatchingKeys(sk, 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match")
	}

	if keys[0].CurrentValue != 330 || keys[0].HasChildren || !keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test value")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match *")
	}

	if keys[0].CurrentValue != 330 || keys[0].HasChildren || !keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test value *")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("t*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match t*")
	}

	if keys[0].CurrentValue != 330 || keys[0].HasChildren || !keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test value t*")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match **")
	}

	if keys[0].CurrentValue != 330 || keys[0].HasChildren || !keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test value **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match **/**")
	}

	if keys[0].CurrentValue != 330 || keys[0].HasChildren || !keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test value **/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**"), 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Fatal("test value match test/**")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateTwoLevel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test", "cat")

	keys := ts.GetMatchingKeys(sk, 0, 100)
	if keys == nil || len(keys) != 0 {
		t.Error("empty match")
	}

	ts.SetKey(sk)

	keys = ts.GetMatchingKeys(sk, 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test/cat match")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test/cat")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test match *")
	}

	if keys[0].CurrentValue != nil || !keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test *")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("t*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test match t*")
	}

	if keys[0].CurrentValue != nil || !keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test t*")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if len(keys) != 2 {
		t.Fatal("test/cat match **")
	}

	if keys[0].CurrentValue != nil || !keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test **")
	}

	if keys[1].CurrentValue != nil || keys[1].HasChildren || keys[1].HasValue || keys[1].Metadata != nil || keys[1].Relationships != nil {
		t.Error("test/cat **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "**"), 0, 100)
	if len(keys) != 2 {
		t.Fatal("test/cat match **/**")
	}

	if keys[0].CurrentValue != nil || !keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test **/**")
	}

	if keys[1].CurrentValue != nil || keys[1].HasChildren || keys[1].HasValue || keys[1].Metadata != nil || keys[1].Relationships != nil {
		t.Error("test/cat **/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat match test/**")
	}

	if keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test/cat test/**")
	}

	if keys[0].Key != "/test/cat" {
		t.Error("test/cat path test/**")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateMidLevel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("test", "cat", "calico")
	ts.SetKey(sk)

	keys := ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if len(keys) != 3 {
		t.Fatal("test/cat/calico match **")
	}

	if keys[0].Key != "/test" || keys[0].CurrentValue != nil || !keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test **")
	}

	if keys[1].Key != "/test/cat" || keys[1].CurrentValue != nil || !keys[1].HasChildren || keys[1].HasValue || keys[1].Metadata != nil || keys[1].Relationships != nil {
		t.Error("test/cat **")
	}

	if keys[2].Key != "/test/cat/calico" || keys[2].CurrentValue != nil || keys[2].HasChildren || keys[2].HasValue || keys[2].Metadata != nil || keys[2].Relationships != nil {
		t.Error("test/cat/calico **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "calico"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match **/calico")
	}

	if keys[0].Key != "/test/cat/calico" || keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("test/cat **/calico")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "calico"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/**/calico")
	}

	if keys[0].Key != "/test/cat/calico" || keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("verify test/**/calico")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "cat", "calico"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/**/cat/calico")
	}

	if keys[0].Key != "/test/cat/calico" || keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("verify test/**/cat/calico")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "cat", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/**/cat/**")
	}

	if keys[0].Key != "/test/cat/calico" || keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("verify test/**/cat/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "cat", "**", "calico", "**"), 0, 100)
	if len(keys) != 0 {
		t.Fatal("test/cat/calico match test/**/cat/**/calico/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "c*", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/c*/**")
	}

	if keys[0].Key != "/test/cat/calico" || keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("verify test/c*/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "*", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/*/**")
	}

	if keys[0].Key != "/test/cat/calico" || keys[0].CurrentValue != nil || keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("verify test/*/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "*", "**"), 0, 100)
	if len(keys) != 2 {
		t.Fatal("test/cat/calico match **/*/**")
	}

	if keys[0].Key != "/test/cat" || keys[0].CurrentValue != nil || !keys[0].HasChildren || keys[0].HasValue || keys[0].Metadata != nil || keys[0].Relationships != nil {
		t.Error("verify **/*")
	}

	if keys[1].Key != "/test/cat/calico" || keys[1].CurrentValue != nil || keys[1].HasChildren || keys[1].HasValue || keys[1].Metadata != nil || keys[1].Relationships != nil {
		t.Error("verify **/*/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "*", "dog"), 0, 100)
	if len(keys) != 0 {
		t.Error("test/cat/calico match test/*/dog")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "dog"), 0, 100)
	if len(keys) != 0 {
		t.Error("test/cat/calico match test/**/dog")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateRanges(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("test", "cat", "ragdoll")
	sk2 := MakeStoreKey("test", "cat", "maine coon")
	sk3 := MakeStoreKey("test", "cat", "siberian")
	sk4 := MakeStoreKey("test", "dog", "brittany")
	sk5 := MakeStoreKey("test", "dog", "springer spaniel")

	ts.SetKey(sk1)
	ts.SetKey(sk2)
	ts.SetKey(sk3)
	ts.SetKey(sk4)
	ts.SetKey(sk5)

	pattern := MakeStoreKey("test", "*", "**")

	keys := ts.GetMatchingKeys(pattern, 0, 0)
	if keys == nil || len(keys) != 0 {
		t.Error("limit 0")
	}

	keys = ts.GetMatchingKeys(pattern, 0, 1)
	if len(keys) != 1 || keys[0].Key != "/test/cat/maine coon" {
		t.Error("limit 1")
	}

	keys = ts.GetMatchingKeys(pattern, 1, 1)
	if len(keys) != 1 || keys[0].Key != "/test/cat/ragdoll" {
		t.Error("limit 1 second")
	}

	keys = ts.GetMatchingKeys(pattern, 2, 2)
	if len(keys) != 2 || keys[0].Key != "/test/cat/siberian" || keys[1].Key != "/test/dog/brittany" {
		t.Error("limit 2 at 2")
	}

	pattern2 := MakeStoreKey("test", "**", "*n")

	keys = ts.GetMatchingKeys(pattern2, 0, 1)
	if len(keys) != 1 || keys[0].Key != "/test/cat/maine coon" {
		t.Error("test/**/*n limit 1")
	}

	keys = ts.GetMatchingKeys(pattern2, 1, 1)
	if len(keys) != 1 || keys[0].Key != "/test/cat/siberian" {
		t.Error("test/**/*n limit 1 at 1")
	}

	keys = ts.GetMatchingKeys(pattern2, 2, 1)
	if len(keys) != 0 {
		t.Error("test/**/*n start 2")
	}

	pattern3 := MakeStoreKey("test", "*", "*n")

	keys = ts.GetMatchingKeys(pattern3, 0, 1)
	if len(keys) != 1 || keys[0].Key != "/test/cat/maine coon" {
		t.Error("test/*/*n limit 1")
	}

	keys = ts.GetMatchingKeys(pattern3, 1, 1)
	if len(keys) != 1 || keys[0].Key != "/test/cat/siberian" {
		t.Error("test/*/*n limit 1 at 1")
	}

	keys = ts.GetMatchingKeys(pattern3, 2, 1)
	if len(keys) != 0 {
		t.Error("test/*/*n start 2")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestValueIterateSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	values := ts.GetMatchingKeyValues(MakeStoreKey(), 0, 100)
	if values == nil || len(values) != 0 {
		t.Fatal("sentinel match")
	}

	ts.SetKeyValue(MakeStoreKey(), 320)

	values = ts.GetMatchingKeyValues(MakeStoreKey(), 0, 100)
	if values == nil || len(values) != 1 {
		t.Fatal("sentinel match")
	}

	if values[0].CurrentValue != 320 || values[0].HasChildren || values[0].Metadata != nil || values[0].Relationships != nil {
		t.Error("sentinel has value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestValueIterate(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("test", "cat", "ragdoll")
	sk2 := MakeStoreKey("test", "cat", "maine coon")
	sk3 := MakeStoreKey("test", "cat", "siberian")
	sk4 := MakeStoreKey("test", "dog", "brittany")
	sk5 := MakeStoreKey("test", "dog", "springer spaniel")
	sk6 := MakeStoreKey("test", "cat", "aaa")
	sk7 := MakeStoreKey("test", "dog", "aaa")

	ts.SetKeyValue(sk1, 101)
	ts.SetKeyValue(sk2, 102)
	ts.SetKeyValue(sk3, 103)
	ts.SetKeyValue(sk4, 104)
	ts.SetKeyValue(sk5, 105)

	ts.SetKey(sk6)
	ts.SetKey(sk7)

	pattern := MakeStoreKey("test", "*", "**")

	values := ts.GetMatchingKeyValues(pattern, 0, 0)
	if values == nil || len(values) != 0 {
		t.Error("limit 0")
	}

	values = ts.GetMatchingKeyValues(pattern, 0, 1)
	if len(values) != 1 || values[0].Key != "/test/cat/maine coon" {
		t.Error("limit 1")
	}

	values = ts.GetMatchingKeyValues(pattern, 1, 1)
	if len(values) != 1 || values[0].Key != "/test/cat/ragdoll" {
		t.Error("limit 1 second")
	}

	values = ts.GetMatchingKeyValues(pattern, 2, 2)
	if len(values) != 2 || values[0].Key != "/test/cat/siberian" || values[1].Key != "/test/dog/brittany" {
		t.Error("limit 2 at 2")
	}

	pattern2 := MakeStoreKey("test", "**", "*n")

	values = ts.GetMatchingKeyValues(pattern2, 0, 1)
	if len(values) != 1 || values[0].Key != "/test/cat/maine coon" {
		t.Error("test/**/*n limit 1")
	}

	values = ts.GetMatchingKeyValues(pattern2, 1, 1)
	if len(values) != 1 || values[0].Key != "/test/cat/siberian" {
		t.Error("test/**/*n limit 1 at 1")
	}

	values = ts.GetMatchingKeyValues(pattern2, 2, 1)
	if len(values) != 0 {
		t.Error("test/**/*n start 2")
	}

	pattern3 := MakeStoreKey("test", "*", "*n")

	values = ts.GetMatchingKeyValues(pattern3, 0, 1)
	if len(values) != 1 || values[0].Key != "/test/cat/maine coon" {
		t.Error("test/*/*n limit 1")
	}

	values = ts.GetMatchingKeyValues(pattern3, 1, 1)
	if len(values) != 1 || values[0].Key != "/test/cat/siberian" {
		t.Error("test/*/*n limit 1 at 1")
	}

	values = ts.GetMatchingKeyValues(pattern3, 2, 1)
	if len(values) != 0 {
		t.Error("test/*/*n start 2")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
