package treestore

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

func TestIterateLevelEmpty(t *testing.T) {
	ts := NewTreeStore()

	keys, count := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty root")
	}

	ts.SetKey(MakeStoreKey())

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty root with sentinel")
	}

	ts.SetKeyValue(MakeStoreKey(), 1)

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty root with sentinel value")
	}

	keys, count = ts.GetLevelKeys(MakeStoreKey("not there"), "*", 0, 100)
	if keys != nil || count != 0 {
		t.Error("empty root no key")
	}
}

func TestIterateLevelRoot(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("a")

	ts.SetKey(sk)

	keys, count := ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "a" || count != 1 {
		t.Error("one node")
	}

	sk2 := MakeStoreKey("b", "c")

	ts.SetKey(sk2)

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "a" || string(keys[1].segment) != "b" || count != 2 {
		t.Error("two nodes")
	}

	sk3 := MakeStoreKey("d")

	ts.SetKey(sk3)

	keys, count = ts.GetLevelKeys(MakeStoreKey(), "*", 0, 100)
	if len(keys) != 3 || string(keys[0].segment) != "a" || string(keys[1].segment) != "b" || string(keys[2].segment) != "d" || count != 3 {
		t.Error("three nodes")
	}
}

func TestIterateLevelNoBase(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("a", "b", "c")

	ts.SetKey(sk)

	keys, count := ts.GetLevelKeys(MakeStoreKey("d"), "*", 0, 100)
	if keys != nil || count != 0 {
		t.Error("no base match")
	}
}

func TestIterateSecondLevel(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("a")

	keys, count := ts.GetLevelKeys(sk, "*", 0, 100)
	if keys != nil || count != 0 {
		t.Error("no second level")
	}

	ts.SetKey(sk)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty second level")
	}

	sk2 := MakeStoreKey("a", "cat")

	ts.SetKey(sk2)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || keys[0].hasChildren {
		t.Error("key only node")
	}

	sk3 := MakeStoreKey("a", "cat", "dog")

	ts.SetKey(sk3)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("key only node")
	}

	sk4 := MakeStoreKey("a", "dog")

	ts.SetKeyValue(sk4, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags")
	}

	if !keys[1].hasValue || keys[1].hasChildren {
		t.Error("dog node flags")
	}

	sk5 := MakeStoreKey("a", "dog", "fido")

	ts.SetKeyValue(sk5, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags 2")
	}

	if !keys[1].hasValue || !keys[1].hasChildren {
		t.Error("dog node flags 2")
	}
}

func TestIterateThirdLevel(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("data", "test")

	keys, count := ts.GetLevelKeys(sk, "*", 0, 100)
	if keys != nil || count != 0 {
		t.Error("no second level")
	}

	ts.SetKey(sk)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if keys == nil || len(keys) != 0 || count != 0 {
		t.Error("empty second level")
	}

	sk2 := MakeStoreKey("data", "test", "cat")

	ts.SetKey(sk2)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || keys[0].hasChildren {
		t.Error("key only node")
	}

	sk3 := MakeStoreKey("data", "test", "cat", "dog")

	ts.SetKey(sk3)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 1 || string(keys[0].segment) != "cat" || count != 1 {
		t.Error("one node")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("key only node")
	}

	sk4 := MakeStoreKey("data", "test", "dog")

	ts.SetKeyValue(sk4, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags")
	}

	if !keys[1].hasValue || keys[1].hasChildren {
		t.Error("dog node flags")
	}

	sk5 := MakeStoreKey("data", "test", "dog", "fido")

	ts.SetKeyValue(sk5, 80)

	keys, count = ts.GetLevelKeys(sk, "*", 0, 100)
	if len(keys) != 2 || string(keys[0].segment) != "cat" || string(keys[1].segment) != "dog" || count != 2 {
		t.Error("two nodes")
	}

	if keys[0].hasValue || !keys[0].hasChildren {
		t.Error("cat node flags 2")
	}

	if !keys[1].hasValue || !keys[1].hasChildren {
		t.Error("dog node flags 2")
	}
}

func TestIterateLevelPages(t *testing.T) {
	ts := NewTreeStore()

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
	keys, count := ts.GetLevelKeys(root, "*", 0, 0)
	if keys == nil || len(keys) != 0 || count != 250 {
		t.Error("empty page")
	}

	for i := 0; i < 250; i++ {
		keys, count = ts.GetLevelKeys(root, "*", i, 0)
		if keys == nil || len(keys) != 0 || count != 250 {
			t.Error("empty page with non zero start")
		}

		keys, count = ts.GetLevelKeys(root, "*", i, 1)
		if keys == nil || len(keys) != 1 || count != 250 {
			t.Error("one item page")
		}

		if string(keys[0].segment) != values[i] {
			t.Error("value mismatch")
		}

		remaining := 250 - i
		remaining2 := 2
		if remaining < remaining2 {
			remaining2 = remaining
		}
		keys, count = ts.GetLevelKeys(root, "*", i, 2)
		if keys == nil || len(keys) != remaining2 || count != 250 {
			t.Error("two item page")
		}

		if string(keys[0].segment) != values[i] {
			t.Error("first value mismatch")
		}
		if len(keys) == 2 {
			if string(keys[1].segment) != values[i+1] {
				t.Error("second value mismatch")
			}
		}

		keys, count = ts.GetLevelKeys(root, "*", i, remaining)
		if keys == nil || len(keys) != remaining || count != 250 {
			t.Error("remaining items page")
		}

		for j := 0; j < remaining; j++ {
			if string(keys[j].segment) != values[i+j] {
				t.Errorf("page value %d", i+j)
			}
		}

		keys, count = ts.GetLevelKeys(root, "*", i, 1000)
		if keys == nil || len(keys) != remaining || count != 250 {
			t.Error("large page with start offset")
		}

		for j := 0; j < remaining; j++ {
			if string(keys[j].segment) != values[i+j] {
				t.Errorf("page value %d", i+j)
			}
		}
	}
}

func TestIterateLevelPattern(t *testing.T) {
	ts := NewTreeStore()

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
	keys, count := ts.GetLevelKeys(root, "", 0, 100)
	if keys == nil || len(keys) != 0 || count != 250 {
		t.Error("empty page")
	}

	keys, count = ts.GetLevelKeys(root, values[0], 0, 100)
	if len(keys) != 1 || count != 250 {
		t.Error("exact match")
	}

	ones := 0
	for _, val := range values {
		if strings.HasPrefix(val, "1") {
			ones++
		}
	}

	keys, count = ts.GetLevelKeys(root, "1*", 0, 250)
	if len(keys) != ones || count != 250 {
		t.Error("match ones")
	}
}

func TestFullIterateSentinel(t *testing.T) {
	ts := NewTreeStore()

	keys := ts.GetMatchingKeys(MakeStoreKey(), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("sentinel match")
	}

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("default sentinel")
	}

	ts.SetKeyValue(MakeStoreKey(), 320)

	keys = ts.GetMatchingKeys(MakeStoreKey(), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("sentinel match")
	}

	if keys[0].currentValue != 320 || keys[0].hasChildren || !keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("sentinel has value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateOneKey(t *testing.T) {
	ts := NewTreeStore()

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

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test key")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match *")
	}

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test key *")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("t*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match t*")
	}

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test key t*")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match **")
	}

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test key **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test key match **/**")
	}

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
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
	ts := NewTreeStore()

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

	if keys[0].currentValue != 330 || keys[0].hasChildren || !keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test value")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match *")
	}

	if keys[0].currentValue != 330 || keys[0].hasChildren || !keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test value *")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("t*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match t*")
	}

	if keys[0].currentValue != 330 || keys[0].hasChildren || !keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test value t*")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match **")
	}

	if keys[0].currentValue != 330 || keys[0].hasChildren || !keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test value **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "**"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test value match **/**")
	}

	if keys[0].currentValue != 330 || keys[0].hasChildren || !keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
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
	ts := NewTreeStore()

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

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test/cat")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test match *")
	}

	if keys[0].currentValue != nil || !keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test *")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("t*"), 0, 100)
	if keys == nil || len(keys) != 1 {
		t.Fatal("test match t*")
	}

	if keys[0].currentValue != nil || !keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test t*")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if len(keys) != 2 {
		t.Fatal("test/cat match **")
	}

	if keys[0].currentValue != nil || !keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test **")
	}

	if keys[1].currentValue != nil || keys[1].hasChildren || keys[1].hasValue || keys[1].metadata != nil || keys[1].relationships != nil {
		t.Error("test/cat **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "**"), 0, 100)
	if len(keys) != 2 {
		t.Fatal("test/cat match **/**")
	}

	if keys[0].currentValue != nil || !keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test **/**")
	}

	if keys[1].currentValue != nil || keys[1].hasChildren || keys[1].hasValue || keys[1].metadata != nil || keys[1].relationships != nil {
		t.Error("test/cat **/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat match test/**")
	}

	if keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test/cat test/**")
	}

	if keys[0].sk.path != "/test/cat" {
		t.Error("test/cat path test/**")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestFullIterateMidLevel(t *testing.T) {
	ts := NewTreeStore()

	sk := MakeStoreKey("test", "cat", "calico")
	ts.SetKey(sk)

	keys := ts.GetMatchingKeys(MakeStoreKey("**"), 0, 100)
	if len(keys) != 3 {
		t.Fatal("test/cat/calico match **")
	}

	if keys[0].sk.path != "/test" || keys[0].currentValue != nil || !keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test **")
	}

	if keys[1].sk.path != "/test/cat" || keys[1].currentValue != nil || !keys[1].hasChildren || keys[1].hasValue || keys[1].metadata != nil || keys[1].relationships != nil {
		t.Error("test/cat **")
	}

	if keys[2].sk.path != "/test/cat/calico" || keys[2].currentValue != nil || keys[2].hasChildren || keys[2].hasValue || keys[2].metadata != nil || keys[2].relationships != nil {
		t.Error("test/cat/calico **")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "calico"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match **/calico")
	}

	if keys[0].sk.path != "/test/cat/calico" || keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("test/cat **/calico")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "calico"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/**/calico")
	}

	if keys[0].sk.path != "/test/cat/calico" || keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("verify test/**/calico")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "cat", "calico"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/**/cat/calico")
	}

	if keys[0].sk.path != "/test/cat/calico" || keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("verify test/**/cat/calico")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "**", "cat", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/**/cat/**")
	}

	if keys[0].sk.path != "/test/cat/calico" || keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
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

	if keys[0].sk.path != "/test/cat/calico" || keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("verify test/c*/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("test", "*", "**"), 0, 100)
	if len(keys) != 1 {
		t.Fatal("test/cat/calico match test/*/**")
	}

	if keys[0].sk.path != "/test/cat/calico" || keys[0].currentValue != nil || keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("verify test/*/**")
	}

	keys = ts.GetMatchingKeys(MakeStoreKey("**", "*", "**"), 0, 100)
	if len(keys) != 2 {
		t.Fatal("test/cat/calico match **/*/**")
	}

	if keys[0].sk.path != "/test/cat" || keys[0].currentValue != nil || !keys[0].hasChildren || keys[0].hasValue || keys[0].metadata != nil || keys[0].relationships != nil {
		t.Error("verify **/*")
	}

	if keys[1].sk.path != "/test/cat/calico" || keys[1].currentValue != nil || keys[1].hasChildren || keys[1].hasValue || keys[1].metadata != nil || keys[1].relationships != nil {
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
	ts := NewTreeStore()

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
	if len(keys) != 1 || keys[0].sk.path != "/test/cat/maine coon" {
		t.Error("limit 1")
	}

	keys = ts.GetMatchingKeys(pattern, 1, 1)
	if len(keys) != 1 || keys[0].sk.path != "/test/cat/ragdoll" {
		t.Error("limit 1 second")
	}

	keys = ts.GetMatchingKeys(pattern, 2, 2)
	if len(keys) != 2 || keys[0].sk.path != "/test/cat/siberian" || keys[1].sk.path != "/test/dog/brittany" {
		t.Error("limit 2 at 2")
	}

	pattern2 := MakeStoreKey("test", "**", "*n")

	keys = ts.GetMatchingKeys(pattern2, 0, 1)
	if len(keys) != 1 || keys[0].sk.path != "/test/cat/maine coon" {
		t.Error("test/**/*n limit 1")
	}

	keys = ts.GetMatchingKeys(pattern2, 1, 1)
	if len(keys) != 1 || keys[0].sk.path != "/test/cat/siberian" {
		t.Error("test/**/*n limit 1 at 1")
	}

	keys = ts.GetMatchingKeys(pattern2, 2, 1)
	if len(keys) != 0 {
		t.Error("test/**/*n start 2")
	}

	pattern3 := MakeStoreKey("test", "*", "*n")

	keys = ts.GetMatchingKeys(pattern3, 0, 1)
	if len(keys) != 1 || keys[0].sk.path != "/test/cat/maine coon" {
		t.Error("test/*/*n limit 1")
	}

	keys = ts.GetMatchingKeys(pattern3, 1, 1)
	if len(keys) != 1 || keys[0].sk.path != "/test/cat/siberian" {
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
	ts := NewTreeStore()

	values := ts.GetMatchingKeyValues(MakeStoreKey(), 0, 100)
	if values == nil || len(values) != 0 {
		t.Fatal("sentinel match")
	}

	ts.SetKeyValue(MakeStoreKey(), 320)

	values = ts.GetMatchingKeyValues(MakeStoreKey(), 0, 100)
	if values == nil || len(values) != 1 {
		t.Fatal("sentinel match")
	}

	if values[0].currentValue != 320 || values[0].hasChildren || values[0].metadata != nil || values[0].relationships != nil {
		t.Error("sentinel has value")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestValueIterate(t *testing.T) {
	ts := NewTreeStore()

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
	if len(values) != 1 || values[0].sk.path != "/test/cat/maine coon" {
		t.Error("limit 1")
	}

	values = ts.GetMatchingKeyValues(pattern, 1, 1)
	if len(values) != 1 || values[0].sk.path != "/test/cat/ragdoll" {
		t.Error("limit 1 second")
	}

	values = ts.GetMatchingKeyValues(pattern, 2, 2)
	if len(values) != 2 || values[0].sk.path != "/test/cat/siberian" || values[1].sk.path != "/test/dog/brittany" {
		t.Error("limit 2 at 2")
	}

	pattern2 := MakeStoreKey("test", "**", "*n")

	values = ts.GetMatchingKeyValues(pattern2, 0, 1)
	if len(values) != 1 || values[0].sk.path != "/test/cat/maine coon" {
		t.Error("test/**/*n limit 1")
	}

	values = ts.GetMatchingKeyValues(pattern2, 1, 1)
	if len(values) != 1 || values[0].sk.path != "/test/cat/siberian" {
		t.Error("test/**/*n limit 1 at 1")
	}

	values = ts.GetMatchingKeyValues(pattern2, 2, 1)
	if len(values) != 0 {
		t.Error("test/**/*n start 2")
	}

	pattern3 := MakeStoreKey("test", "*", "*n")

	values = ts.GetMatchingKeyValues(pattern3, 0, 1)
	if len(values) != 1 || values[0].sk.path != "/test/cat/maine coon" {
		t.Error("test/*/*n limit 1")
	}

	values = ts.GetMatchingKeyValues(pattern3, 1, 1)
	if len(values) != 1 || values[0].sk.path != "/test/cat/siberian" {
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
