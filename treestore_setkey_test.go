package treestore

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestSetKeyOne(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk)
	if address != verifyAddr || !exists {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyIfExistsOne(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")
	testSk := MakeStoreKey("other")

	testAddr, exists := ts.SetKey(testSk)
	if testAddr == 0 || exists {
		t.Error("test key exists")
	}

	address, exists := ts.SetKeyIfExists(testSk, sk)
	if address == 0 || exists {
		t.Error("set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("set indexed")
	}

	address2, exists := ts.SetKeyIfExists(testSk, sk)
	if address2 != address || !exists {
		t.Error("set2")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyDoesntExistOne(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")
	testSk := MakeStoreKey("other")

	address, exists := ts.SetKeyIfExists(testSk, sk)
	if address != 0 || exists {
		t.Error("set")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOneTwoLevels(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "abc")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk)
	if address != verifyAddr || !exists {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOneThreeLevels(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test", "abc", "def")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk)
	if address != verifyAddr || !exists {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyTwoTwoLevels(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "abc")

	firstAddr, exists := ts.SetKey(sk1)
	if firstAddr == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	secondAddr, exists := ts.SetKey(sk2)
	if firstAddr == 0 || exists {
		t.Error("second set")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("set first again")
	}

	verifyAddr, exists = ts.SetKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("set second again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyTwoTwoLevelsFlip(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test", "abc")
	sk2 := MakeStoreKey("test")

	firstAddr, exists := ts.SetKey(sk1)
	if firstAddr == 0 || exists {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk1)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	secondAddr, exists := ts.SetKey(sk2)
	if secondAddr == 0 || !exists {
		t.Error("second set")
	}

	verifyAddr, exists = ts.LocateKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk2)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	verifyAddr, exists = ts.SetKey(sk1)
	if firstAddr != verifyAddr || !exists {
		t.Error("set first again")
	}

	verifyAddr, exists = ts.SetKey(sk2)
	if secondAddr != verifyAddr || !exists {
		t.Error("set second again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetNotExist(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, nil)
	if address != 0 || !exists || orgVal != nil {
		t.Error("first set again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetNotExistValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, 400, SetExMustNotExist, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, nil)
	if address != 0 || !exists || orgVal != 400 {
		t.Error("first set again")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetMustExist(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustExist, 0, nil)
	if address != 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set again")
	}

	address2, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate|SetExMustExist, 0, nil)
	if address2 != address || !exists || orgVal != nil {
		t.Error("set exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetMustExistValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, 400, SetExMustExist, 0, nil)
	if address != 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 401, 0, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set again")
	}

	address2, exists, orgVal := ts.SetKeyValueEx(sk, 402, SetExMustExist, 0, nil)
	if address2 != address || !exists || orgVal != 401 {
		t.Error("set exists")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetDbValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey()

	address, exists := ts.SetKey(sk)
	if address != 1 || !exists {
		t.Error("first db set")
	}

	address, exists = ts.SetKeyValue(sk, 25)
	if address != 1 || !exists {
		t.Error("first db value set")
	}

	address, exists, orgVal := ts.SetKeyValueEx(sk, 26, SetExMustExist, 0, nil)
	if address != 1 || !exists || orgVal != 25 {
		t.Error("first setex")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 27, 0, 0, nil)
	if address != 1 || !exists || orgVal != 26 {
		t.Error("first setex again")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, 28, SetExMustNotExist, 0, nil)
	if address != 0 || !exists || orgVal != 27 {
		t.Error("must not exist")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetExNoValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	address, exists, orgVal := ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, nil)
	if address == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	verifyAddr, exists := ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate first set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("first set indexed")
	}

	address, exists, orgVal = ts.SetKeyValueEx(sk, nil, SetExNoValueUpdate, 0, nil)
	if address == 0 || !exists || orgVal != nil {
		t.Error("set again")
	}

	verifyAddr, exists = ts.LocateKey(sk)
	if address != verifyAddr || !exists {
		t.Error("locate second set")
	}

	verifyAddr, exists = ts.IsKeyIndexed(sk)
	if verifyAddr != 0 || exists {
		t.Error("second set indexed")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("pet", "cat")
	sk2 := MakeStoreKey("sound", "meow")
	sk3 := MakeStoreKey("color", "multi")

	address2, exists := ts.SetKey(sk2)
	if address2 == 0 || exists {
		t.Error("first set")
	}

	address1, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address2})
	if address1 == 0 || exists || orgVal != nil {
		t.Error("second set")
	}

	// setting a relationship gives the key a value, even if nil
	verifyVal, keyExists, valueExists := ts.GetKeyValue(sk1)
	if verifyVal != nil || !keyExists || !valueExists {
		t.Error("value verify")
	}

	address3, exists, orgVal := ts.SetKeyValueEx(sk3, "calico", 0, 0, []StoreAddress{address1})
	if address3 == 0 || exists || orgVal != nil {
		t.Error("third set")
	}

	sk4 := MakeStoreKey("sound", "roar")
	address4, exists := ts.SetKey(sk4)
	if address4 == 0 || exists {
		t.Error("fourth set")
	}

	verifyAddr, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address4})
	if verifyAddr != address1 || !exists || orgVal != nil {
		t.Error("change relationship")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOneRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("index", "1")
	sk2 := MakeStoreKey("data", "sample")

	address2, exists := ts.SetKey(sk2)
	if address2 == 0 || exists {
		t.Error("second set")
	}

	address1, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address2})
	if address1 == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	hasLink, rv := ts.GetRelationshipValue(sk1, 0)
	if !hasLink || rv == nil || rv.CurrentValue != nil || rv.Sk.Path != "/data/sample" {
		t.Error("follow relationship")
	}

	hasLink, rv = ts.GetRelationshipValue(sk1, 1)
	if hasLink || rv != nil {
		t.Error("no relationship")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOneRelationshipWithValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("index", "1")
	sk2 := MakeStoreKey("data", "sample")

	address2, firstValue := ts.SetKeyValue(sk2, 687)
	if address2 == 0 || !firstValue {
		t.Error("second set")
	}

	address1, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{address2})
	if address1 == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	hasLink, rv := ts.GetRelationshipValue(sk1, 0)
	if !hasLink || rv == nil || rv.CurrentValue != 687 || rv.Sk.Path != "/data/sample" {
		t.Error("follow relationship")
	}

	hasLink, rv = ts.GetRelationshipValue(sk1, 1)
	if hasLink || rv != nil {
		t.Error("no relationship")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyOddRelationships(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("index", "1")
	sk2 := MakeStoreKey("index", "2")

	address1, exists, orgVal := ts.SetKeyValueEx(sk1, nil, SetExNoValueUpdate, 0, []StoreAddress{200})
	if address1 == 0 || exists || orgVal != nil {
		t.Error("first set")
	}

	address2, exists, orgVal := ts.SetKeyValueEx(sk2, nil, SetExNoValueUpdate, 0, []StoreAddress{0, 1})
	if address2 == 0 || exists || orgVal != nil {
		t.Error("second set")
	}

	hasLink, rv := ts.GetRelationshipValue(sk1, 0)
	if !hasLink || rv != nil {
		t.Error("address doesn't exist")
	}

	hasLink, rv = ts.GetRelationshipValue(sk2, 0)
	if hasLink || rv != nil {
		t.Error("link to nothing")
	}

	hasLink, rv = ts.GetRelationshipValue(sk2, 1)
	if !hasLink || rv == nil || len(rv.Sk.Tokens) != 0 {
		t.Error("link to sentinel")
	}

	hasLink, rv = ts.GetRelationshipValue(MakeStoreKey("missing"), 1)
	if hasLink || rv != nil {
		t.Error("no key")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestSetKeyNoValueRelationship(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("a")
	sk2 := MakeStoreKey("b")

	addr1, _ := ts.SetKey(sk1)
	if addr1 != 2 {
		t.Error("set key")
	}

	addr2, _, _ := ts.SetKeyValueEx(sk2, nil, SetExNoValueUpdate, 0, []StoreAddress{addr1})
	if addr2 != 3 {
		t.Error("set key 2")
	}

	hasLink, rv := ts.GetRelationshipValue(sk2, 0)
	if !hasLink || rv == nil || rv.Sk.Path != "/a" {
		t.Error("key link")
	}

	if rv.CurrentValue != nil {
		t.Error("value nil")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestGetKeyByAddress(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("fox", "mouse", "chicken")

	address, exists := ts.SetKey(sk)
	if address == 0 || exists {
		t.Error("key set")
	}

	sk2, exists := ts.KeyFromAddress(address)
	if !exists || sk2.Path != "/fox/mouse/chicken" {
		t.Error("key from address")
	}

	sk2, exists = ts.KeyFromAddress(1)
	if !exists || sk2.Path != "" {
		t.Error("sentinel from address")
	}

	sk2, exists = ts.KeyFromAddress(0)
	if exists {
		t.Error("null address")
	}

	sk2, exists = ts.KeyFromAddress(100)
	if exists {
		t.Error("missing address")
	}

	keyExists, valueExists, sk2, value := ts.KeyValueFromAddress(address)
	if !keyExists || valueExists || sk2.Path != "/fox/mouse/chicken" || value != nil {
		t.Error("value from address")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestGetValueKeyByAddress(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey("fox", "mouse", "chicken")

	address, firstValue := ts.SetKeyValue(sk, 972)
	if address == 0 || !firstValue {
		t.Error("value set")
	}

	sk2, exists := ts.KeyFromAddress(address)
	if !exists || sk2.Path != "/fox/mouse/chicken" {
		t.Error("key from address")
	}

	sk2, exists = ts.KeyFromAddress(1)
	if !exists || sk2.Path != "" {
		t.Error("sentinel from address")
	}

	sk2, exists = ts.KeyFromAddress(0)
	if exists {
		t.Error("null address")
	}

	sk2, exists = ts.KeyFromAddress(100)
	if exists {
		t.Error("missing address")
	}

	keyExists, valueExists, sk2, value := ts.KeyValueFromAddress(address)
	if !keyExists || !valueExists || sk2.Path != "/fox/mouse/chicken" || value != 972 {
		t.Error("value from address")
	}

	keyExists, valueExists, sk2, value = ts.KeyValueFromAddress(1)
	if !keyExists || valueExists || sk2.Path != "" || value != nil {
		t.Error("sentinal key from address")
	}

	keyExists, valueExists, sk2, value = ts.KeyValueFromAddress(0)
	if keyExists || valueExists || value != nil {
		t.Error("null value address")
	}

	keyExists, valueExists, sk2, value = ts.KeyValueFromAddress(100)
	if keyExists || valueExists || value != nil {
		t.Error("invalid value address")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestGetSentinelValueByAddress(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk := MakeStoreKey()

	address, firstValue := ts.SetKeyValue(sk, 972)
	if address != 1 || !firstValue {
		t.Error("value set")
	}

	keyExists, valueExists, sk2, value := ts.KeyValueFromAddress(address)
	if !keyExists || !valueExists || sk2.Path != "" || value != 972 {
		t.Error("value from address")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
