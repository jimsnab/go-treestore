package treestore

import (
	"context"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestMathOneValue(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk, "1")
	if addr != 2 || newVal != float64(1) {
		t.Error("set constant")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(1) {
		t.Error("verify constant")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathIncrement(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk, "self+1")
	if addr != 0 || newVal != nil {
		t.Error("increment missing constant")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "float(self)+1")
	if addr != 2 || newVal != float64(1) {
		t.Error("increment constant")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(1) {
		t.Error("verify constant")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "self+1")
	if addr != 2 || newVal != float64(2) {
		t.Error("increment constant")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(2) {
		t.Error("verify constant")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathIncrementShortcut(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk, "i+1")
	if addr != 2 || newVal != int(1) {
		t.Error("increment missing")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val != int(1) {
		t.Error("verify int")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "u+1")
	if addr != 2 || newVal != uint(2) {
		t.Error("increment int to uint")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != uint(2) {
		t.Error("verify uint")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "u+u")
	if addr != 2 || newVal != uint(4) {
		t.Error("increment uint plus uint")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != uint(4) {
		t.Error("verify doubling")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "u+i+f")
	if addr != 2 || newVal != float64(12) {
		t.Error("mismatched types normalized")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(12) {
		t.Error("verify 3x")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "f-1")
	if addr != 2 || newVal != float64(11) {
		t.Error("float decrement")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != float64(11) {
		t.Error("verify float")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathTime(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk, "utc()")
	if addr != 2 || newVal.(int64) < 100 {
		t.Error("set seconds")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "i<100?100:fail()")
	if addr != 0 || newVal != nil {
		t.Error("false path")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "i>100?100:fail()")
	if addr != 2 || newVal != 100 {
		t.Error("true path")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val != int(100) {
		t.Error("verify set")
	}

	addr, newVal = ts.CalculateKeyValue(sk, "i<=100?fail():200")
	if addr != 0 || newVal != nil {
		t.Error("false path")
	}

	val, ke, ve = ts.GetKeyValue(sk)
	if !ke || !ve || val != int(100) {
		t.Error("verify set")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathTimeNs(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk := MakeStoreKey("test")

	nowNs := time.Now().UTC().UnixNano()

	addr, newVal := ts.CalculateKeyValue(sk, "utcns()")
	if addr != 2 || newVal.(int64) <= nowNs {
		t.Error("set nanoseconds")
	}

	val, ke, ve := ts.GetKeyValue(sk)
	if !ke || !ve || val.(int64) <= nowNs {
		t.Error("verify set")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathLookup(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test1")
	sk2 := MakeStoreKey("test2")

	ts.SetKeyValue(sk2, 220)

	addr, newVal := ts.CalculateKeyValue(sk1, `i + lookup("/test2")`)
	if addr != 3 || newVal != 220 {
		t.Error("lookup add")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != 220 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathLookupMissing(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test1")

	addr, newVal := ts.CalculateKeyValue(sk1, `i + lookup("/test2")`)
	if addr != 0 || newVal != nil {
		t.Error("lookup missing")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathLookupRelative(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey("test", "data")

	ts.SetKeyValue(sk2, 220)

	addr, newVal := ts.CalculateKeyValue(sk1, `i + lookup("data")`)
	if addr != 2 || newVal != 220 {
		t.Error("lookup add")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != 220 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathLookupSentinel(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey()

	ts.SetKeyValue(sk2, 220)

	addr, newVal := ts.CalculateKeyValue(sk1, `i + lookup("")`)
	if addr != 2 || newVal != 220 {
		t.Error("lookup add")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != 220 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathLookupInvalid(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")
	sk2 := MakeStoreKey()

	ts.SetKeyValue(sk2, 220)

	addr, newVal := ts.CalculateKeyValue(sk1, `i + lookup(22)`)
	if addr != 0 || newVal != nil {
		t.Error("lookup invalid")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCastInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int(-2.5)`)
	if addr != 2 || newVal != -2 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != -2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCastUint(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `uint(-2.5)`)
	if addr != 2 || newVal.(uint) != 18446744073709551614 {
		t.Error("convert to uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(uint) != 18446744073709551614 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCastFloat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `float(uint(-2.5))`)
	if addr != 2 || newVal.(float64) != 18446744073709551614 {
		t.Error("convert to float from uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(float64) != 18446744073709551614 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCastStringInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int("-2")`)
	if addr != 2 || newVal != -2 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != -2 {
		t.Error("verify")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `int("-2.2")`)
	if addr != 0 || newVal != nil {
		t.Error("not an int")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCastStringUint(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `uint("2")`)
	if addr != 2 || newVal.(uint) != 2 {
		t.Error("convert to uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(uint) != 2 {
		t.Error("verify")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `uint("-2")`)
	if addr != 0 || newVal != nil {
		t.Error("not a uint")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCastStringFloat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `float("-2.2")`)
	if addr != 2 || newVal.(float64) != -2.2 {
		t.Error("convert to float")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(float64) != -2.2 {
		t.Error("verify")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `float("bad")`)
	if addr != 0 || newVal != nil {
		t.Error("not a float")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastFloatInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int(float(-2.2))`)
	if addr != 2 || newVal != -2 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != -2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastFloatUint(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `uint(float(-2.2))`)
	if addr != 2 || newVal.(uint) != 18446744073709551614 {
		t.Error("convert to uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(uint) != 18446744073709551614 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastUintInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int(uint(-2))`)
	if addr != 2 || newVal != -2 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != -2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastTimeInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int(utc())`)
	if addr != 2 || newVal.(int64) == 0 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int64) == 0 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastTimeUint(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `uint(utc())`)
	if addr != 2 || newVal.(uint) == 0 {
		t.Error("convert to uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(uint) == 0 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastUintFloat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `float(uint(-2))`)
	if addr != 2 || newVal.(float64) != 18446744073709551614 {
		t.Error("convert to uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(float64) != 18446744073709551614 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastIntUint(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `uint(int(-2))`)
	if addr != 2 || newVal.(uint) != 18446744073709551614 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(uint) != 18446744073709551614 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastIntFloat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `float(int(-2))`)
	if addr != 2 || newVal.(float64) != -2 {
		t.Error("convert to float")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(float64) != -2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastIntInt(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int(int(-2))`)
	if addr != 2 || newVal != -2 {
		t.Error("convert to int")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(int) != -2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastUintUint(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `uint(uint(2))`)
	if addr != 2 || newVal.(uint) != 2 {
		t.Error("convert to uint")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(uint) != 2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastFloatFloat(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `float(float(-2))`)
	if addr != 2 || newVal.(float64) != -2 {
		t.Error("convert to float")
	}

	val, ke, ve := ts.GetKeyValue(sk1)
	if !ke || !ve || val.(float64) != -2 {
		t.Error("verify")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastBool(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `int(true))`)
	if addr != 0 || newVal != nil {
		t.Error("bool to int")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `uint(true))`)
	if addr != 0 || newVal != nil {
		t.Error("bool to uint")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `float(true))`)
	if addr != 0 || newVal != nil {
		t.Error("bool to float")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathCrossCastCustom(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")
	m := map[string]string{}

	ts.SetKeyValue(sk1, m)

	addr, newVal := ts.CalculateKeyValue(sk1, `int(self)`)
	if addr != 0 || newVal != nil {
		t.Error("self to int")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `uint(self)`)
	if addr != 0 || newVal != nil {
		t.Error("self to uint")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `float(self)`)
	if addr != 0 || newVal != nil {
		t.Error("self to float")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathBadCast(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `float(1, 2)`)
	if addr != 0 || newVal != nil {
		t.Error("bad float")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `int(1, 2)`)
	if addr != 0 || newVal != nil {
		t.Error("bad int")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `uint(1, 2)`)
	if addr != 0 || newVal != nil {
		t.Error("bad uint")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathZeroCast(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)

	sk1 := MakeStoreKey("test")
	addr, newVal := ts.CalculateKeyValue(sk1, `int()`)
	if addr != 2 || newVal != 0 {
		t.Error("zero int")
	}

	sk2 := MakeStoreKey("test2")
	addr, newVal = ts.CalculateKeyValue(sk2, `uint()`)
	if addr != 3 || newVal.(uint) != 0 {
		t.Error("zero int")
	}

	sk3 := MakeStoreKey("test3")
	addr, newVal = ts.CalculateKeyValue(sk3, `float()`)
	if addr != 4 || newVal.(float64) != 0 {
		t.Error("zero int")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathBadExpression(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `self+`)
	if addr != 0 || newVal != nil {
		t.Error("bad expression")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathBadLookup(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `lookup()`)
	if addr != 0 || newVal != nil {
		t.Error("bad lookup")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathBadFail(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `fail(true)`)
	if addr != 0 || newVal != nil {
		t.Error("bad fail")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `fail(false, false)`)
	if addr != 0 || newVal != nil {
		t.Error("bad fail")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `fail(25)`)
	if addr != 0 || newVal != nil {
		t.Error("bad fail")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `fail(false)`)
	if addr != 2 || newVal != nil {
		t.Error("bad fail")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}

func TestMathBadTime(t *testing.T) {
	ts := NewTreeStore(lane.NewTestingLane(context.Background()), 0)
	sk1 := MakeStoreKey("test")

	addr, newVal := ts.CalculateKeyValue(sk1, `utc(10)`)
	if addr != 0 || newVal != nil {
		t.Error("bad utc")
	}

	addr, newVal = ts.CalculateKeyValue(sk1, `utcns(10)`)
	if addr != 0 || newVal != nil {
		t.Error("bad utcns")
	}

	if !ts.DiagDump() {
		t.Error("final diag dump")
	}
}
