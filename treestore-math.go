package treestore

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Knetic/govaluate"
)

type (
	typeConverter func(x any) any
)

var intConverter typeConverter = func(x any) any {
	switch t := x.(type) {
	case int:
		return t
	case int64: // time
		return t
	case uint:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, err := strconv.ParseInt(t, 10, 32)
		if err != nil {
			return err
		}
		return int(n)
	default:
		return errors.New("incompatible type")
	}
}

var uintConverter typeConverter = func(x any) any {
	switch t := x.(type) {
	case int:
		return uint(t)
	case int64: // time
		return uint(t)
	case uint:
		return t
	case float64:
		return uint(t)
	case string:
		n, err := strconv.ParseUint(t, 10, 32)
		if err != nil {
			return err
		}
		return uint(n)
	default:
		return errors.New("incompatible type")
	}
}

var floatConverter typeConverter = func(x any) any {
	switch t := x.(type) {
	case int:
		return float64(t)
	case int64: // time
		return float64(t)
	case uint:
		return float64(t)
	case float64:
		return t
	case string:
		n, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return err
		}
		return n
	default:
		return errors.New("incompatible type")
	}
}

var defaultConverter typeConverter = func(x any) any { return x }

// Evaluate a math expression and store the result.
//
// The expression operators include + - / * & | ^ ** % >> <<,
// comparators >, <=, etc., and logical || &&.
//
// Constants are 64-bit floating point, string constants, dates or true/false.
//
// Parenthesis specify order of evaluation.
//
// Unary operators ! - ~ are supported.
//
// Ternary conditionals are supported with <expr> ? <on-true> : <on-false>
//
// Null coalescence is supported with ??
//
// Basic type conversion is supported - int(value), uint(value) and float(value)
//
// The target's store key original value is accessed with variable 'self'.
//
// The 'self' can also be referred to as 'i' for int, 'u' for uint or 'f' for float,
// for which if there are no other types specified, the result will be stored as
// the type specified. This is useful for compact, simple expressions such as:
//
//	"i+1"        increments existing int (or zero), stores result as int
//
// The operation is computed in 64-bit floating point before it is stored in its
// final type.
//
// String values can be converted in casts, e.g., int("-35")
//
// Other input keys can be accessed using the lookup(sk) function, where sk is the
// key path containing a value.
//
//	`lookup("/my/store/key")+25`
//
// If the initial slash is not specified, the store key path is a child of the
// target sk.
//
// For ternary conditionals, an operation can be skipped by using fail().
//
//	"i>100?i+1:fail()"        no modifications if the sk value is < 100
func (ts *TreeStore) CalculateKeyValue(sk StoreKey, expression string) (address StoreAddress, newValue any) {
	var mathExtensions = map[string]govaluate.ExpressionFunction{
		"lookup": func(args ...any) (any, error) {
			if len(args) != 1 {
				return nil, errors.New("invalid expression")
			}
			subpath, valid := args[0].(string)
			if !valid {
				return nil, errors.New("invalid expression")
			}

			var subSk StoreKey
			if subpath != "" {
				if subpath[0] != '/' {
					subSk = MakeStoreKeyFromPath(sk.Path + "/" + TokenPath(subpath))
				} else {
					subSk = MakeStoreKeyFromPath(TokenPath(subpath))
				}
			} else {
				subSk = MakeStoreKey()
			}

			ll, tokenIndex, kn, expired := ts.locateKeyNodeForReadLocked(subSk)
			defer ts.completeKeyNodeRead(ll)
			if tokenIndex < len(subSk.Tokens) || expired || kn.current == nil {
				return nil, fmt.Errorf("value doesn't exist: %s", string(subSk.Path))
			}

			return floatConverter(kn.current.value), nil
		},
		"utcns": func(args ...any) (any, error) {
			if len(args) != 0 {
				return nil, errors.New("invalid expression")
			}

			return time.Now().UTC().UnixNano(), nil
		},
		"utc": func(args ...any) (any, error) {
			if len(args) != 0 {
				return nil, errors.New("invalid expression")
			}

			return time.Now().UTC().Unix(), nil
		},
		"fail": func(args ...any) (any, error) {
			if len(args) == 1 {
				b, ok := args[0].(bool)
				if !ok {
					return nil, errors.New("invalid expression")
				}
				if b {
					return nil, errors.New("expression terminated")
				} else {
					return nil, nil
				}
			}
			if len(args) != 0 {
				return nil, errors.New("invalid expression")
			}

			return nil, errors.New("expression terminated")
		},
		"int": func(args ...any) (any, error) {
			if len(args) == 0 {
				return int(0), nil
			}
			if len(args) != 1 {
				return nil, errors.New("invalid expression")
			}

			return intConverter(args[0]), nil
		},
		"uint": func(args ...any) (any, error) {
			if len(args) == 0 {
				return uint(0), nil
			}
			if len(args) != 1 {
				return nil, errors.New("invalid expression")
			}

			return uintConverter(args[0]), nil
		},
		"float": func(args ...any) (any, error) {
			if len(args) == 0 {
				return float64(0), nil
			}
			if len(args) != 1 {
				return nil, errors.New("invalid expression")
			}

			return floatConverter(args[0]), nil
		},
	}

	expr, err := govaluate.NewEvaluableExpressionWithFunctions(expression, mathExtensions)
	if err != nil {
		return
	}
	now := currentUnixTimestampBytes()

	// the key node linkage may change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	params := map[string]any{}
	_, tokenIndex, kn, expired := ts.locateKeyNodeForLock(sk)
	if tokenIndex >= len(sk.Tokens) && !expired && kn.current != nil {
		v := kn.current.value
		params["self"] = v
		f := floatConverter(v)
		i := floatConverter(v)
		u := floatConverter(v)
		params["f"] = f
		params["i"] = i
		params["u"] = u
	} else {
		params["self"] = nil
		params["f"] = float64(0)
		params["i"] = float64(0)
		params["u"] = float64(0)
	}

	var typeVar = ""
	for _, token := range expr.Tokens() {
		if token.Kind == govaluate.VARIABLE {
			v := token.Value.(string)
			switch v {
			case "i", "u", "f":
				if typeVar == "" {
					typeVar = v
				} else if typeVar != v {
					typeVar = "X"
				}
			default:
				typeVar = "X"
			}
		}
	}

	var toType typeConverter
	switch typeVar {
	case "i":
		toType = intConverter
	case "u":
		toType = uintConverter
	case "f":
		toType = floatConverter
	default:
		toType = defaultConverter
	}

	result, err := expr.Evaluate(params)
	if err != nil {
		return
	}

	result = toType(result)
	switch result.(type) {
	case error:
		return
	}

	kn, ll, _ := ts.ensureKeyWithValue(sk)
	defer ts.completeKeyNodeWrite(ll)

	if kn.history == nil {
		kn.history = newAvlTree[*valueInstance]()
	}

	newLeaf := &valueInstance{
		value: result,
	}

	kn.current = newLeaf
	kn.history.Set(now, newLeaf)

	address = kn.address
	newValue = result
	return
}
