package treestore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type (
	JsonOptions int
)

const (
	JsonStringValuesAsKeys JsonOptions = 1 << iota
)

// Retrieves the child key tree and leaf values in the form of json. If
// metadata "array" is "true" then the child key nodes are treated as
// array indicies. (They must be big endian uint32.)
func (ts *TreeStore) GetKeyAsJson(sk StoreKey, opts JsonOptions) (jsonData []byte, err error) {
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	var jd any

	level, tokenIndex, kn, expired := ts.locateKeyNodeForReadLocked(sk)
	defer ts.completeKeyNodeRead(level)

	if tokenIndex >= len(sk.Tokens) && !expired {
		jd = ts.buildJsonLevel(kn, opts)
	}

	jsonData, err = json.Marshal(jd)
	return
}

func (ts *TreeStore) buildJsonLevel(kn *keyNode, opts JsonOptions) any {
	if kn.metadata != nil {
		isArray := kn.metadata["array"]
		if isArray == "true" {
			return ts.buildJsonLevelArray(kn, opts)
		}
	}

	if kn.nextLevel != nil {
		level := kn.nextLevel
		level.lock.RLock()
		ts.activeLocks.Add(1)
		defer ts.completeKeyNodeRead(level)

		if (opts & JsonStringValuesAsKeys) != 0 {
			if level.tree.nodes == 1 {
				if level.tree.root.value.current == nil &&
					level.tree.root.value.nextLevel == nil {
					return string(level.tree.root.key)
				}
			}
		}

		m := map[string]any{}
		level.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			m[string(node.key)] = ts.buildJsonLevel(node.value, opts)
			return true
		})
		return m
	} else if kn.current != nil {
		switch t := kn.current.value.(type) {
		case int:
			return float64(t)
		case uint:
			return float64(t)
		case int8:
			return float64(t)
		case uint8:
			return float64(t)
		case int16:
			return float64(t)
		case uint16:
			return float64(t)
		case int32:
			return float64(t)
		case uint32:
			return float64(t)
		case int64:
			return float64(t)
		case uint64:
			return float64(t)
		case float32:
			return float64(t)
		case nil, float64, string, bool:
			return t
		}
	}

	return nil
}

func (ts *TreeStore) buildJsonLevelArray(kn *keyNode, opts JsonOptions) []any {
	if kn.nextLevel == nil {
		return []any{}
	}

	level := kn.nextLevel
	level.lock.RLock()
	ts.activeLocks.Add(1)
	defer ts.completeKeyNodeRead(level)

	a := make([]any, level.tree.nodes)
	level.tree.Iterate(func(node *avlNode[*keyNode]) bool {
		var n uint32
		if len(node.key) == 4 {
			n = binary.BigEndian.Uint32(node.key)
			if n >= uint32(level.tree.nodes) {
				return true // ignore invalid
			}
		} else {
			return true // ignore invalid
		}

		a[n] = ts.buildJsonLevel(node.value, opts)
		return true
	})

	return a
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk exists, its value, children and history are deleted, and the new
// json data takes its place.
func (ts *TreeStore) SetKeyJson(sk StoreKey, jsonData []byte, opts JsonOptions) (replaced bool, address StoreAddress, err error) {
	// build up the new node before locking
	newKn, err := ts.newJsonKey(jsonData, opts)
	if err != nil {
		return
	}

	// node linkage will change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	kn, level, created := ts.ensureKey(sk)
	defer ts.completeKeyNodeWrite(level)

	if !created {
		replaced = true
		ts.resetNode(sk, kn)
	}

	ts.assignJsonKey(sk, kn, newKn)
	address = kn.address
	return
}

// Saves a json object under a temporary name. A one minute expiration is set.
// This is used in the case where the caller has multiple operations to perform
// to stage data, and then atomically commits it with MoveKey or MoveReferencedKey.
// If the caller happens to abort, the staged data expires.
//
// The caller provides a staging key, and the json data is stored under a subkey
// with a unique identifier.
func (ts *TreeStore) StageKeyJson(stagingSk StoreKey, jsonData []byte, opts JsonOptions) (tempSk StoreKey, address StoreAddress, err error) {
	// build up the new node before locking
	newKn, err := ts.newJsonKey(jsonData, opts)
	if err != nil {
		return
	}

	// node linkage will change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	// temp key segment name matches the address of the key for convenience
	// ensure the base key exists
	_, baseLevel, _ := ts.ensureKey(stagingSk)
	ts.completeKeyNodeWrite(baseLevel)

	addr := StoreAddress(ts.nextAddress.Load() + 1)
	tempSk = AppendStoreKeySegmentStrings(stagingSk, fmt.Sprintf("%d", addr))

	kn, level, created := ts.ensureKey(tempSk)
	defer ts.completeKeyNodeWrite(level)

	if !created {
		err = errors.New("temporary key collision")
		return
	}

	kn.expiration = time.Now().Add(time.Minute).UnixNano()

	// the newKn's address is the temp key unique value
	ts.assignJsonKey(tempSk, kn, newKn)
	if addr != kn.address {
		panic("unexpected address")
	}
	address = addr
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk doesn't exists, no changes are made. Otherwise the key node's
// value and children are deleted, and the new json data takes its place.
func (ts *TreeStore) ReplaceKeyJson(sk StoreKey, jsonData []byte, opts JsonOptions) (replaced bool, address StoreAddress, err error) {
	// build up the new node before locking
	newKn, err := ts.newJsonKey(jsonData, opts)
	if err != nil {
		return
	}

	// node linkage will change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	level, tokenIndex, kn, expired := ts.locateKeyNodeForWriteLocked(sk)
	defer ts.completeKeyNodeWrite(level)

	if tokenIndex < len(sk.Tokens) || expired {
		return
	}

	replaced = true
	ts.resetNode(sk, kn)
	ts.assignJsonKey(sk, kn, newKn)
	address = kn.address
	return
}

// Takes the generalized json data and stores it at the specified key path.
// If the sk exists, no changes are made. Otherwise a new key node is created
// with its child data set according to the json structure.
func (ts *TreeStore) CreateKeyJson(sk StoreKey, jsonData []byte, opts JsonOptions) (created bool, address StoreAddress, err error) {
	// build up the new node before locking
	newKn, err := ts.newJsonKey(jsonData, opts)
	if err != nil {
		return
	}

	// node linkage will change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	level, tokenIndex, kn, expired := ts.locateKeyNodeForLock(sk)
	if tokenIndex >= len(sk.Tokens) {
		if !expired || kn.hasChild() {
			return
		}
		level.lock.Lock()
		ts.activeLocks.Add(1)
		ts.resetNode(sk, kn)
	} else {
		level.lock.Lock()
		ts.activeLocks.Add(1)
		kn, level = ts.createRestOfKey(sk, level, tokenIndex, kn)
	}
	created = true
	defer ts.completeKeyNodeWrite(level)

	ts.assignJsonKey(sk, kn, newKn)
	address = kn.address
	return
}

// Overlays json data on top of existing data. This is one of the slower APIs
// because each part of json is independently written to the store, and a
// write lock is required across the whole operation.
func (ts *TreeStore) MergeKeyJson(sk StoreKey, jsonData []byte, opts JsonOptions) (address StoreAddress, err error) {
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	var data any
	if err = json.Unmarshal(jsonData, &data); err != nil {
		return
	}

	kn, ll, _ := ts.ensureKey(sk)
	defer ts.completeKeyNodeWrite(ll)

	ts.mergeJsonKey(sk, kn, data, opts)
	address = kn.address
	return
}

func (ts *TreeStore) mergeJsonKey(sk StoreKey, kn *keyNode, data any, opts JsonOptions) {
	if (opts & JsonStringValuesAsKeys) == 0 {
		ts.mergeJsonKeyValue(sk, kn, data, opts)
	} else {
		s, is := data.(string)
		if !is {
			ts.mergeJsonKeyValue(sk, kn, data, opts)
		} else {
			key := []byte(s)
			ts.discardChildren(sk, kn)
			_, lockedLevel := ts.ensureMergeChild(kn, key)
			ts.completeKeyNodeWrite(lockedLevel)
		}
	}
}

func (ts *TreeStore) mergeJsonKeyValue(sk StoreKey, kn *keyNode, data any, opts JsonOptions) {
	switch t := data.(type) {
	case nil, float64, string, bool:
		newLeaf := valueInstance{
			value: t,
		}
		kn.history = newAvlTree[*valueInstance]()
		kn.current = &newLeaf
		now := currentUnixTimestampBytes()
		kn.history.Set(now, &newLeaf)
		ts.keys[sk.Path] = kn.address

	case []any:
		if kn.metadata == nil {
			kn.metadata = map[string]string{"array": "true"}
		} else {
			kn.metadata["array"] = "true"
		}

		arrayLen := 0
		if kn.nextLevel != nil {
			arrayLen = kn.nextLevel.tree.nodes
		}

		// append this array to an existing array
		for i, v := range t {
			n := make([]byte, 4)
			binary.BigEndian.PutUint32(n, uint32(i+arrayLen))
			childSk := AppendStoreKeySegments(sk, n)
			childKn, lockedLevel := ts.ensureMergeChild(kn, n)
			ts.mergeJsonKey(childSk, childKn, v, opts)
			ts.completeKeyNodeWrite(lockedLevel)
		}

	case map[string]any:
		for k, v := range t {
			key := []byte(k)
			childSk := AppendStoreKeySegments(sk, key)
			childKn, lockedLevel := ts.ensureMergeChild(kn, key)
			ts.mergeJsonKey(childSk, childKn, v, opts)
			ts.completeKeyNodeWrite(lockedLevel)
		}
	}
}

// Worker for merge that ensures a child key exists
func (ts *TreeStore) ensureMergeChild(parentKn *keyNode, key []byte) (kn *keyNode, lockedLevel *keyTree) {
	if parentKn.nextLevel == nil {
		parentKn.nextLevel = newKeyTree(parentKn)
	}

	lockedLevel = parentKn.nextLevel
	lockedLevel.lock.Lock()
	ts.activeLocks.Add(1)
	avlNode := lockedLevel.tree.Find(key)
	if avlNode != nil {
		kn = avlNode.value
	} else {
		kn = &keyNode{
			key:       key,
			address:   StoreAddress(ts.nextAddress.Add(1)),
			ownerTree: lockedLevel,
		}
		ts.addresses[kn.address] = kn
		lockedLevel.tree.Set(key, kn)
	}

	return
}

// Worker that builds a new tree level with contents of the provided json data.
func (ts *TreeStore) newJsonKey(jsonData []byte, opts JsonOptions) (kn *keyNode, err error) {
	var data any
	if err = json.Unmarshal(jsonData, &data); err != nil {
		return
	}

	kn = &keyNode{}
	ts.nextJsonKeyLevel(kn, data, opts)
	return
}

// Worker that sets a leaf key node value, or recurses to fill the key node's
// child array or map.
func (ts *TreeStore) nextJsonKeyLevel(kn *keyNode, data any, opts JsonOptions) {
	if (opts & JsonStringValuesAsKeys) == 0 {
		ts.nextJsonKeyValueLevel(kn, data, opts)
	} else {
		s, is := data.(string)
		if !is {
			ts.nextJsonKeyValueLevel(kn, data, opts)
		} else {
			level := newKeyTree(kn)
			kn.nextLevel = level
			key := []byte(s)
			childKn := &keyNode{
				key:       key,
				address:   StoreAddress(ts.nextAddress.Add(1)),
				ownerTree: level,
			}

			level.tree.Set(key, childKn)
		}
	}
}

// Worker that sets a leaf key node value, or recurses to fill the key node's
// child array or map.
func (ts *TreeStore) nextJsonKeyValueLevel(kn *keyNode, data any, opts JsonOptions) {
	switch t := data.(type) {
	case nil, string, float64, bool:
		newLeaf := valueInstance{
			value: t,
		}
		kn.history = newAvlTree[*valueInstance]()
		kn.current = &newLeaf
		now := currentUnixTimestampBytes()
		kn.history.Set(now, &newLeaf)

	case []any:
		level := newKeyTree(kn)
		kn.nextLevel = level

		if kn.metadata == nil {
			kn.metadata = map[string]string{"array": "true"}
		} else {
			kn.metadata["array"] = "true"
		}

		for i, v := range t {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))

			childKn := &keyNode{
				key:       key,
				address:   StoreAddress(ts.nextAddress.Add(1)),
				ownerTree: level,
			}

			level.tree.Set(key, childKn)
			ts.nextJsonKeyLevel(childKn, v, opts)
		}

	case map[string]any:
		level := newKeyTree(kn)
		kn.nextLevel = level
		for k, v := range t {
			key := TokenStringToSegment(k)
			childKn := &keyNode{
				key:       key,
				address:   StoreAddress(ts.nextAddress.Add(1)),
				ownerTree: level,
			}

			level.tree.Set(key, childKn)
			ts.nextJsonKeyLevel(childKn, v, opts)
		}
	}
}

// Worker that assigns a json key node tree to its base.
func (ts *TreeStore) assignJsonKey(sk StoreKey, baseKn *keyNode, jsonKn *keyNode) {
	baseKn.current = jsonKn.current
	baseKn.history = jsonKn.history
	baseKn.metadata = jsonKn.metadata
	baseKn.nextLevel = jsonKn.nextLevel
	if baseKn.nextLevel != nil {
		baseKn.nextLevel.parent = baseKn
	}

	ts.assignJsonKeyIndex(sk, baseKn)
	ts.addToIndicies(sk.Tokens, baseKn)
}

// Worker that iterates the newly assigned nodes and ensures they are indexed
func (ts *TreeStore) assignJsonKeyIndex(sk StoreKey, kn *keyNode) {
	if kn.current != nil {
		ts.keys[sk.Path] = kn.address
	}
	ts.addresses[kn.address] = kn

	if kn.nextLevel != nil {
		kn.nextLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			nextSk := AppendStoreKeySegments(sk, node.key)
			ts.assignJsonKeyIndex(nextSk, node.value)
			return true
		})
	}
}
