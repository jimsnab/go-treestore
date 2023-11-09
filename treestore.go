package treestore

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	TreeStore struct {
		l            lane.Lane
		appVersion   int
		keyNodeMu    sync.RWMutex
		dbNode       keyNode
		dbNodeLevel  *keyTree
		nextAddress  atomic.Uint64
		addresses    map[StoreAddress]*keyNode
		keys         map[TokenPath]StoreAddress
		cas          map[StoreAddress]uint64
		activeLocks  atomic.Int32
		deferredRefs []*deferredRef
		sanityAddr   map[StoreAddress]TokenPath
	}

	StoreAddress uint64

	keyTree struct {
		lock   sync.RWMutex
		tree   *avlTree[*keyNode]
		parent *keyNode
	}

	keyNode struct {
		key        []byte
		address    StoreAddress
		ownerTree  *keyTree
		nextLevel  *keyTree
		current    *valueInstance
		history    *avlTree[*valueInstance]
		expiration int64
		metadata   map[string]string
		autoLinks  *keyAutoLinks
	}

	valueInstance struct {
		value         any
		relationships []StoreAddress
	}

	SetExFlags int

	RelationshipValue struct {
		Sk           StoreKey
		CurrentValue any
	}

	deferredRef struct {
		target TokenPath
		vi     *valueInstance
		index  int
	}

	keyNodeCallback func(level *keyTree, kn *keyNode)
)

const (
	SetExMustExist SetExFlags = 1 << iota
	SetExMustNotExist
	SetExNoValueUpdate
)

func NewTreeStore(l lane.Lane, appVersion int) *TreeStore {
	ts := TreeStore{
		l:          l,
		appVersion: appVersion,
		keys:       map[TokenPath]StoreAddress{},
		cas:        map[StoreAddress]uint64{},
	}
	ts.nextAddress.Store(1)

	ts.dbNodeLevel = newKeyTree(&ts.dbNode)

	ts.dbNode.address = 1
	ts.dbNode.ownerTree = ts.dbNodeLevel
	ts.dbNode.key = []byte{}
	ts.dbNodeLevel.tree.Set(ts.dbNode.key, &ts.dbNode)
	ts.addresses = map[StoreAddress]*keyNode{1: &ts.dbNode}
	ts.sanityAddr = map[StoreAddress]TokenPath{}
	return &ts
}

func newKeyTree(parent *keyNode) *keyTree {
	return &keyTree{
		tree:   newAvlTree[*keyNode](),
		parent: parent,
	}
}

// Looks up the store key using the full key path, returning a read lock if a value exists
// for the key.
//
// - Empty sk (zero tokens): `kn` points to the dbRoot key node
// - Key node found: `kn` points to the leaf node of the key path
// - Key node not found: `kn` is nil
//
// If `kn` is non-nil, the caller receives read lock ownership of `lockedLevel`.
// If `kn` is nil, `lockedLevel` is nil also.
func (ts *TreeStore) getKeyNodeForValueRead(sk StoreKey) (kn *keyNode, lockedLevel *keyTree) {
	if len(sk.Tokens) == 0 {
		kn = &ts.dbNode
		lockedLevel = kn.ownerTree
		lockedLevel.lock.RLock()
		ts.activeLocks.Add(1)
		return
	}

	// ensure the linkage between keynodes remains stable
	ts.keyNodeMu.RLock()

	address := ts.keys[sk.Path]
	if address != 0 {
		kn = ts.addresses[address]
		if /*kn == nil ||*/ kn.expiration > 0 && kn.expiration < time.Now().UTC().UnixNano() {
			// not found because it is expired
			kn = nil
			ts.keyNodeMu.RUnlock()
			return
		}

		lockedLevel = kn.ownerTree
		lockedLevel.lock.RLock()
		ts.activeLocks.Add(1)
	}

	ts.keyNodeMu.RUnlock()
	return
}

// Looks up the store key using the full key path, returning a write lock if a value exists
// for the key. This is only appropriate for writes that don't alter the key node linkage,
// such as a value update.
//
// - Empty sk (zero tokens): `kn` points to the dbRoot key node
// - Key node found: `kn` points to the leaf node of the key path
// - Key node not found: `kn` is nil
//
// If `kn` is non-nil, the caller receives read/write lock ownership of `lockedLevel`.
// If `kn` is nil, `lockedLevel` is nil also.
func (ts *TreeStore) getKeyNodeForWrite(sk StoreKey) (kn *keyNode, lockedLevel *keyTree) {
	// ensure the linkage between keynodes remains stable
	ts.keyNodeMu.RLock()

	address := ts.keys[sk.Path]
	if address != 0 {
		kn = ts.addresses[address]
		if kn == nil || kn.expiration > 0 && kn.expiration < time.Now().UTC().UnixNano() {
			// not found because it is expired
			kn = nil
			ts.keyNodeMu.RUnlock()
			return
		}

		lockedLevel = kn.ownerTree
		lockedLevel.lock.Lock()
		ts.activeLocks.Add(1)
	}

	ts.keyNodeMu.RUnlock()
	return
}

// Worker that traverses the levels according to the key path. The caller must
// have a lock on ts.keyNodeMu.
func (ts *TreeStore) locateKeyNodeForLock(sk StoreKey) (level *keyTree, tokenIndex int, kn *keyNode, expired bool) {
	// can traverse levels freely thanks to keyNodeMu
	tokens := sk.Tokens
	kn = &ts.dbNode
	level = ts.dbNodeLevel

	end := len(tokens)
	for tokenIndex = 0; tokenIndex < end; tokenIndex++ {
		if kn.nextLevel == nil {
			break
		}

		level = kn.nextLevel

		avlNode := level.tree.Find(tokens[tokenIndex])
		if avlNode == nil {
			kn = nil
			return
		}

		kn = avlNode.value
	}

	expired = kn.isExpired()
	return
}

// Worker that traverses the levels according to the key path. If a
// segment in sk is nil, the level is iterated.
//
// The caller must have a lock on ts.keyNodeMu.
func (ts *TreeStore) locateKeyNodesLocked(sk StoreKey, callback keyNodeCallback) {
	// can traverse levels freely thanks to keyNodeMu
	ts.locateKeyNodesWorker(sk.Tokens, ts.dbNodeLevel, &ts.dbNode, callback)
}

// recursive worker
func (ts *TreeStore) locateKeyNodesWorker(tokens TokenSet, level *keyTree, kn *keyNode, callback keyNodeCallback) {
	if len(tokens) == 0 {
		// expired included
		callback(level, kn)
		return
	}
	token := tokens[0]

	level = kn.nextLevel
	if level == nil {
		return
	}
	subTokens := tokens[1:]

	if token != nil {
		avlNode := level.tree.Find(token)
		if avlNode == nil {
			return
		}

		ts.locateKeyNodesWorker(subTokens, level, avlNode.value, callback)
	} else {
		level.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			ts.locateKeyNodesWorker(subTokens, level, node.value, callback)
			return true
		})
	}
}

// Wrapper that ensures keyNodeMu is locked - see locateKeyNodeForReadLocked for details.
func (ts *TreeStore) locateKeyNodeForRead(sk StoreKey) (level *keyTree, tokenIndex int, kn *keyNode, expired bool) {
	// ensure the linkage between keynodes remains stable
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()
	return ts.locateKeyNodeForReadLocked(sk)
}

// Walks the levels of the tree store, locating the specified key's leaf key node,
// or the last parent. The caller must have a lock on ts.keyNodeMu.
//
// loc.kn will provide the located key node at the specified sk, or nil if it doesn't
// exist.
//
// The caller always receives ownership of a read lock in loc.level, even if the leaf
// was not found (loc.kn is not nil). The caller must release this lock.
//
// loc.index will provide the token index where the search ended and will be equal
// to len(sk.Tokens) upon a full match.
//
// An empty store key locks the sentinel key node.
func (ts *TreeStore) locateKeyNodeForReadLocked(sk StoreKey) (level *keyTree, tokenIndex int, kn *keyNode, expired bool) {
	level, tokenIndex, kn, expired = ts.locateKeyNodeForLock(sk)
	level.lock.RLock()
	ts.activeLocks.Add(1)
	return
}

// Walks the levels of the tree store, locating the specified key's leaf key node,
// or the last parent. The caller must have a lock on ts.keyNodeMu.
//
// The caller always receives ownership of a read/write lock in loc.level and must
// release it.
//
// loc.kn will provide the located key node at the specified sk, or nil if it doesn't
// exist.
//
// loc.index will provide the token index where the search ended and will be equal
// to len(sk.Tokens) upon a full match.
//
// An empty store key locks the sentinel key node.
func (ts *TreeStore) locateKeyNodeForWriteLocked(sk StoreKey) (level *keyTree, tokenIndex int, kn *keyNode, expired bool) {
	level, tokenIndex, kn, expired = ts.locateKeyNodeForLock(sk)
	level.lock.Lock()
	ts.activeLocks.Add(1)
	return
}

// Unlocks a valueInstance key node accessed for read
func (ts *TreeStore) completeKeyNodeRead(level *keyTree) {
	level.lock.RUnlock()
	ts.activeLocks.Add(-1)
}

// Unlocks a valueInstance key node accessed for write
func (ts *TreeStore) completeKeyNodeWrite(level *keyTree) {
	ts.sanityCheck()
	level.lock.Unlock()
	ts.activeLocks.Add(-1)
}

// worker - caller must hold a write lock on ts.keyNodeMu
func (ts *TreeStore) appendKeyNode(lockedLevel *keyTree, token TokenSegment) (kn *keyNode) {
	kn = &keyNode{
		key:       token,
		address:   StoreAddress(ts.nextAddress.Add(1)),
		ownerTree: lockedLevel,
	}

	lockedLevel.tree.Set(kn.key, kn)
	ts.addresses[kn.address] = kn
	return
}

// worker - caller must hold a write lock on ts.keyNodeMu
func (ts *TreeStore) createRestOfKey(sk StoreKey, parentLevel *keyTree, index int, parent *keyNode) (kn *keyNode, lockedLevel *keyTree) {
	// currently holding the write lock on the level to append, and caller
	// ensures at least one more key token needs to be added to the store

	lockedLevel = parentLevel
	kn = parent

	// add middle levels if necessary
	for end := len(sk.Tokens); index < end; index++ {
		if kn != nil {
			kn.nextLevel = newKeyTree(kn)

			// moving the lock ensures proper cleanup; there won't be lock contention
			// because the caller holds a write lock on ts.keyNodeMu
			kn.nextLevel.lock.Lock()
			lockedLevel.lock.Unlock()

			lockedLevel = kn.nextLevel
		}

		kn = ts.appendKeyNode(lockedLevel, sk.Tokens[index])
	}

	ts.addAutoLinks(sk.Tokens, kn, false)
	return
}

// worker - like createRestOfKey, but used when caller has acquired the exclusive database lock
func (ts *TreeStore) createRestOfKeyExclusive(sk StoreKey, parentLevel *keyTree, tokenIndex int, parent *keyNode, updateIndexes bool) (kn *keyNode) {
	// currently have the entire database locked
	level := parentLevel
	kn = parent

	// add middle levels if necessary
	for end := len(sk.Tokens); tokenIndex < end; tokenIndex++ {
		if kn != nil {
			kn.nextLevel = newKeyTree(kn)
			level = kn.nextLevel
		}

		kn = ts.appendKeyNode(level, sk.Tokens[tokenIndex])
	}

	if updateIndexes {
		ts.addAutoLinks(sk.Tokens, kn, false)
	}
	return
}

// worker - caller must hold a write lock on ts.keyNodeMu
func (ts *TreeStore) removeKeyFromIndexLocked(sk StoreKey) (removed bool) {
	_, removed = ts.keys[sk.Path]
	if removed {
		delete(ts.keys, sk.Path)
	}
	return
}

func (ts *TreeStore) repurposeExpiredKn(sk StoreKey, kn *keyNode) {
	delete(ts.keys, sk.Path)
	delete(ts.addresses, kn.address)
	ts.purgeIndicies(kn)
	kn.address = StoreAddress(ts.nextAddress.Add(1))
	ts.addresses[kn.address] = kn
	kn.current = nil
	kn.expiration = 0
	kn.history = nil
	kn.metadata = nil

	ts.addAutoLinks(sk.Tokens, kn, false)
}

// Worker to make sure a key exists, and returns the valueInstance key node and a write lock on
// the last level; the caller must release lockedLevel.lock.
// The caller must have a write lock on ts.keyNodeMu.
func (ts *TreeStore) ensureKey(sk StoreKey) (kn *keyNode, lockedLevel *keyTree, created bool) {
	lockedLevel, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)

	if expired {
		ts.repurposeExpiredKn(sk, kn)
		created = true
	} else if index < len(sk.Tokens) {
		kn, lockedLevel = ts.createRestOfKey(sk, lockedLevel, index, kn)
		created = true
	}

	return
}

// Worker like ensureKey but used when the caller has locked the whole database
func (ts *TreeStore) ensureKeyExclusive(sk StoreKey, updateIndexes bool) (kn *keyNode, created bool) {
	level, index, kn, expired := ts.locateKeyNodeForLock(sk)

	if expired {
		ts.repurposeExpiredKn(sk, kn)
		created = true
	} else if index < len(sk.Tokens) {
		kn = ts.createRestOfKeyExclusive(sk, level, index, kn, updateIndexes)
		created = true
	}

	return
}

// Worker to make sure an indexed key exists, and returns the valueInstance key node and a write
// lock on the last level; the caller must release lockedLevel.lock.
// This function is similar to ensureKey except it adds the key node to the index of values.
// The caller must have a write lock on ts.keyNodeMu.
func (ts *TreeStore) ensureKeyWithValue(sk StoreKey) (kn *keyNode, lockedLevel *keyTree, created bool) {
	lockedLevel, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)

	if expired {
		ts.repurposeExpiredKn(sk, kn)
		ts.keys[sk.Path] = kn.address
		created = true
	} else if index < len(sk.Tokens) {
		kn, lockedLevel = ts.createRestOfKey(sk, lockedLevel, index, kn)
		created = true
		ts.keys[sk.Path] = kn.address
	} else {
		_, exists := ts.keys[sk.Path]
		if !exists {
			ts.keys[sk.Path] = kn.address
			created = true
		}
	}

	return
}

// Set a key without a value and without an expiration, doing nothing if the
// key already exists. The key index is not altered.
func (ts *TreeStore) SetKey(sk StoreKey) (address StoreAddress, exists bool) {
	// the key node linkage may change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	kn, ll, created := ts.ensureKey(sk)
	defer ts.completeKeyNodeWrite(ll)

	address = kn.address
	exists = !created
	return
}

// If the test key exists, set a key without a value and without an expiration,
// doing nothing if the test key does not exist or if the key already exists.
// The key index is not altered.
//
// If the test key does not exist, address will be returned as 0.
// The return value 'exists' is true if the target sk exists.
func (ts *TreeStore) SetKeyIfExists(testKey, sk StoreKey) (address StoreAddress, exists bool) {
	// the key node linkage may change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	testLevel, tokenIndex, _, expired := ts.locateKeyNodeForReadLocked(testKey)
	ts.completeKeyNodeRead(testLevel)

	if tokenIndex >= len(testKey.Tokens) && !expired {
		kn, ll, created := ts.ensureKey(sk)
		defer ts.completeKeyNodeWrite(ll)

		address = kn.address
		exists = !created
	}

	return
}

// Set a key with a value, without an expiration, adding to value history if the
// key already exists.
func (ts *TreeStore) SetKeyValue(sk StoreKey, value any) (address StoreAddress, firstValue bool) {
	newLeaf := &valueInstance{
		value: value,
	}

	now := currentUnixTimestampBytes()

	// the key node linkage may change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	kn, ll, created := ts.ensureKeyWithValue(sk)
	defer ts.completeKeyNodeWrite(ll)

	if kn.history == nil {
		kn.history = newAvlTree[*valueInstance]()
	}

	kn.current = newLeaf
	kn.history.Set(now, newLeaf)

	address = kn.address
	firstValue = created
	return
}

// Ensures a key exists, optionally sets a value, optionally sets or removes key expiration, and
// optionally replaces the relationships array.
//
// Flags:
//
//	SetExNoValueUpdate - do not alter the key's value (ignore `value` argument, do not alter key index)
//	SetExMustExist - perform only if the key exists
//	SetExMustNotExist - perform only if the key does not exist
//
// For `expireNs`, specify the Unix nanosecond tick of when the key will expire. Specify zero to
// remove expiration. Specify -1 to retain the current key expiration.
//
// `originalValue` will be provided if the key exists and has a value, even if no change is made.
//
// A non-nil `relationships` will replace the relationships of the key node. An empty array
// removes all relationships. Specify nil to retain the current key relationships.
func (ts *TreeStore) SetKeyValueEx(sk StoreKey, value any, flags SetExFlags, expireNs int64, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any) {
	// the key node linkage may change
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	return ts.setKeyValueExLocked(sk, value, flags, expireNs, relationships)
}

func (ts *TreeStore) setKeyValueExLocked(sk StoreKey, value any, flags SetExFlags, expireNs int64, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any) {

	level, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)

	var ll *keyTree
	if expired {
		if (flags & SetExMustExist) != 0 {
			ts.completeKeyNodeWrite(level)
			return
		}

		ts.repurposeExpiredKn(sk, kn)
		ll = level
	} else if index >= len(sk.Tokens) {
		exists = true

		if kn.current != nil {
			originalValue = kn.current.value
		}

		ll = level
		if (flags & SetExMustNotExist) != 0 {
			ts.completeKeyNodeWrite(ll)
			return
		}
	} else {
		if (flags & SetExMustExist) != 0 {
			ts.completeKeyNodeWrite(level)
			return
		}

		kn, ll = ts.createRestOfKey(sk, level, index, kn)
	}

	defer ts.completeKeyNodeWrite(ll)

	if (flags&SetExNoValueUpdate) == 0 || relationships != nil {
		newLeaf := &valueInstance{
			value: value,
		}

		if (flags & SetExNoValueUpdate) != 0 {
			if kn.current != nil {
				newLeaf.value = kn.current.value
			} else {
				newLeaf.value = nil
			}
		}

		if len(relationships) > 0 {
			newLeaf.relationships = relationships
		} else if relationships == nil && kn.current != nil {
			newLeaf.relationships = kn.current.relationships
		}

		now := currentUnixTimestampBytes()
		if kn.history == nil {
			kn.history = newAvlTree[*valueInstance]()
		}

		kn.current = newLeaf
		kn.history.Set(now, newLeaf)
		ts.keys[sk.Path] = kn.address
	}

	if expireNs < -1 {
		expireNs = time.Now().UTC().UnixNano() - expireNs
	}
	if expireNs >= 0 {
		kn.expiration = expireNs
	}

	address = kn.address
	return
}

// Looks up the key in the index and returns true if it exists and has value history.
func (ts *TreeStore) IsKeyIndexed(sk StoreKey) (address StoreAddress, exists bool) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	exists = (kn != nil)
	if exists {
		address = kn.address
		ts.completeKeyNodeRead(ll)
	}
	return
}

// Walks the tree level by level and returns the current address, whether or not
// the key path is indexed. This avoids putting a lock on the index, but will lock
// tree levels while walking the tree.
func (ts *TreeStore) LocateKey(sk StoreKey) (address StoreAddress, exists bool) {
	level, index, kn, expired := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(level)

	exists = (index >= len(sk.Tokens)) && !expired
	if exists {
		address = kn.address
	}
	return
}

// Navigates to the valueInstance key node and returns the expiration time in Unix nanoseconds, or
// -1 if the key path does not exist.
func (ts *TreeStore) GetKeyTtl(sk StoreKey) (ttl int64) {
	level, index, kn, expired := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(level)

	if index >= len(sk.Tokens) && !expired {
		ttl = kn.expiration
	} else {
		ttl = -1
	}
	return
}

// Navigates to the valueInstance key node and sets the expiration time in Unix nanoseconds.
// Specify 0 for no expiration.
func (ts *TreeStore) SetKeyTtl(sk StoreKey, expiration int64) (exists bool) {
	if len(sk.Tokens) == 0 {
		exists = true
		return
	}

	// the key node linkage will not change
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	level, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)
	defer ts.completeKeyNodeWrite(level)

	if index >= len(sk.Tokens) && !expired {
		if expiration >= 0 {
			kn.expiration = expiration
		}
		exists = true
	}
	return
}

// Looks up the key in the index and returns the current value and flags
// that indicate if the key was set, and if so, if it has a value.
func (ts *TreeStore) GetKeyValue(sk StoreKey) (value any, keyExists, valueExists bool) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	if kn != nil {
		keyExists = true
		if kn.current != nil {
			valueExists = true
			value = kn.current.value
		}
		ts.completeKeyNodeRead(ll)
	}
	return
}

// Looks up the key and returns the expiration time in Unix nanoseconds, or
// -1 if the key value does not exist.
func (ts *TreeStore) GetKeyValueTtl(sk StoreKey) (ttl int64) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	if kn != nil {
		ttl = kn.expiration
		ts.completeKeyNodeRead(ll)
	} else {
		ttl = -1
	}
	return
}

// Looks up the key and sets the expiration time in Unix nanoseconds. Specify
// 0 to clear the expiration.
func (ts *TreeStore) SetKeyValueTtl(sk StoreKey, expiration int64) (exists bool) {
	kn, ll := ts.getKeyNodeForWrite(sk)
	if kn != nil {
		if expiration >= 0 {
			kn.expiration = expiration
		}
		ts.completeKeyNodeWrite(ll)
		exists = true
	}
	return
}

// Looks up the key in the index and scans history for the specified Unix ns tick,
// returning the value at that moment in time, if one exists.
//
// To specify a relative time, specify `tickNs` as the negative ns from the current
// time, e.g., -1000000000 is one second ago.
func (ts *TreeStore) GetKeyValueAtTime(sk StoreKey, tickNs int64) (value any, exists bool) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	if kn != nil {
		if tickNs < 0 {
			tickNs = time.Now().UTC().UnixNano() - tickNs
		}
		if kn.history != nil && tickNs >= 0 {
			item := kn.history.FindLeft(unixTimestampBytes(tickNs))
			if item != nil {
				value = item.value.value
				exists = true
			}
		}
		ts.completeKeyNodeRead(ll)
	}

	return
}

// Deletes an indexed key that has a value, including its value history, and its metadata.
// Specify `clean` as `true` to delete parent key nodes that become empty, or `false` to only
// remove the valueInstance key node.
//
// Returns `removed` == true if the value was deleted.
//
// The valueInstance key will still exist if it has children or if it is the sentinel key node.
func (ts *TreeStore) DeleteKeyWithValue(sk StoreKey, clean bool) (removed bool, originalValue any) {
	// acquire right to change the key node linkage
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	return ts.deleteKeyWithValueLocked(sk, clean)
}

func (ts *TreeStore) deleteKeyWithValueLocked(sk StoreKey, clean bool) (removed bool, originalValue any) {
	end := len(sk.Tokens)
	if end == 0 {
		ts.dbNodeLevel.lock.Lock() // ensure any pending operations on this keynode complete
		ts.activeLocks.Add(1)
		if ts.dbNode.current != nil {
			originalValue = ts.dbNode.current.value
			removed = true
			ts.dbNode.current = nil
			ts.dbNode.history = nil
		}
		ts.dbNode.metadata = nil
		ts.removeKeyFromIndexLocked(sk)
		ts.dbNodeLevel.lock.Unlock()
		ts.activeLocks.Add(-1)
		return
	}

	level, index, kn, expired := ts.locateKeyNodeForLock(sk)
	if index < end {
		return
	}

	level.lock.Lock() // ensure any pending operations on the keynode complete
	ts.activeLocks.Add(1)

	if ts.removeKeyFromIndexLocked(sk) || expired {
		ts.removeAutoLinks(sk.Tokens, kn, false)

		if kn.current != nil {
			originalValue = kn.current.value
			kn.current = nil
		}
		kn.history = nil
		kn.metadata = nil

		if kn.nextLevel == nil {
			for tokenIndex := end - 1; tokenIndex >= 0; tokenIndex-- {
				// permanently delete the node
				delete(ts.addresses, kn.address)
				level.tree.Delete(kn.key)
				kn.ownerTree = nil
				ts.purgeIndicies(kn)

				// stop if this level contains siblings
				if level.tree.nodes > 0 {
					break
				}

				// move to the parent and clear linkage to deleted level
				sk = StoreKey{
					Tokens: sk.Tokens[0:tokenIndex],
				}
				sk.Path = TokenSetToTokenPath(sk.Tokens)

				kn = level.parent
				if kn == &ts.dbNode {
					break
				}

				level.parent = nil
				kn.nextLevel = nil

				// stop if not cleaning
				if !clean {
					break
				}

				// stop here if the parent is indexed
				_, indexed := ts.keys[sk.Path]
				if indexed {
					break
				}

				// continue with the parent level
				kn.ownerTree.lock.Lock()
				level.lock.Unlock()
				level = kn.ownerTree
			}
		}

		removed = !expired
	}

	ts.sanityCheck()
	level.lock.Unlock()
	ts.activeLocks.Add(-1)

	return
}

// Deletes a key value, including its value history, and its metadata - and the
// valueInstance key node also if it does not have children.
//
// The parent key node is not altered.
//
// `keyRemoved` == `true` when the valueInstance key node is deleted.
// `valueRemoved` == true if the key value is cleared.
//
// All key nodes along the store key path will be locked during the operation, so
// this operation blocks subsequent operations until it completes.
//
// The sentinal (root) key node cannot be deleted; only its value can be cleared.
func (ts *TreeStore) DeleteKey(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue any) {
	// likely to modify the linkage of keynodes
	ts.keyNodeMu.Lock()
	defer ts.sanityCheck()
	defer ts.keyNodeMu.Unlock()

	keyRemoved, valueRemoved, originalValue, _ = ts.deleteKeyLocked(sk)
	return
}

func (ts *TreeStore) deleteKeyLocked(sk StoreKey) (keyRemoved, valueRemoved bool, originalValue any, parent *keyNode) {
	end := len(sk.Tokens)
	if end == 0 {
		valueRemoved, originalValue = ts.deleteKeyWithValueLocked(sk, true)
		return
	}

	level, index, kn, expired := ts.locateKeyNodeForLock(sk)
	if index < end {
		return
	}

	level.lock.Lock() // ensure any pending operations on the keynode complete
	ts.activeLocks.Add(1)
	defer func() {
		level.lock.Unlock()
		ts.activeLocks.Add(-1)
	}()

	if ts.removeKeyFromIndexLocked(sk) || expired {
		if !expired {
			valueRemoved = true
			originalValue = kn.current.value
		}
		kn.current = nil
	}
	kn.history = nil
	kn.metadata = nil

	if kn.nextLevel == nil {
		if kn.ownerTree != nil {
			parent = kn.ownerTree.parent
		}

		ts.deleteKeyNodeLocked(sk, level, kn)
		keyRemoved = true
	}

	return
}

// Removes the key, and every empty parent key, stopping at (and not deleting) the root key.
func (ts *TreeStore) deleteKeyUpToLocked(rootSk, deleteSk StoreKey) (keyRemoved, valueRemoved bool, originalValue any) {
	tokens := deleteSk.Tokens
	for len(rootSk.Tokens) < len(tokens) {
		sk := MakeStoreKeyFromTokenSegments(tokens...)
		var parent *keyNode
		keyRemoved, valueRemoved, originalValue, parent = ts.deleteKeyLocked(sk)

		if parent == nil || (parent.nextLevel != nil && parent.nextLevel.tree.nodes > 0) {
			break
		}

		tokens = tokens[:len(tokens)-1]
	}

	return
}

func (ts *TreeStore) deleteKeyNodeLocked(sk StoreKey, level *keyTree, kn *keyNode) {
	ts.removeAutoLinks(sk.Tokens, kn, false)

	// permanently delete the node
	delete(ts.addresses, kn.address)
	level.tree.Delete(kn.key)
	kn.ownerTree = nil

	// if the level is empty now, unlink the parent
	if level.tree.nodes == 0 {
		parent := level.parent
		if parent != nil {
			parent.nextLevel = nil
			level.parent = nil
		}
	}

	ts.purgeIndicies(kn)
}

// Sets a metadata attribute on a key, returning the original value (if any)
func (ts *TreeStore) SetMetadataAttribute(sk StoreKey, attribute, value string) (keyExists bool, originalValue string) {
	// the key node linkage will not change
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	level, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)
	defer ts.completeKeyNodeWrite(level)

	if index < len(sk.Tokens) || expired {
		return
	}
	keyExists = true

	if kn.metadata != nil {
		originalValue = kn.metadata[attribute]
	} else {
		kn.metadata = map[string]string{}
	}

	kn.metadata[attribute] = value
	return
}

// Removes a single metadata attribute from a key
func (ts *TreeStore) ClearMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string) {
	// the key node linkage will not change
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	level, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)
	defer ts.completeKeyNodeWrite(level)

	if index < len(sk.Tokens) || expired {
		return
	}

	if kn.metadata != nil {
		originalValue, attributeExists = kn.metadata[attribute]
		if attributeExists {
			if len(kn.metadata) == 1 {
				kn.metadata = nil
			} else {
				delete(kn.metadata, attribute)
			}
		}
	}

	return
}

// Discards all metadata on the specific key
func (ts *TreeStore) ClearKeyMetadata(sk StoreKey) {
	// the key node linkage will not change
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	level, index, kn, expired := ts.locateKeyNodeForWriteLocked(sk)
	defer ts.completeKeyNodeWrite(level)

	if index < len(sk.Tokens) || expired {
		return
	}

	kn.metadata = nil
}

// Fetches a key's metadata value for a specific attribute
func (ts *TreeStore) GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string) {
	level, index, kn, expired := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(level)

	if index < len(sk.Tokens) || expired {
		return
	}

	if kn.metadata != nil {
		value, attributeExists = kn.metadata[attribute]
	}

	return
}

// Returns an array of attribute names of metadata stored for the specified key
func (ts *TreeStore) GetMetadataAttributes(sk StoreKey) (attributes []string) {
	level, index, kn, expired := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(level)

	if index < len(sk.Tokens) || expired {
		return
	}

	if kn.metadata != nil {
		attributes = make([]string, 0, len(kn.metadata))
		for attribute := range kn.metadata {
			attributes = append(attributes, attribute)
		}
		sort.Strings(attributes)
	} else {
		attributes = []string{}
	}

	return
}

func (ts *TreeStore) getTokenSet(kn *keyNode) (tokens TokenSet) {
	for kn != &ts.dbNode {
		tokens = append(TokenSet{kn.key}, tokens...)
		kn = kn.ownerTree.parent
	}
	return
}

func (ts *TreeStore) getTokenSetForAddressLocked(addr StoreAddress) (leaf *keyNode, tokens TokenSet) {
	kn := ts.addresses[addr]
	if kn == nil {
		return
	}
	leaf = kn

	for kn != &ts.dbNode {
		tokens = append(TokenSet{kn.key}, tokens...)
		kn = kn.ownerTree.parent
	}
	return
}

// Converts an address to a store key
func (ts *TreeStore) KeyFromAddress(addr StoreAddress) (sk StoreKey, exists bool) {
	// prevent keynode linkage from changing
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	return ts.keyFromAddressLocked(addr)
}

func (ts *TreeStore) keyFromAddressLocked(addr StoreAddress) (sk StoreKey, exists bool) {
	kn, tokens := ts.getTokenSetForAddressLocked(addr)
	if kn != nil && !kn.isExpired() {
		exists = true
		sk = MakeStoreKeyFromTokenSegments(tokens...)
	}

	return
}

// Fetches the current value by address
func (ts *TreeStore) KeyValueFromAddress(addr StoreAddress) (keyExists, valueExists bool, sk StoreKey, value any) {
	// prevent keynode linkage from changing
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	kn, tokens := ts.getTokenSetForAddressLocked(addr)
	if kn != nil {
		keyExists = true
		if kn.current != nil {
			valueExists = true
			value = kn.current.value
		}
		sk = MakeStoreKeyFromTokenSegments(tokens...)
	}

	return
}

// Retreives a value by following a relationship link. The target value is
// returned in `rv`, and will be nil if the target doesn't exist. The
// `hasLink` flag indicates true when a relationship is stored at the
// specified `relationshipIndex`.
func (ts *TreeStore) GetRelationshipValue(sk StoreKey, relationshipIndex int) (hasLink bool, rv *RelationshipValue) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	if kn == nil {
		return
	}

	if kn.current == nil || kn.current.relationships == nil || len(kn.current.relationships) <= relationshipIndex {
		ts.completeKeyNodeRead(ll)
		return
	}

	targetAddr := kn.current.relationships[relationshipIndex]
	ts.completeKeyNodeRead(ll)

	if targetAddr == 0 {
		return
	}
	hasLink = true

	// prevent key node linkage from changing
	ts.keyNodeMu.RLock()
	defer ts.keyNodeMu.RUnlock()

	kn, tokens := ts.getTokenSetForAddressLocked(targetAddr)
	if kn == nil || kn.isExpired() {
		return
	}

	rv = &RelationshipValue{
		Sk: MakeStoreKeyFromTokenSegments(tokens...),
	}
	if kn.current != nil {
		rv.CurrentValue = kn.current.value
	}

	return
}

// worker - iterates the sublevels and removes all values from the index
// the caller must hold a write lock on ts.keyNodeMu
func (ts *TreeStore) discardChildren(sk StoreKey, kn *keyNode) {
	level := kn.nextLevel
	if level != nil {
		level.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			childSk := AppendStoreKeySegments(sk, node.key)
			ts.discardChildren(childSk, node.value)
			delete(ts.addresses, node.value.address)
			if node.value.current != nil {
				delete(ts.keys, childSk.Path)
			}
			return true
		})
		level.tree.Clear()
		kn.nextLevel = nil
	}
}

// worker - removes the node's value (if any) as well as all child keys
// the caller must hold a write lock on ts.keyNodeMu
func (ts *TreeStore) resetNode(sk StoreKey, kn *keyNode) {
	ts.removeAutoLinks(sk.Tokens, kn, true)

	ts.discardChildren(sk, kn)
	kn.current = nil
	kn.metadata = nil
	kn.expiration = 0
	delete(ts.keys, sk.Path)
}

func (ts *TreeStore) sanityCheck() {
	for sk, addr := range ts.keys {
		_, found := ts.addresses[addr]
		if !found {
			panic(fmt.Sprintf("key %s refers to missing address %d", sk, addr))
		}
	}

	// scan the entire tree store and ensure an address hasn't become a different key
	ts.sanityCheckLevel(ts.dbNode.nextLevel)
}

func (ts *TreeStore) sanityCheckLevel(kt *keyTree) {
	if kt == nil {
		return
	}

	kt.tree.Iterate(func(node *avlNode[*keyNode]) bool {
		kn := node.value
		if kn.current != nil {
			for _, addr := range kn.current.relationships {
				target := ts.addresses[addr]
				if target == nil {
					continue
				}
				path := TokenSetToTokenPath(target.getTokenSet())

				prior, exists := ts.sanityAddr[addr]
				if !exists {
					ts.sanityAddr[addr] = path
				} else if prior != path {
					panic(fmt.Sprintf("reference to address %d changed from %s to %s", addr, prior, path))
				}
			}
		}
		return true
	})
}
