package treestore

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	TreeStore struct {
		l           lane.Lane
		dbNode      keyNode
		nextAddress StoreAddress
		addresses   map[StoreAddress]*keyNode
		keys        map[TokenPath]StoreAddress
		keyMu       sync.RWMutex
		cas         map[StoreAddress]uint64
		activeLocks atomic.Int32
	}

	StoreAddress uint64

	keyTree struct {
		lock   sync.RWMutex
		tree   *AvlTree[*keyNode]
		parent *keyNode
	}

	keyNode struct {
		address    StoreAddress
		ownerTree  *keyTree
		nextLevel  *keyTree
		current    *valueInstance
		history    *AvlTree[*valueInstance]
		expiration int64
		metadata   map[string]string
	}

	valueInstance struct {
		value         any
		relationships []StoreAddress
	}

	keyLocation struct {
		level *keyTree
		index int
		kn    *keyNode
	}

	keyLockPath struct {
		levels    []*keyTree
		lastLevel *keyTree
		kn        *keyNode
	}

	SetExFlags int

	testHookLockPromotion func()
)

var (
	lockPromotion testHookLockPromotion
)

const (
	SetExMustExist SetExFlags = 1 << iota
	SetExMustNotExist
	SetExNoValueUpdate
)

func NewTreeStore(l lane.Lane) *TreeStore {
	dbNode := keyNode{
		address:   1,
		ownerTree: newKeyTree(nil),
	}

	ts := TreeStore{
		l:           l,
		dbNode:      dbNode,
		nextAddress: 1,
		keys:        map[TokenPath]StoreAddress{},
		cas:         map[StoreAddress]uint64{},
	}
	dbNode.ownerTree.tree.Set([]byte{}, &ts.dbNode)
	ts.addresses = map[StoreAddress]*keyNode{1: &ts.dbNode}
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
	if len(sk.tokens) == 0 {
		kn = &ts.dbNode
		lockedLevel = kn.ownerTree
		lockedLevel.lock.RLock()
		ts.activeLocks.Add(1)
		return
	}

	for {
		ts.keyMu.RLock()

		address := ts.keys[sk.path]
		if address != 0 {
			//
			// This code, while holding a read lock on the index, wants to acquire a key node read
			// lock. A deadlock could occur with code that is trying to acquire a write lock on the
			// index while already holding a write lock on the key node.
			//
			// To avoid the deadlock, this code starts over upon a lock collision.
			//
			kn = ts.addresses[address]
			if kn.expiration > 0 && kn.expiration < time.Now().UTC().UnixNano() {
				// not found because it is expired
				kn = nil
				ts.keyMu.RUnlock()
				return
			}

			if lockPromotion != nil {
				lockPromotion() // test hook
			}

			lockedLevel = kn.ownerTree
			obtained := lockedLevel.lock.TryRLock()
			ts.keyMu.RUnlock()

			if obtained {
				ts.activeLocks.Add(1)
				return
			}

			lockedLevel = nil
			kn = nil
		} else {
			// not found
			ts.keyMu.RUnlock()
			return
		}
	}
}

// Looks up the store key using the full key path, returning a write lock if a value exists
// for the key.
//
// - Empty sk (zero tokens): `kn` points to the dbRoot key node
// - Key node found: `kn` points to the leaf node of the key path
// - Key node not found: `kn` is nil
//
// If `kn` is non-nil, the caller receives read/write lock ownership of `lockedLevel`.
// If `kn` is nil, `lockedLevel` is nil also.
func (ts *TreeStore) getKeyNodeForWrite(sk StoreKey) (kn *keyNode, lockedLevel *keyTree) {
	for {
		ts.keyMu.RLock()

		address := ts.keys[sk.path]
		if address != 0 {
			//
			// This code, while holding a read lock on the index, wants to acquire a key node write
			// lock. A deadlock could occur with code that is trying to acquire a write lock on the
			// index while already holding a write lock on the key node.
			//
			// To avoid the deadlock, this code starts over upon a lock collision.
			//
			kn = ts.addresses[address]
			if kn.expiration > 0 && kn.expiration < time.Now().UTC().UnixNano() {
				// not found because it is expired
				kn = nil
				ts.keyMu.RUnlock()
				return
			}

			lockedLevel = kn.ownerTree
			obtained := lockedLevel.lock.TryLock()
			ts.keyMu.RUnlock()

			if obtained {
				ts.activeLocks.Add(1)
				return
			}

			lockedLevel = nil
			kn = nil
		} else {
			// not found
			ts.keyMu.RUnlock()
			return
		}
	}
}

// Walks the levels of the tree store, locating the specified key, or the last parent.
//
// The caller always receives ownership of a read lock in loc.level.
//
// loc.kn will provide the located key node at the specified sk, or nil if it doesn't exist.
//
// loc.index will provide the token index where the search ended and will be equal
// to len(sk.tokens) upon a full match.
//
// A store key with zero-length token array will obtain a read lock on the root level
// which is useful with iteration.
func (ts *TreeStore) locateKeyNodeForRead(sk StoreKey) (loc keyLocation) {
	// lock the root sentinel
	kn := &ts.dbNode
	lockedLevel := kn.ownerTree
	lockedLevel.lock.RLock()
	ts.activeLocks.Add(1)
	nextLevel := kn.nextLevel

	tokens := sk.tokens
	end := len(tokens)
	var tokenIndex int
	for tokenIndex = 0; tokenIndex < end; tokenIndex++ {
		if nextLevel == nil {
			break
		}
		nextLevel.lock.RLock()
		lockedLevel.lock.RUnlock()
		lockedLevel = nextLevel

		avlNode := lockedLevel.tree.Find(tokens[tokenIndex])
		if avlNode == nil {
			kn = nil
			break
		}

		kn = avlNode.value
		nextLevel = kn.nextLevel
	}

	loc.level = lockedLevel
	loc.kn = kn
	loc.index = tokenIndex
	return
}

// Walks the levels of the tree store, locating the specified key, or the last parent.
//
// The caller always receives ownership of a read/write lock in loc.level.
//
// loc.kn will provide the located key node at the specified sk, or nil if it doesn't exist.
//
// loc.index will provide the token index where the search ended and will be equal
// to len(sk.tokens) upon a full match.
//
// A store key with zero-length token array will obtain a read lock on the root level
// which is useful in preventing new requests from obtaining a lock. The caller can then
// wait for the number of active operations to go to zero to know it has exclusive
// ownership of the whole database.
func (ts *TreeStore) locateKeyNodeForWrite(sk StoreKey) (loc keyLocation) {
	for {
		loc = ts.locateKeyNodeForRead(sk)

		level := loc.level
		kn := loc.kn
		index := loc.index

		// switch the R lock to a W lock
		level.lock.RUnlock()

		// account for the small chance a key delete will come after release of R lock and
		// before acquire of W lock - just retry upon such a collision
		//
		// note that upon a delete of the level's last token, level becomes an orphan
		if lockPromotion != nil {
			lockPromotion() // testing hook
		}

		level.lock.Lock()

		// index-1 is the deepest level that has an existing token segment match
		if index > 0 {
			existingNode := (level.tree.Find(sk.tokens[index-1]) != nil)
			if existingNode != (kn != nil) {
				level.lock.Unlock()
				ts.activeLocks.Add(-1)
				continue // retry
			}
		}

		return
	}
}

// Locates the store key by walking each level, locking each node along the way.
// The caller must ensure len(sk.tokens) > 0.
func (ts *TreeStore) locateKeyNodeForDelete(sk StoreKey) (lockPath *keyLockPath) {
	lockedLevels := make([]*keyTree, 0, len(sk.tokens)+1)

	// lock the root sentinel
	kn := &ts.dbNode
	lockedLevel := kn.ownerTree
	lockedLevel.lock.Lock()
	ts.activeLocks.Add(1)
	lockedLevels = append(lockedLevels, lockedLevel)
	nextLevel := kn.nextLevel

	tokens := sk.tokens
	end := len(tokens)
	var tokenIndex int
	for tokenIndex = 0; tokenIndex < end; tokenIndex++ {
		if nextLevel == nil {
			break
		}
		nextLevel.lock.Lock()
		lockedLevel = nextLevel
		lockedLevels = append(lockedLevels, lockedLevel)

		avlNode := lockedLevel.tree.Find(tokens[tokenIndex])
		if avlNode == nil {
			kn = nil
			break
		}

		kn = avlNode.value
		nextLevel = kn.nextLevel
	}

	lockPath = &keyLockPath{
		levels: lockedLevels,
		kn:     kn,
	}

	if tokenIndex >= end {
		lockPath.lastLevel = lockedLevels[tokenIndex]
	}
	return
}

// Unlocks the store path locked by lockKeyNodeForDelete
func (ts *TreeStore) unlockKeyPath(lockPath *keyLockPath) {
	for _, level := range lockPath.levels {
		level.lock.Unlock()
	}
	ts.activeLocks.Add(-1)
}

// Unlocks a valueInstance key node accessed for read
func (ts *TreeStore) completeKeyNodeRead(level *keyTree) {
	level.lock.RUnlock()
	ts.activeLocks.Add(-1)
}

// Unlocks a valueInstance key node accessed for write
func (ts *TreeStore) completeKeyNodeWrite(level *keyTree) {
	level.lock.Unlock()
	ts.activeLocks.Add(-1)
}

func (ts *TreeStore) appendKeyNode(lockedLevel *keyTree, token TokenSegment) (kn *keyNode) {
	kn = &keyNode{
		address:   StoreAddress(atomic.AddUint64((*uint64)(&ts.nextAddress), 1)),
		ownerTree: lockedLevel,
	}

	lockedLevel.tree.Set(token, kn)

	ts.keyMu.Lock()
	ts.addresses[kn.address] = kn
	ts.keyMu.Unlock()
	return
}

func (ts *TreeStore) createRestOfKey(sk StoreKey, loc keyLocation) (kn *keyNode, lockedLevel *keyTree) {
	// currently holding the write lock on the level to append, and caller
	// ensures at least one more key token needs to be added to the store

	index := loc.index
	lockedLevel = loc.level
	kn = loc.kn

	// add middle levels if necessary
	for end := len(sk.tokens); index < end; index++ {
		if kn != nil {
			kn.nextLevel = newKeyTree(kn)
			kn.nextLevel.lock.Lock()
			lockedLevel.lock.Unlock()
			lockedLevel = kn.nextLevel
		}

		kn = ts.appendKeyNode(lockedLevel, sk.tokens[index])
	}

	return
}

func (ts *TreeStore) addKeyToIndex(sk StoreKey, kn *keyNode) {
	ts.keyMu.Lock()
	ts.keys[sk.path] = kn.address
	ts.keyMu.Unlock()
}

func (ts *TreeStore) removeKeyFromIndex(sk StoreKey) (removed bool) {
	ts.keyMu.Lock()
	_, removed = ts.keys[sk.path]
	if removed {
		delete(ts.keys, sk.path)
	}
	ts.keyMu.Unlock()
	return
}

func (ts *TreeStore) removeAddress(kn *keyNode) {
	ts.keyMu.Lock()
	delete(ts.addresses, kn.address)
	ts.keyMu.Unlock()
}

func (ts *TreeStore) addKeyToIndexIfNeeded(sk StoreKey, kn *keyNode) (exists bool) {
	ts.keyMu.Lock()
	_, exists = ts.keys[sk.path]
	if !exists {
		ts.keys[sk.path] = kn.address
		ts.addresses[kn.address] = kn
	}
	ts.keyMu.Unlock()
	return
}

func (ts *TreeStore) isKeyIndexed(sk StoreKey) (exists bool) {
	ts.keyMu.Lock()
	_, exists = ts.keys[sk.path]
	ts.keyMu.Unlock()
	return
}

// Worker to make sure a key exists, and returns the valueInstance key node and a write lock on
// the last level; the caller must release lockedLevel.lock.
func (ts *TreeStore) ensureKey(sk StoreKey) (kn *keyNode, lockedLevel *keyTree, created bool) {
	loc := ts.locateKeyNodeForWrite(sk)
	if loc.index < len(sk.tokens) {
		kn, lockedLevel = ts.createRestOfKey(sk, loc)
		created = true
	} else {
		kn = loc.kn
		lockedLevel = loc.level
	}

	return
}

// Worker to make sure an indexed key exists, and returns the valueInstance key node and a write
// lock on the last level; the caller must release lockedLevel.lock.
func (ts *TreeStore) ensureKeyWithValue(sk StoreKey) (kn *keyNode, lockedLevel *keyTree, created bool) {
	loc := ts.locateKeyNodeForWrite(sk)
	if loc.index < len(sk.tokens) {
		kn, lockedLevel = ts.createRestOfKey(sk, loc)
		created = true
		ts.addKeyToIndex(sk, kn)
	} else {
		kn = loc.kn
		lockedLevel = loc.level
		created = !ts.addKeyToIndexIfNeeded(sk, kn)
	}

	return
}

// Set a key without a value and without an expiration, doing nothing if the
// key already exists. The key index is not altered.
func (ts *TreeStore) SetKey(sk StoreKey) (address StoreAddress, exists bool) {
	kn, ll, created := ts.ensureKey(sk)
	defer ts.completeKeyNodeWrite(ll)

	address = kn.address
	exists = !created
	return
}

// Set a key with a value, without an expiration, adding to value history if the
// key already exists.
func (ts *TreeStore) SetKeyValue(sk StoreKey, value any) (address StoreAddress, firstValue bool) {
	newLeaf := &valueInstance{
		value: value,
	}

	now := currentTickBytes()

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
	loc := ts.locateKeyNodeForWrite(sk)

	var kn *keyNode
	var ll *keyTree
	if loc.index >= len(sk.tokens) {
		exists = true

		if loc.kn.current != nil {
			originalValue = loc.kn.current.value
		}

		kn = loc.kn
		ll = loc.level
		if (flags & SetExMustNotExist) != 0 {
			ts.completeKeyNodeWrite(ll)
			return
		}
	} else {
		if (flags & SetExMustExist) != 0 {
			ts.completeKeyNodeWrite(loc.level)
			return
		}

		kn, ll = ts.createRestOfKey(sk, loc)
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

		now := currentTickBytes()
		if kn.history == nil {
			kn.history = newAvlTree[*valueInstance]()
		}

		kn.current = newLeaf
		kn.history.Set(now, newLeaf)

		ts.addKeyToIndex(sk, kn)
	}

	if expireNs != -1 {
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
	loc := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(loc.level)
	exists = (loc.index >= len(sk.tokens))
	if exists {
		if loc.kn.isExpired() {
			exists = false
		} else {
			address = loc.kn.address
		}
	}
	return
}

// Navigates to the valueInstance key node and returns the expiration time in Unix nanoseconds, or
// -1 if the key path does not exist.
func (ts *TreeStore) GetKeyTtl(sk StoreKey) (ttl int64) {
	if len(sk.tokens) == 0 {
		return
	}

	loc := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(loc.level)
	if loc.index >= len(sk.tokens) {
		ttl = loc.kn.expiration
	} else {
		ttl = -1
	}
	return
}

// Navigates to the valueInstance key node and sets the expiration time in Unix nanoseconds.
// Specify 0 for no expiration.
func (ts *TreeStore) SetKeyTtl(sk StoreKey, expiration int64) (exists bool) {

	if len(sk.tokens) == 0 {
		exists = true
		return
	}

	loc := ts.locateKeyNodeForWrite(sk)
	defer ts.completeKeyNodeWrite(loc.level)

	if loc.index >= len(sk.tokens) {
		if expiration >= 0 {
			loc.kn.expiration = expiration
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
			tickNs = time.Now().UTC().UnixNano() + tickNs
		}
		if kn.history != nil && tickNs >= 0 {
			item := kn.history.FindLeft(tickBytes(tickNs))
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
	if len(sk.tokens) == 0 {
		ts.dbNode.ownerTree.lock.Lock()
		ts.activeLocks.Add(1)
		if ts.dbNode.current != nil {
			originalValue = ts.dbNode.current.value
			removed = true
			ts.dbNode.current = nil
			ts.dbNode.history = nil
		}
		ts.dbNode.metadata = nil
		ts.removeKeyFromIndex(sk)
		ts.dbNode.ownerTree.lock.Unlock()
		ts.activeLocks.Add(-1)
		return
	}

	klp := ts.locateKeyNodeForDelete(sk)
	defer ts.unlockKeyPath(klp)

	level := klp.lastLevel
	if level == nil {
		return
	}

	tokenIndex := len(sk.tokens) - 1
	kn := klp.kn

	if kn != nil {
		if ts.removeKeyFromIndex(sk) {
			if kn.current != nil {
				originalValue = kn.current.value
				kn.current = nil
			}
			kn.history = nil
			kn.metadata = nil

			if kn.nextLevel == nil {
				for {
					// permanently delete the node
					ts.removeAddress(kn)
					level.tree.Delete(sk.tokens[tokenIndex])
					kn.ownerTree = nil

					// stop if this level contains siblings
					if level.tree.nodes > 0 {
						break
					}

					// move to the parent and clear linkage to deleted level
					sk = StoreKey{
						tokens: sk.tokens[0:tokenIndex],
					}
					sk.path = TokenSetToTokenPath(sk.tokens)
					tokenIndex--

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
					if ts.isKeyIndexed(sk) {
						break
					}

					// continue with the parent level
					level = kn.ownerTree
				}
			}

			removed = true
		}
	}

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
	if len(sk.tokens) == 0 {
		valueRemoved, originalValue = ts.DeleteKeyWithValue(sk, true)
		return
	}

	klp := ts.locateKeyNodeForDelete(sk)
	defer ts.unlockKeyPath(klp)

	level := klp.lastLevel
	if level == nil {
		return
	}

	tokenIndex := len(sk.tokens) - 1
	kn := klp.kn

	if kn != nil {
		if ts.removeKeyFromIndex(sk) {
			valueRemoved = true
			if kn.current != nil {
				originalValue = kn.current.value
				kn.current = nil
			}
		}
		kn.history = nil
		kn.metadata = nil

		if kn.nextLevel == nil {
			// permanently delete the node
			ts.removeAddress(kn)
			level.tree.Delete(sk.tokens[tokenIndex])
			kn.ownerTree = nil

			// if the level is empty now, unlink the parent
			if level.tree.nodes == 0 {
				parent := level.parent
				if parent != nil {
					parent.nextLevel = nil
					level.parent = nil
				}
			}

			keyRemoved = true
		}
	}

	return
}

// Sets a metadata attribute on a key, returning the original value (if any)
func (ts *TreeStore) SetMetdataAttribute(sk StoreKey, attribute, value string) (keyExists bool, originalValue string) {
	loc := ts.locateKeyNodeForWrite(sk)
	defer ts.completeKeyNodeWrite(loc.level)

	kn := loc.kn
	if loc.index < len(sk.tokens) {
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
func (ts *TreeStore) ClearMetdataAttribute(sk StoreKey, attribute string) (attributeExists bool, originalValue string) {
	loc := ts.locateKeyNodeForWrite(sk)
	defer ts.completeKeyNodeWrite(loc.level)

	kn := loc.kn
	if loc.index < len(sk.tokens) {
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

// Discards all metdata on the specific key
func (ts *TreeStore) ClearKeyMetdata(sk StoreKey) {
	loc := ts.locateKeyNodeForWrite(sk)
	defer ts.completeKeyNodeWrite(loc.level)

	kn := loc.kn
	if loc.index < len(sk.tokens) {
		return
	}

	kn.metadata = nil
}

// Fetches a key's metadata value for a specific attribute
func (ts *TreeStore) GetMetadataAttribute(sk StoreKey, attribute string) (attributeExists bool, value string) {
	loc := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(loc.level)

	kn := loc.kn
	if loc.index < len(sk.tokens) {
		return
	}

	if kn.metadata != nil {
		value, attributeExists = kn.metadata[attribute]
	}

	return
}

// Returns an array of attribute names of metadata stored for the specified key
func (ts *TreeStore) GetMetadataAttributes(sk StoreKey) (attributes []string) {
	loc := ts.locateKeyNodeForRead(sk)
	defer ts.completeKeyNodeRead(loc.level)

	kn := loc.kn
	if loc.index < len(sk.tokens) {
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
