package treestore

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type (
	TreeStore struct {
		root        *keyTree
		nextAddress StoreAddress
		addresses   map[StoreAddress]*keyNode
		keys        map[TokenPath]StoreAddress
		keyMu       sync.RWMutex
		cas         map[StoreAddress]uint64
	}

	StoreAddress uint64

	keyTree struct {
		lock   sync.RWMutex
		tree   *AvlTree
		parent *keyNode
	}

	keyNode struct {
		address    StoreAddress
		ownerTree  *keyTree
		nextLevel  *keyTree
		current    *leaf
		history    *AvlTree
		expiration int64
		metadata   *keyMetadata
	}

	keyMetadata struct {
		metadata map[string]string
	}

	leaf struct {
		value         any
		relationships []StoreAddress
	}

	keyLocation struct {
		level *keyTree
		index int
		kn    *keyNode
	}

	SetExFlags int

	testHookLockPromotion func()
)

var (
	ErrObsolete = errors.New("computation is based on outdated data")
)

var (
	lockPromotion testHookLockPromotion
)

const (
	SetExMustExist SetExFlags = 1 << iota
	SetExMustNotExist
	SetExNoValueUpdate
)

func errObsolete() error { return ErrObsolete }

func NewTreeStore() *TreeStore {
	return &TreeStore{
		root:        newKeyTree(nil),
		nextAddress: 1,
		addresses:   map[StoreAddress]*keyNode{},
		keys:        map[TokenPath]StoreAddress{},
		cas:         map[StoreAddress]uint64{},
	}
}

func newKeyTree(parent *keyNode) *keyTree {
	return &keyTree{
		tree:   NewAvlTree(),
		parent: parent,
	}
}

// Looks up the store key using the full key path, returning the key node if it exists, holding
// a value read lock.
func (ts *TreeStore) getKeyNodeForValueRead(sk *StoreKey) (kn *keyNode, lockedLevel *keyTree) {
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
				ts.keyMu.RUnlock()
				return
			}

			lockedLevel = kn.ownerTree
			obtained := lockedLevel.lock.TryRLock()
			ts.keyMu.RUnlock()

			if obtained {
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

// Looks up the store key using the full key path, returning the key node if it exists, holding
// a write lock.
func (ts *TreeStore) getKeyNodeForWrite(sk *StoreKey) (kn *keyNode, lockedLevel *keyTree) {
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
				ts.keyMu.RUnlock()
				return
			}

			lockedLevel = kn.ownerTree
			obtained := lockedLevel.lock.TryLock()
			ts.keyMu.RUnlock()

			if obtained {
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
func (ts *TreeStore) locateKeyNodeForRead(sk *StoreKey) (loc keyLocation) {
	var level *keyTree
	var tokenIndex int
	var kn *keyNode

	tokens := sk.tokens
	end := len(tokens)
	if end > 0 {
		level = ts.root
		level.lock.RLock()
		avlNode := level.tree.Find(tokens[0])
		if avlNode != nil {
			kn = avlNode.value.(*keyNode)
			tokenIndex = 1

			for tokenIndex < end {
				nextLevel := kn.nextLevel
				if nextLevel == nil {
					break
				}
			
				level.lock.RUnlock()
				level = nextLevel
				level.lock.RLock()

				avlNode = level.tree.Find(tokens[tokenIndex])
				if avlNode == nil {
					break
				}
				kn = avlNode.value.(*keyNode)
				tokenIndex++
			}
		}
	}

	loc.level = level
	loc.kn = kn
	loc.index = tokenIndex
	return
}

// Walks the levels of the tree store, locating the specified key, or the last parent.
func (ts *TreeStore) locateKeyNodeForWrite(sk *StoreKey) (loc keyLocation) {
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
			lockPromotion()	// testing hook
		}

		level.lock.Lock()

		// index-1 is the deepest level that has an existing token segment match
		if index > 0 {
			existingNode := (level.tree.Find(sk.tokens[index - 1]) != nil)
			if existingNode != (kn != nil) {
				level.lock.Unlock()
				continue // retry
			}
		}

		return
	}
}

func (ts *TreeStore) createRestOfKey(sk *StoreKey, loc keyLocation) (kn *keyNode, lockedLevel *keyTree) {
	// currently holding the write lock on a parent key node, and caller
	// ensures at least one more key token needs to be added to the store

	p := &ts.nextAddress
	end := len(sk.tokens)
	level := loc.level
	kn = loc.kn
	index := loc.index

	// append the rest of the token key nodes
	for {
		if index > 0 {
			level = newKeyTree(kn)
			kn.nextLevel = level
		}

		nextKeyNode := &keyNode{
			address:   StoreAddress(atomic.AddUint64((*uint64)(p), 1)),
			ownerTree: level,
		}

		level.tree.Set(sk.tokens[index], nextKeyNode)
		kn = nextKeyNode

		index++
		if index >= end {
			break
		}
	}

	if level != loc.level {
		level.lock.Lock()       // 'level' is brand new and no one else can lock it yet
		loc.level.lock.Unlock() // release the parent lock
	}

	lockedLevel = level
	return
}

func (ts *TreeStore) addKeyToIndex(sk *StoreKey, kn *keyNode) {
	ts.keyMu.Lock()
	ts.keys[sk.path] = kn.address
	ts.addresses[kn.address] = kn
	ts.keyMu.Unlock()
}

func (ts *TreeStore) removeKeyFromIndex(sk *StoreKey) (removed bool) {
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

func (ts *TreeStore) addKeyToIndexIfNeeded(sk *StoreKey, kn *keyNode) (exists bool) {
	ts.keyMu.Lock()
	_, exists = ts.keys[sk.path]
	if !exists {
		ts.keys[sk.path] = kn.address
		ts.addresses[kn.address] = kn
	}
	ts.keyMu.Unlock()
	return
}

func (ts *TreeStore) isKeyIndexed(sk *StoreKey) (exists bool) {
	ts.keyMu.Lock()
	_, exists = ts.keys[sk.path]
	ts.keyMu.Unlock()
	return
}

// Worker to make sure a key exists, and returns the leaf key node and a write lock on
// the last level; the caller must release lockedLevel.lock.
func (ts *TreeStore) ensureKey(sk *StoreKey) (kn *keyNode, lockedLevel *keyTree, created bool) {
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

// Worker to make sure an indexed key exists, and returns the leaf key node and a write
// lock on the last level; the caller must release lockedLevel.lock.
func (ts *TreeStore) ensureKeyWithValue(sk *StoreKey) (kn *keyNode, lockedLevel *keyTree, created bool) {
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
func (ts *TreeStore) SetKey(sk *StoreKey) (address StoreAddress, exists bool) {
	kn, ll, created := ts.ensureKey(sk)
	ll.lock.Unlock()

	address = kn.address
	exists = !created
	return
}

// Set a key with a value, without an expiration, adding to value history if the
// key already exists.
func (ts *TreeStore) SetKeyValue(sk *StoreKey, value any) (address StoreAddress, firstValue bool) {
	newLeaf := &leaf{
		value: value,
	}

	now := currentTickBytes()

	kn, ll, created := ts.ensureKeyWithValue(sk)
	defer ll.lock.Unlock()

	if kn.history == nil {
		kn.history = NewAvlTree()
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
//  SetExMustNotExist - perform only if the key does not exist
//
// For `expireNs`, specify the Unix nanosecond tick of when the key will expire. Specify zero to
// remove expiration. Specify -1 to retain the current key expiration.
//
// `originalValue` will be provided if the key exists and has a value, even if no change is made.
//
// A non-nil `relationships` will replace the relationships of the key node. An empty array
// removes all relationships. Specify nil to retain the current key relationships.
func (ts *TreeStore) SetKeyValueEx(sk *StoreKey, value any, flags SetExFlags, expireNs int64, relationships []StoreAddress) (address StoreAddress, exists bool, originalValue any) {
	loc := ts.locateKeyNodeForWrite(sk)

	if (flags & SetExMustExist) != 0 {
		if loc.index < len(sk.tokens) {
			loc.level.lock.Unlock()
			return
		}
	} else if (flags & SetExMustNotExist) != 0 {
		if loc.index >= len(sk.tokens) {
			if loc.kn.current != nil {
				originalValue = loc.kn.current.value
			}
			loc.level.lock.Unlock()
			address = loc.kn.address
			return
		}
	}

	var kn *keyNode
	var ll *keyTree
	if loc.index < len(sk.tokens) {
		kn, ll = ts.createRestOfKey(sk, loc)
	} else {
		kn = loc.kn
		ll = loc.level
		exists = true
	}
	defer ll.lock.Unlock()

	if (flags&SetExNoValueUpdate) == 0 || relationships != nil {
		newLeaf := &leaf{
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
			kn.history = NewAvlTree()
		}

		kn.current = newLeaf
		kn.history.Set(now, newLeaf)

		ts.addKeyToIndex(sk, kn)
	}

	if expireNs != -1 {
		kn.expiration = expireNs
	}

	return
}

// Looks up the key in the index and returns true if it exists and has value history.
func (ts *TreeStore) IsKeyIndexed(sk *StoreKey) (address StoreAddress, exists bool) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	exists = (kn != nil)
	if exists {
		address = kn.address
		ll.lock.RUnlock()
	}
	return
}

// Walks the tree level by level and returns the current address, whether or not
// the key path is indexed. This avoids putting a lock on the index, but will lock
// tree levels while walking the tree.
func (ts *TreeStore) LocateKey(sk *StoreKey) (address StoreAddress, exists bool) {
	loc := ts.locateKeyNodeForRead(sk)
	loc.level.lock.RUnlock()
	exists = (loc.kn != nil)
	if exists {
		address = loc.kn.address
	}
	return
}

// Looks up the key in the index and returns the current value and flags
// that indicate if the key was set, and if so, if it has a value.
func (ts *TreeStore) GetKeyValue(sk *StoreKey) (value any, keyExists, valueExists bool) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	defer ll.lock.Unlock()
	if kn != nil {
		keyExists = true
		if kn.current != nil {
			valueExists = true
			value = kn.current.value
		}
	}
	return
}

// Looks up the key and returns the expiration time in Unix nanoseconds, or
// -1 if the key does not exist.
func (ts *TreeStore) GetKeyTtl(sk *StoreKey) (ttl int64) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	defer ll.lock.Unlock()
	if kn != nil {
		ttl = kn.expiration
	} else {
		ttl = -1
	}
	return
}

// Looks up the key in the index and scans history for the specified Unix ns tick,
// returning the value at that moment in time, if one exists.
func (ts *TreeStore) GetKeyValueAtTime(sk *StoreKey, tickNs int64) (value any, exists bool) {
	kn, ll := ts.getKeyNodeForValueRead(sk)
	defer ll.lock.Unlock()
	if kn != nil && kn.history != nil {
		item := kn.history.FindLeft(sk.tokens[len(sk.tokens)-1])
		if item != nil {
			value = item.value
			exists = true
			return
		}
	}
	return
}

// Deletes an indexed key that has a value, including its value history, and its metadata.
// Specify `clean` as `true` to delete parent key nodes that become empty, or `false` to only
// remove the leaf key node.
func (ts *TreeStore) DeleteKeyWithValue(sk *StoreKey, clean bool) (removed bool, originalValue any) {
	if sk == nil || len(sk.tokens) == 0 {
		return
	}

	loc := ts.locateKeyNodeForWrite(sk)	
	level := loc.level
	kn := loc.kn
	tokenIndex := len(sk.tokens) - 1

	if kn != nil {
		if ts.removeKeyFromIndex(sk) {
			if kn.current != nil {
				originalValue = loc.kn.current.value
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
					if level.tree.nodes > 0  {
						break
					}

					// move to the parent and clear linkage to deleted level
					sk = &StoreKey{
						tokens: sk.tokens[0:tokenIndex],
					}
					sk.path = TokenSetToTokenPath(sk.tokens)
					tokenIndex--

					kn = level.parent
					if kn == nil {
						break
					}
					kn.nextLevel = nil

					// stop if not cleaning
					if !clean {
						break
					}

					// stop here if the parent is indexed
					if ts.isKeyIndexed(sk) {
						break
					}

					// lock the parent level
					parentLevel := kn.ownerTree
					parentLevel.lock.Lock()
					level.lock.Unlock()
					level = parentLevel
				}
			}
			
			removed = true
		}
	}

	level.lock.Unlock()
	return
}

// Deletes a key that has a value, including its value history, and its metadata.
// The parent key is not altered. `removed` will be returned `true` only when the
// leaf key node is deleted.
func (ts *TreeStore) DeleteKey(sk *StoreKey) (removed bool, originalValue any) {
	if sk == nil || len(sk.tokens) == 0 {
		return
	}

	loc := ts.locateKeyNodeForWrite(sk)	
	level := loc.level
	kn := loc.kn
	tokenIndex := len(sk.tokens) - 1

	if kn != nil {
		if ts.removeKeyFromIndex(sk) {
			if kn.current != nil {
				originalValue = loc.kn.current.value
			}
		}
		kn.history = nil
		kn.metadata = nil

		if kn.nextLevel == nil {
			// permanently delete the node
			ts.removeAddress(kn)
			level.tree.Delete(sk.tokens[tokenIndex])
			kn.ownerTree = nil

			// if the level is empty, unlink the parent
			if level.tree.nodes == 0 {
				parent := level.parent
				if parent != nil {
					parentLevel := parent.ownerTree
					parentLevel.lock.Lock()
					level.lock.Unlock()
					level = parentLevel

					parent.nextLevel = nil
				}
			}
			
			removed = true
		}
	}

	level.lock.Unlock()
	return
}

