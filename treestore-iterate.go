package treestore

import (
	"bytes"
	"sync/atomic"
)

type (
	LevelKey struct {
		segment TokenSegment
		hasValue bool
		hasChildren bool

	}
)

// Navigates to the specified store key and returns all of the key segments
// matching the simple wildcard `pattern`. If the store key does not exist,
// the return `keys` will be nil.
//
// Memory is allocated up front to hold `limit` keys, so be careful to pass
// a reasonable limit.
func (ts *TreeStore) GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey, count int) {
	var loc keyLocation
	if len(sk.tokens) == 0 {
		loc.kn = ts.dbNode
		loc.level = ts.dbNode.ownerTree
		loc.level.lock.RLock()
		atomic.AddInt32(&ts.activeLocks, 1)
	} else {
		loc = ts.locateKeyNodeForRead(sk)
		if loc.level == nil {
			return
		}
	}

	lockedLevel := loc.level	

	if loc.index < len(sk.tokens) {
		ts.completeKeyNodeRead(lockedLevel)
		return
	}

	keys = make([]LevelKey, 0, limit)

	nextLockedLevel := loc.kn.nextLevel
	if nextLockedLevel == nil {
		// no children
		ts.completeKeyNodeRead(lockedLevel)
		return
	}

	nextLockedLevel.lock.RLock()
	lockedLevel.lock.RUnlock()
	lockedLevel = nextLockedLevel

	count = lockedLevel.tree.nodes

	if limit > 0 {
		n := 0
		patternRunes := []rune(pattern)
		lockedLevel.tree.Iterate(func(node *AvlNode) bool {
			if isPatternRunes(patternRunes, bytes.Runes(node.key)) {
				if n >= startAt {
					kn := node.value.(*keyNode)
					lk := LevelKey{
						segment: node.key,
						hasValue: kn.current != nil,
						hasChildren: kn.nextLevel != nil,
					}
					keys = append(keys, lk)
					if len(keys) >= limit {
						return false
					}
				}
				n++
			}
			return true
		})
	}

	ts.completeKeyNodeRead(lockedLevel)
	return
}