package treestore

import (
	"bytes"
	"strings"
)

type (
	LevelKey struct {
		Segment     TokenSegment `json:"segment"`
		HasValue    bool         `json:"has_value"`
		HasChildren bool         `json:"has_children"`
	}

	KeyMatch struct {
		Key           TokenPath         `json:"key"`
		Metadata      map[string]string `json:"metadata,omitempty"`
		HasValue      bool              `json:"has_value"`
		HasChildren   bool              `json:"has_children"`
		CurrentValue  any               `json:"current_value,omitempty"`
		Relationships []StoreAddress    `json:"relationships,omitempty"`
	}

	KeyValueMatch struct {
		Key           TokenPath         `json:"key"`
		Metadata      map[string]string `json:"metadata,omitempty"`
		HasChildren   bool              `json:"has_children"`
		CurrentValue  any               `json:"current_value,omitempty"`
		Relationships []StoreAddress    `json:"relationships,omitempty"`
	}

	iterateFullCallback func(km *KeyMatch) bool
)

// Navigates to the specified store key and returns all of the key segments
// matching the simple wildcard `pattern`. If the store key does not exist,
// the return `keys` will be nil.
//
// Memory is allocated up front to hold `limit` keys, so be careful to pass
// a reasonable limit.
func (ts *TreeStore) GetLevelKeys(sk StoreKey, pattern string, startAt, limit int) (keys []LevelKey) {
	var level *keyTree
	var index int
	var kn *keyNode
	var expired bool

	end := len(sk.Tokens)

	if end == 0 {
		kn = &ts.dbNode
		level = ts.dbNode.ownerTree
		level.lock.RLock()
		ts.activeLocks.Add(1)
		expired = false
	} else {
		level, index, kn, expired = ts.locateKeyNodeForRead(sk)
	}

	lockedLevel := level

	if index < end || expired {
		ts.completeKeyNodeRead(lockedLevel)
		return
	}

	keys = make([]LevelKey, 0, limit)

	nextLockedLevel := kn.nextLevel
	if nextLockedLevel == nil {
		// no children
		ts.completeKeyNodeRead(lockedLevel)
		return
	}

	nextLockedLevel.lock.RLock()
	lockedLevel.lock.RUnlock()
	lockedLevel = nextLockedLevel

	if limit > 0 {
		n := 0
		patternRunes := []rune(pattern)
		lockedLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			kn := node.value
			if kn.isExpired() {
				return true
			}

			if isPatternRunes(patternRunes, bytes.Runes(node.key)) {
				if n >= startAt {
					lk := LevelKey{
						Segment:     node.key,
						HasValue:    kn.current != nil,
						HasChildren: kn.nextLevel != nil,
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

// worker that calls the full iterator callback
func (ts *TreeStore) iterateFullInvokeCallback(segments []TokenSegment, kn *keyNode, callback iterateFullCallback) (stopped bool) {
	if kn.isExpired() {
		return
	}

	km := KeyMatch{
		Key:         TokenSetToTokenPath(segments),
		HasValue:    kn.current != nil,
		HasChildren: kn.nextLevel != nil,
	}
	if kn.metadata != nil {
		km.Metadata = kn.metadata
	}
	if kn.current != nil {
		km.CurrentValue = kn.current.value
		km.Relationships = kn.current.relationships
	}

	stopped = !callback(&km)
	return
}

// worker that tests for a multi-level pattern match
func (ts *TreeStore) iterateFullWorkerIsMatch(patternSegs []TokenSegment, candidate []TokenSegment) bool {
	cpos := 0
	ppos := 0

	patStr := string(patternSegs[ppos])

	for {
		if ppos+2 <= len(patternSegs) && patStr == "**" && string(patternSegs[ppos+1]) == "**" {
			ppos++
			patStr = string(patternSegs[ppos])
		} else {
			break
		}
	}

	for {
		if ppos >= len(patternSegs) {
			break
		}
		if cpos >= len(candidate) {
			break
		}

		patStr = string(patternSegs[ppos])
		if patStr == "**" {
			if ppos+1 >= len(patternSegs) {
				return true
			}
			for {
				if ts.iterateFullWorkerIsMatch(patternSegs[ppos+1:], candidate[cpos:]) {
					return true
				}
				cpos++
				if cpos >= len(candidate) {
					return false
				}
			}
		} else if !isPattern(string(patternSegs[ppos]), string(candidate[cpos])) {
			return false
		}

		ppos++
		cpos++
	}

	if ppos == len(patternSegs)-1 && patStr == "**" {
		return true
	}

	return (ppos == len(patternSegs) && cpos == len(candidate))
}

// worker that iterates through the tree store, calling the callback for each key
// that matches the pattern segment(s)
func (ts *TreeStore) iterateFullWorker(patternSegs []TokenSegment, patternIndex int, segments []TokenSegment, nextLevel *keyTree, callback iterateFullCallback) (stopped bool) {

	var lockedLevel *keyTree
	if nextLevel == nil {
		return
	}

	lockedLevel = nextLevel
	lockedLevel.lock.RLock()
	ts.activeLocks.Add(1)

	for {
		seg := patternSegs[patternIndex]
		segstr := string(seg)
		if !strings.Contains(segstr, "*") {
			// no wildcard
			avlNode := lockedLevel.tree.Find(seg)
			if avlNode == nil {
				break
			}

			segments = append(segments, seg)
			kn := avlNode.value

			patternIndex++
			if patternIndex >= len(patternSegs) {
				// valueInstance match
				stopped = ts.iterateFullInvokeCallback(segments, kn, callback)
				break
			}

			// test next level
			nextLevel = kn.nextLevel
			if nextLevel == nil {
				break
			}
			nextLevel.lock.RLock()
			lockedLevel.lock.RUnlock()
			lockedLevel = nextLevel
		} else if segstr == "**" {
			// multi-level pattern iteration
			all := lockedLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
				subSegments := append(segments, node.key)
				kn := node.value

				if ts.iterateFullWorkerIsMatch(patternSegs, subSegments) {
					if ts.iterateFullInvokeCallback(subSegments, kn, callback) {
						return false
					}
				}

				// N.B., the patternIndex is not advanced - which causes the entire subtree to be examined.
				// This could be optimized.
				if ts.iterateFullWorker(patternSegs, patternIndex, subSegments, kn.nextLevel, callback) {
					return false
				}

				return true
			})
			stopped = !all
			break
		} else {
			// single-level pattern iteration
			nextPatternIndex := patternIndex + 1
			end := nextPatternIndex >= len(patternSegs)

			all := lockedLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
				subSegments := append(segments, node.key)
				kn := node.value

				if ts.iterateFullWorkerIsMatch(patternSegs, subSegments) {
					if ts.iterateFullInvokeCallback(subSegments, kn, callback) {
						return false
					}
				}

				if !end {
					if ts.iterateFullWorker(patternSegs, nextPatternIndex, subSegments, kn.nextLevel, callback) {
						return false
					}
				}
				return true
			})

			stopped = !all
			break
		}
	}

	lockedLevel.lock.RUnlock()
	ts.activeLocks.Add(-1)
	return
}

func (ts *TreeStore) iterateFull(skPattern StoreKey, callback iterateFullCallback) {
	segments := make([]TokenSegment, 0, len(skPattern.Tokens))
	nextLevel := ts.dbNode.nextLevel

	ts.iterateFullWorker(skPattern.Tokens, 0, segments, nextLevel, callback)
}

// Full iteration function walks each tree store level according to skPattern and returns every
// detail of matching keys.
func (ts *TreeStore) GetMatchingKeys(skPattern StoreKey, startAt, limit int) (keys []*KeyMatch) {
	keys = []*KeyMatch{}

	if limit == 0 {
		return
	}

	if len(skPattern.Tokens) == 0 {
		// sentinel special case
		ts.dbNode.ownerTree.lock.RLock()
		ts.activeLocks.Add(1)
		defer func() {
			ts.dbNode.ownerTree.lock.RUnlock()
			ts.activeLocks.Add(-1)
		}()

		ts.iterateFullInvokeCallback(skPattern.Tokens, &ts.dbNode, func(km *KeyMatch) bool {
			keys = append(keys, km)
			return true
		})

		return
	}

	n := 0
	ts.iterateFull(skPattern, func(km *KeyMatch) bool {
		if n >= startAt {
			keys = append(keys, km)
			if len(keys) >= limit {
				return false
			}
		}
		n++
		return true
	})

	return
}

// Full iteration function walks each tree store level according to skPattern and returns every
// detail of matching keys that have values.
func (ts *TreeStore) GetMatchingKeyValues(skPattern StoreKey, startAt, limit int) (values []*KeyValueMatch) {
	values = []*KeyValueMatch{}

	if limit == 0 {
		return
	}

	if len(skPattern.Tokens) == 0 {
		// sentinel special case
		ts.dbNode.ownerTree.lock.RLock()
		ts.activeLocks.Add(1)
		defer func() {
			ts.dbNode.ownerTree.lock.RUnlock()
			ts.activeLocks.Add(-1)
		}()

		ts.iterateFullInvokeCallback(skPattern.Tokens, &ts.dbNode, func(km *KeyMatch) bool {
			if km.HasValue {
				kvm := &KeyValueMatch{
					Key:           km.Key,
					Metadata:      km.Metadata,
					HasChildren:   km.HasChildren,
					CurrentValue:  km.CurrentValue,
					Relationships: km.Relationships,
				}
				values = append(values, kvm)
			}
			return true
		})

		return
	}

	n := 0
	ts.iterateFull(skPattern, func(km *KeyMatch) bool {
		if km.HasValue {
			if n >= startAt {
				kvm := &KeyValueMatch{
					Key:           km.Key,
					Metadata:      km.Metadata,
					HasChildren:   km.HasChildren,
					CurrentValue:  km.CurrentValue,
					Relationships: km.Relationships,
				}
				values = append(values, kvm)
				if len(values) >= limit {
					return false
				}
			}
			n++
		}
		return true
	})

	return
}
