package treestore

// Deletes a key and all of its child data.
//
// All key nodes along the store key path will be locked during the operation, so
// this operation blocks subsequent operations until it completes.
//
// The sentinal (root) key node cannot be deleted; only its value can be cleared.
func (ts *TreeStore) DeleteKeyTree(sk StoreKey) (removed bool) {
	// likely to modify the linkage of keynodes
	ts.keyNodeMu.Lock()
	defer ts.keyNodeMu.Unlock()

	return ts.deleteKeyTreeLocked(sk)
}

func (ts *TreeStore) deleteKeyTreeLocked(sk StoreKey) (removed bool) {
	end := len(sk.Tokens)
	if end == 0 {
		removed, _ = ts.deleteKeyWithValueLocked(sk, true)
		return
	}

	level, index, kn, expired := ts.locateKeyNodeForLock(sk)
	if index < end {
		return
	}

	if !expired {
		removed = true
	}

	ts.discardChildren(sk, kn)

	if ts.removeKeyFromIndexLocked(sk) || expired {
		kn.current = nil
	}
	kn.history = nil
	kn.metadata = nil

	ts.removeFromIndicies(sk.Tokens, kn)
	if kn.nextLevel == nil {
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

	return
}
