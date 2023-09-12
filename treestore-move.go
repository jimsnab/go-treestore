package treestore

// Moves a key tree to a new location, optionally overwriting an existing tree.
func (ts *TreeStore) MoveKey(srcSk, destSk StoreKey, overwrite bool) (exists, moved bool) {
	// the entire database is locked for implementation simplicity
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	slevel, tokenIndex, skn, expired := ts.locateKeyNodeForLock(srcSk)
	if tokenIndex < len(srcSk.Tokens) || expired {
		return
	}
	exists = true

	_, tokenIndex, _, expired = ts.locateKeyNodeForLock(destSk)
	if tokenIndex >= len(destSk.Tokens) {
		if !expired && !overwrite {
			// destination exists and not overwriting
			return
		}

		if len(destSk.Tokens) > 0 {
			ts.deleteKeyTreeLocked(destSk)
		}
	}

	ts.unindexNodesLocked(srcSk, skn)
	if len(srcSk.Tokens) > 0 {
		ts.deleteKeyNodeLocked(slevel, skn)
	}

	dkn, ll, _ := ts.ensureKey(destSk)
	defer ts.completeKeyNodeWrite(ll)

	if len(srcSk.Tokens) > 0 {
		// src not the sentinel - move child keys
		dkn.nextLevel = skn.nextLevel
	}
	if len(destSk.Tokens) > 0 {
		// dest not the sentinel - move expiration
		dkn.expiration = skn.expiration
	}
	dkn.current = skn.current
	dkn.history = skn.history
	dkn.metadata = skn.metadata

	skn.current = nil
	skn.expiration = 0
	skn.history = nil
	skn.metadata = nil

	if dkn.nextLevel != nil {
		dkn.nextLevel.parent = dkn
	}

	ts.indexMovedNodesLocked(destSk, dkn, skn.address, dkn.address)

	moved = true
	return
}

// worker that removes the indexed key paths from a tree, but does not
// unlink the key nodes
func (ts *TreeStore) unindexNodesLocked(sk StoreKey, kn *keyNode) {
	if kn.current != nil {
		delete(ts.keys, sk.Path)
	}
	if kn.nextLevel != nil {
		kn.nextLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			childSk := AppendStoreKeySegments(sk, node.key)
			ts.unindexNodesLocked(childSk, node.value)
			return true
		})
	}
}

// worker that adds all key nodes having values to the key index
func (ts *TreeStore) indexMovedNodesLocked(sk StoreKey, kn *keyNode, oldAddress, newAddress StoreAddress) {
	if kn.current != nil {
		ts.keys[sk.Path] = kn.address

		if kn.current.relationships != nil {
			for i, v := range kn.current.relationships {
				if v == oldAddress {
					kn.current.relationships[i] = newAddress
				}
			}
		}
	}
	if kn.nextLevel != nil {
		kn.nextLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			childSk := AppendStoreKeySegments(sk, node.key)
			ts.indexMovedNodesLocked(childSk, node.value, oldAddress, newAddress)
			return true
		})
	}
}
