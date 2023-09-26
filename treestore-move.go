package treestore

// Moves a key tree to a new location, optionally overwriting an existing tree.
func (ts *TreeStore) MoveKey(srcSk, destSk StoreKey, overwrite bool) (exists, moved bool) {
	return ts.MoveReferencedKey(srcSk, destSk, overwrite, -1, []StoreKey{}, []StoreKey{})
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

// This API is intended for an indexing scenario, where:
//
//   - A "source key" is staged with a temporary path, and with a short expiration
//   - The children of the source key are filled, usually with multiple steps
//   - When the source key is ready, it is moved to a "destination key" (its
//     permanent path), and the expiration is removed or set to a longer expiration.
//   - At the time of moving source to destination, separate "index keys" are
//     maintained atomically with a reference to the destination key.
//
// If the reference keys do not exist, they are created, and the destination
// address is placed in relationship index 0.
//
// If a ttl change is specified, it is applied to the destination key and the
// reference keys as well.
//
// If ttl == 0, expiration is cleared. If ttl > 0, it is the Unix nanosecond
// tick of key expiration. Specify -1 for ttl to retain the source key's expiration.
//
// N.B., the address of a child source node does not change when the parent
// key is moved. Also expiration is not altered for child keys.
//
// The caller can specify keys to unreference upon the move. This supports
// the scenario where an index key is moving also. The old index key is
// specified in unrefs, and the new index key is specified in refs.
//
// This move operation can be used to make a temporary key permanent, with
// overwrite false for create, or true for update. It can also be used for
// delete by making source and destination the same and specifying an already
// expired ttl.
func (ts *TreeStore) MoveReferencedKey(srcSk, destSk StoreKey, overwrite bool, ttl int64, refs []StoreKey, unrefs []StoreKey) (exists, moved bool) {
	// the entire database is locked for implementation simplicity
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	slevel, tokenIndex, skn, expired := ts.locateKeyNodeForLock(srcSk)
	if tokenIndex < len(srcSk.Tokens) || expired {
		return
	}
	exists = true

	// if not overwriting, ensure refs do not exist
	if !overwrite {
		for _, ref := range refs {
			inUnrefs := false
			for _, unref := range unrefs {
				if ref.Path == unref.Path {
					inUnrefs = true
					break
				}
			}
			if !inUnrefs {
				_, refTokenIndex, rkn, refExpired := ts.locateKeyNodeForLock(ref)
				if refTokenIndex >= len(ref.Tokens) && !refExpired && rkn.current != nil && len(rkn.current.relationships) > 0 {
					if rkn.current.relationships[0] > 0 && rkn.current.relationships[0] != skn.address {
						// ref exists and points to something other than the source - do not move
						return
					}
				}
			}
		}
	}

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
	if ttl < 0 {
		if len(destSk.Tokens) > 0 {
			// dest not the sentinel - move expiration
			dkn.expiration = skn.expiration
		}
	} else {
		dkn.expiration = ttl
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

	for _, unrefSk := range unrefs {
		_, tokenIndex, kn, expired := ts.locateKeyNodeForLock(unrefSk)
		if tokenIndex >= len(unrefSk.Tokens) && !expired && kn.current != nil {
			for i, addr := range kn.current.relationships {
				if addr == skn.address {
					if len(kn.current.relationships) == 1 {
						kn.expiration = 1
					} else {
						kn.current.relationships[i] = 0
					}
				}
			}
		}
	}

	for _, refSk := range refs {
		kn, created := ts.ensureKeyExclusive(refSk)
		if created {
			now := currentUnixTimestampBytes()
			kn.history = newAvlTree[*valueInstance]()
			kn.current = &valueInstance{
				relationships: []StoreAddress{dkn.address},
			}
			kn.history.Set(now, kn.current)
			ts.keys[refSk.Path] = kn.address
		} else if kn.current != nil {
			for i, addr := range kn.current.relationships {
				if addr == skn.address {
					kn.current.relationships[i] = dkn.address
				}
			}
		}

		if ttl >= 0 {
			kn.expiration = ttl
		}
	}

	moved = true
	return
}
