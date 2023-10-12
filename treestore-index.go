package treestore

type (
	IndexPath TokenSet

	keyAutoLinkDefinition struct {
		indexSk StoreKey
		fields  []SubPath
	}

	keyAutoLinks struct {
		autoLinkMap map[TokenPath]*keyAutoLinkDefinition
	}

	recordDataCallback func(seg TokenSegment, affected bool)

	IndexDefinition struct {
		IndexSk StoreKey
		Fields  []SubPath
	}

	changedRecordState struct {
		recordKn    *keyNode
		recordSk    StoreKey
		indexBaseSk StoreKey
		removal     bool
		tree        bool
		changedSk   StoreKey
	}
)

// Makes an index definition.
//
// To use an index, target data must be stored in a specific way:
//
//			A "record" to be indexed is a key, possibly with child keys. It
//			must have a unique ID. (Key values aren't indexable.)
//
//			The path to a record must be stored as <parent>/<unique id>/<record>,
//	     where <record> is typically a key tree of properites.
//
//			The `dataParentSk` parameter specifies <parent>.
//
// An index is maintained according to `fields`:
//
//	      A "field" is a subpath of the record; an empty subpath for the record ID.
//
//			 The index key is constructed as <index>/<field>/<field>/...
//
//			 When the record key is created, the corresponding index key is
//		     also created, and relationship 0 holds the address of the record.
//
//			 When the record key is deleted, the corresponding index key is
//	      also deleted.
//
// A typical pattern is to stage key creation in a staging key, and then move
// the key under `dataParentSk`. The record becomes atomically indexed upon
// that move.
//
// Using the TreeStore Json APIs works very well with autoLinks.
//
// Creating an index acquires an exclusive lock of the database. If the data
// parent key does not exist, it will be created. The operation will be nearly
// instant if the data parent key has little to no children. A large number of
// records will take some time to index.
//
// Index entries might point to expired keys. It is handy to use GetRelationshipValue
// to determine if the index entry is valid, and to get the key's current value.
//
// If one of the `fields` can contain multiple children, it is important to
// include the record ID at the tail, to avoid overlapping index keys (which
// result in incorrect indexing).
func (ts *TreeStore) CreateIndex(dataParentSk, indexSk StoreKey, fields []SubPath) (recordKeyExists, indexCreated bool) {
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	_, tokenIndex, _, expired := ts.locateKeyNodeForLock(indexSk)
	if tokenIndex >= len(indexSk.Tokens) && !expired {
		// not allowed to create this index because index key already exists
		return
	}

	_, tokenIndex, kn, expired := ts.locateKeyNodeForLock(dataParentSk)
	if tokenIndex >= len(dataParentSk.Tokens) && !expired {
		recordKeyExists = true
	} else {
		kn, _ = ts.ensureKeyExclusive(dataParentSk, false)
	}

	kals := kn.autoLinks
	if kals == nil {
		kals = &keyAutoLinks{
			autoLinkMap: map[TokenPath]*keyAutoLinkDefinition{},
		}
		kn.autoLinks = kals
	} else {
		_, defined := kals.autoLinkMap[indexSk.Path]
		if defined {
			return
		}
	}

	kald := keyAutoLinkDefinition{
		indexSk: indexSk,
		fields:  fields,
	}
	kals.autoLinkMap[indexSk.Path] = &kald
	ts.populateIndex(dataParentSk, kn, &kald)
	indexCreated = true
	return
}

// Removes an index from a store key.
//
// See CreateIndex for details on treestore autoLinks.
//
// An exclusive lock is held during the removal of the index. If the
// index is large, the operation may take some time to delete.
func (ts *TreeStore) DeleteIndex(dataParentSk, indexSk StoreKey) (recordKeyExists, indexRemoved bool) {
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	_, tokenIndex, kn, expired := ts.locateKeyNodeForLock(dataParentSk)
	if tokenIndex >= len(dataParentSk.Tokens) && !expired {
		recordKeyExists = true

		ki := kn.autoLinks
		if ki != nil {
			_, defined := ki.autoLinkMap[indexSk.Path]
			if defined {
				delete(ki.autoLinkMap, indexSk.Path)
				indexRemoved = ts.deleteKeyTreeLocked(indexSk)
			}
		}
	}

	return
}

func (ts *TreeStore) populateIndex(dataParentSk StoreKey, dataParentKn *keyNode, kald *keyAutoLinkDefinition) {
	//
	// Iterate all of the unique IDs under recordSk, and establish index records for each.
	//

	if dataParentKn.nextLevel == nil {
		return
	}

	dataParentKn.nextLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
		kn := node.value
		if !kn.isExpired() {
			tokens := append(dataParentSk.Tokens, kn.key)
			ts.addAutoLinks(tokens, kn, true)
		}
		return true
	})
}

// worker - iterates key segments for an index field, filtering to only those that are
// altered by a key add/remove/move
//
// Example:
//
//	stored key: myrecords/123/user/Joe
//	recordSk:   myrecords/123
//	subPath: user
//
//	-> callback invoked with seg 'Joe'
//
// The recordSk can have multiple child keys. If example also has myrecords/123/user/Mary,
// callback is called with 'Joe' then 'Mary'.
//
// N.B., The entire subPath array can be empty; this will incorporate the record unique ID
//
//	in the index path.
//
//	A subPath can contain nil array elements. Those will match any record key segment.
func (ts *TreeStore) iterateRecordFieldWorker(crs *changedRecordState, subPath SubPath, callback recordDataCallback) {
	// if subPath is empty, return the record unique ID
	if len(subPath) == 0 {
		// only affected when added; the id does not change for removal
		callback(crs.recordKn.key, !crs.removal)
		return
	}

	// iterate the keys within the record that match the specified subpath
	containerSk := JoinSubPath(crs.recordSk, subPath)
	affected := storeKeyHasBase(containerSk, crs.changedSk)
	if !affected && crs.tree {
		// when a tree of keys changes at once, the whole changedSk must
		// be considered modified
		affected = storeKeyHasBase(crs.changedSk, containerSk)
	}

	ts.locateKeyNodesLocked(containerSk, func(level *keyTree, fieldKn *keyNode) {
		if fieldKn.nextLevel == nil {
			return
		}

		// DESIGN BUG: expiration design is wrong - need to change the
		// design to remove expired keys, and rework all the code to
		// assume if a key is present, it is not expired
		//
		// Because otherwise the index can refer to records where
		// some or all data has expired.
		//
		// This is a big change - will do later.

		tree := fieldKn.nextLevel.tree

		// iterate the child segment(s) - these are the field values
		switch tree.nodes {
		case 0:
			// unreachable
			return

		case 1:
			callback(fieldKn.nextLevel.tree.root.key, affected)
			return

		default:
			tree.Iterate(func(node *avlNode[*keyNode]) bool {
				kn := node.value
				if !kn.isExpired() {
					callback(kn.key, affected)
				}
				return true
			})
			return
		}
	})
}

// recursive worker - iterates the index subpath(s) impacted by a record change
//
// Example:
//
//	Stored data:
//	  /myrecord/123/user/Joe
//	  /myrecord/123/user/Mary
//	  /myrecord/123/service/status/active
//
//	Inputs:
//	    recordSk: "/myrecord/123"
//	    subPaths: [
//	      ["user"],
//	      ["service", "status"]
//	    ]
//
//	Outputs:
//	  callback(["Joe", "active"])
//	  callback(["Mary", "active"])
func (ts *TreeStore) iterateAffectedIndexSubpaths(crs *changedRecordState, subPaths []SubPath, parent IndexPath, parentAffected bool) {
	leaf := len(subPaths) == 1

	ts.iterateRecordFieldWorker(crs, subPaths[0], func(seg TokenSegment, affected bool) {
		child := append(parent, seg)
		if leaf {
			if affected || parentAffected {
				indexSk := AppendStoreKeySegments(crs.indexBaseSk, child...)
				if crs.removal {
					ts.deleteKeyUpToLocked(crs.indexBaseSk, indexSk)
				} else {
					ts.setKeyValueExLocked(indexSk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, []StoreAddress{crs.recordKn.address})
				}
			}
		} else {
			ts.iterateAffectedIndexSubpaths(crs, subPaths[1:], child, parentAffected || affected)
		}
	})
}

// worker - given a key of a record that has changed, iterates through every impacted index key
func (ts *TreeStore) processIndexPaths(crs *changedRecordState, fields []SubPath) {
	if len(fields) > 0 {
		ts.iterateAffectedIndexSubpaths(crs, fields, IndexPath{}, false)
	}
}

// worker - starting from a changed record, key segments are walked backwards to find
// index definition(s). For each index, the index fields are processed, and if impacted
// by the modified record key (or subkey), the index key(s) are updated to reflect
// the change.
func (ts *TreeStore) processKeyLinks(tokens TokenSet, recordKn *keyNode, removal, tree bool) {
	kn := recordKn // never nil, might be a subkey of a record

	crs := changedRecordState{
		removal:   removal,
		tree:      tree,
		changedSk: MakeStoreKeyFromTokenSegments(tokens...),
	}

	for end := len(tokens); end > 0; end-- {
		crs.recordKn = kn
		kn = kn.getParent()
		if kn.autoLinks != nil {
			// one or more autoLinks are defined at this record container key
			crs.recordSk = MakeStoreKeyFromTokenSegments(tokens[0:end]...)

			for _, kald := range kn.autoLinks.autoLinkMap {
				// process this index
				crs.indexBaseSk = kald.indexSk
				ts.processIndexPaths(&crs, kald.fields)
			}
		}
	}
}

// Creation of some or all of the sk occurred. Caller must hold write lock on ts.keyNodeMu.
func (ts *TreeStore) addAutoLinks(tokens TokenSet, kn *keyNode, tree bool) {
	ts.processKeyLinks(tokens, kn, false, tree)
}

// Removal of sk occurred. Caller must hold write lock on ts.keyNodeMu.
func (ts *TreeStore) removeAutoLinks(tokens TokenSet, kn *keyNode, tree bool) {
	ts.processKeyLinks(tokens, kn, true, tree)
}

// Record key was destroyed. Caller must hold write lock on ts.keyNodeMu.
func (ts *TreeStore) purgeIndicies(kn *keyNode) {
	if kn.autoLinks != nil {
		for _, kald := range kn.autoLinks.autoLinkMap {
			ts.deleteKeyTreeLocked(kald.indexSk)
		}
		kn.autoLinks = nil
	}
}

// Returns all autoLinks defined for the specified data key, or nil if none.
func (ts *TreeStore) GetIndex(dataParentSk StoreKey) (id []IndexDefinition) {
	level, index, kn, expired := ts.locateKeyNodeForRead(dataParentSk)
	defer ts.completeKeyNodeRead(level)

	if index < len(dataParentSk.Tokens) || expired {
		return
	}

	if kn.autoLinks != nil && len(kn.autoLinks.autoLinkMap) > 0 {
		id = make([]IndexDefinition, 0, len(kn.autoLinks.autoLinkMap))
		for _, kald := range kn.autoLinks.autoLinkMap {
			elem := IndexDefinition{
				IndexSk: kald.indexSk,
				Fields:  kald.fields,
			}
			id = append(id, elem)
		}
	}
	return
}
