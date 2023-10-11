package treestore

type (
	IndexPath TokenSet

	keyIndexDefinition struct {
		indexSk StoreKey
		fields  []SubPath
	}

	keyIndicies struct {
		indexMap map[TokenPath]*keyIndexDefinition
	}

	recordDataCallback func(seg TokenSegment)
	indexKeyCallback   func(ip IndexPath)
	indexSkCallback    func(sk StoreKey, kn *keyNode)

	IndexDefinition struct {
		IndexSk StoreKey
		Fields  []SubPath
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
// Using the TreeStore Json APIs works very well with indexes.
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
		kn, _ = ts.ensureKeyExclusive(dataParentSk)
	}

	ki := kn.indicies
	if ki == nil {
		ki = &keyIndicies{
			indexMap: map[TokenPath]*keyIndexDefinition{},
		}
		kn.indicies = ki
	} else {
		_, defined := ki.indexMap[indexSk.Path]
		if defined {
			return
		}
	}

	kid := keyIndexDefinition{
		indexSk: indexSk,
		fields:  fields,
	}
	ki.indexMap[indexSk.Path] = &kid
	ts.populateIndex(dataParentSk, kn, &kid)
	indexCreated = true
	return
}

// Removes an index from a store key.
//
// See CreateIndex for details on treestore indexes.
//
// An exclusive lock is held during the removal of the index. If the
// index is large, the operation may take some time to delete.
func (ts *TreeStore) DeleteIndex(dataParentSk, indexSk StoreKey) (recordKeyExists, indexRemoved bool) {
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	_, tokenIndex, kn, expired := ts.locateKeyNodeForLock(dataParentSk)
	if tokenIndex >= len(dataParentSk.Tokens) && !expired {
		recordKeyExists = true

		ki := kn.indicies
		if ki != nil {
			_, defined := ki.indexMap[indexSk.Path]
			if defined {
				delete(ki.indexMap, indexSk.Path)
				indexRemoved = ts.deleteKeyTreeLocked(indexSk)
			}
		}
	}

	return
}

func (ts *TreeStore) populateIndex(dataParentSk StoreKey, dataParentKn *keyNode, kid *keyIndexDefinition) {
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
			ts.addToIndicies(tokens, kn)
		}
		return true
	})
}

// worker - iterates all of the children of the specified record key; the leaf node is
// the "field value" used in the index path.
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
func (ts *TreeStore) iterateRecordFieldWorker(recordSk StoreKey, subPath SubPath, includeExpired bool, callback recordDataCallback) {
	// if subPath is empty, return the record unique ID
	if len(subPath) == 0 {
		callback(recordSk.Tokens[len(recordSk.Tokens)-1])
		return
	}

	// iterate the keys within the record that match the specified subpath
	fieldSk := JoinSubPath(recordSk, subPath)
	ts.locateKeyNodesLocked(fieldSk, includeExpired, func(level *keyTree, recordKn *keyNode) {
		if recordKn.nextLevel == nil {
			return
		}
		tree := recordKn.nextLevel.tree

		// iterate the child segment(s) - these are the field values
		switch tree.nodes {
		case 0:
			// unreachable
			return

		case 1:
			callback(recordKn.nextLevel.tree.root.key)
			return

		default:
			tree.Iterate(func(node *avlNode[*keyNode]) bool {
				kn := node.value
				if !kn.isExpired() {
					callback(kn.key)
				}
				return true
			})
			return
		}
	})
}

// recursive worker - adds each child field to the recordSk
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
func (ts *TreeStore) iterateIndexPathWorker(recordSk StoreKey, subPaths []SubPath, parent IndexPath, includeExpired bool, callback indexKeyCallback) {
	leaf := len(subPaths) == 1

	ts.iterateRecordFieldWorker(recordSk, subPaths[0], includeExpired, func(seg TokenSegment) {
		child := append(parent, seg)
		if leaf {
			callback(child)
		} else {
			ts.iterateIndexPathWorker(recordSk, subPaths[1:], child, includeExpired, callback)
		}
	})
}

// worker - given a record key, iterates through every matching index key
func (ts *TreeStore) processIndexPaths(recordSk StoreKey, recordKn *keyNode, kid *keyIndexDefinition, includeExpired bool, callback indexKeyCallback) {
	if recordKn == nil {
		// all records gone; invoke callback with nil
		callback(nil)
		return
	}

	if len(kid.fields) > 0 {
		ts.iterateIndexPathWorker(recordSk, kid.fields, IndexPath{}, includeExpired, callback)
	}
}

// worker - for each index associated with this record key, iterates through every index key
func (ts *TreeStore) processKeyIndex(tokens TokenSet, recordKn *keyNode, includeExpired bool, callback indexSkCallback) {
	kn := recordKn

	for end := len(tokens); end > 0; end-- {
		idKn := kn
		kn = kn.getParent()
		if kn.indicies != nil {
			for _, kid := range kn.indicies.indexMap {
				recordSk := MakeStoreKeyFromTokenSegments(tokens[0:end]...)
				ts.processIndexPaths(recordSk, idKn, kid, includeExpired, func(ip IndexPath) {
					if ip == nil {
						callback(kid.indexSk, nil)
					} else {
						callback(AppendStoreKeySegments(kid.indexSk, ip...), idKn)
					}
				})
			}
		}
	}
}

// Creation of some or all of the sk occurred. Caller must hold write lock on ts.keyNodeMu.
func (ts *TreeStore) addToIndicies(tokens TokenSet, kn *keyNode) {
	ts.processKeyIndex(tokens, kn, false, func(indexSk StoreKey, recordKn *keyNode) {
		if kn != nil {
			ts.setKeyValueExLocked(indexSk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, []StoreAddress{recordKn.address})
		}
	})
}

// Removal of sk occurred. Caller must hold write lock on ts.keyNodeMu.
func (ts *TreeStore) removeFromIndicies(tokens TokenSet, kn *keyNode) {
	ts.processKeyIndex(tokens, kn, true, func(indexSk StoreKey, recordKn *keyNode) {
		if recordKn == nil {
			ts.deleteKeyTreeLocked(indexSk)
		} else {
			ts.deleteKeyLocked(indexSk)
		}
	})
}

// Record key was destroyed. Caller must hold write lock on ts.keyNodeMu.
func (ts *TreeStore) purgeIndicies(kn *keyNode) {
	if kn.indicies != nil {
		for _, kid := range kn.indicies.indexMap {
			ts.deleteKeyTreeLocked(kid.indexSk)
		}
		kn.indicies = nil
	}
}

// Returns all indexes defined for the specified data key, or nil if none.
func (ts *TreeStore) GetIndex(dataParentSk StoreKey) (id []IndexDefinition) {
	level, index, kn, expired := ts.locateKeyNodeForRead(dataParentSk)
	defer ts.completeKeyNodeRead(level)

	if index < len(dataParentSk.Tokens) || expired {
		return
	}

	if kn.indicies != nil && len(kn.indicies.indexMap) > 0 {
		id = make([]IndexDefinition, 0, len(kn.indicies.indexMap))
		for _, kid := range kn.indicies.indexMap {
			elem := IndexDefinition{
				IndexSk: kid.indexSk,
				Fields:  kid.fields,
			}
			id = append(id, elem)
		}
	}
	return
}
