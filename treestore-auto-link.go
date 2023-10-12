package treestore

type (
	AutoLinkPath TokenSet

	keyAutoLinkDefinition struct {
		autoLinkSk StoreKey
		fields     []SubPath
	}

	keyAutoLinks struct {
		autoLinkMap map[TokenPath]*keyAutoLinkDefinition
	}

	recordDataCallback func(seg TokenSegment, affected bool)

	AutoLinkDefinition struct {
		AutoLinkSk StoreKey
		Fields     []SubPath
	}

	changedRecordState struct {
		recordKn  *keyNode
		recordSk  StoreKey
		alBaseSk  StoreKey
		removal   bool
		tree      bool
		changedSk StoreKey
	}
)

// Makes an auto-link definition.
//
// To use auto-linking, target data must be stored in a specific way:
//
//   * A "record" to be linked is a key, possibly with child keys. It must have
//     a unique ID. (Key values aren't linkable.)
//
//   * The path to a record must be stored as <parent>/<unique id>/<record>,
//     where <record> is typically a key tree of properites.
//
//   * The `dataParentSk` parameter specifies <parent>.
//
// An auto-link key is maintained according to `fields`:
//
//   * A "field" is a subpath of the record; or an empty subpath for the record ID.
//
//   * The auto-link key is constructed as <auto-link-key>/<field-value>/<field-value>/...
//
//   * When the record key is created, the corresponding auto-link key is
//     also created, and relationship 0 holds the address of the record.
//
//   * When the record key is deleted, the corresponding auto-link key is
//     also deleted.
//
// A typical pattern is to stage key creation in a staging key, and then move
// the key under `dataParentSk`. The record becomes atomically linked upon
// that move.
//
// Using the TreeStore Json APIs works very well with auto-links.
//
// Creating an auto-link key requires an exclusive lock of the database. If the data
// parent key does not exist, it will be created. The operation will be nearly
// instant if the data parent key has little to no children. A large number of
// records will take some time to link.
//
// Links might point to expired keys. It is handy to use GetRelationshipValue
// to determine if the auto-link entry is valid, and to get the key's current value.
//
// If one of the `fields` can contain multiple children, it is important to
// include the record ID at the tail of the field subpath, to avoid overlapping
// auto-link keys (which results in loss of links).
func (ts *TreeStore) DefineAutoLinkKey(dataParentSk, autoLinkSk StoreKey, fields []SubPath) (recordKeyExists, autoLinkCreated bool) {
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	_, tokenIndex, _, expired := ts.locateKeyNodeForLock(autoLinkSk)
	if tokenIndex >= len(autoLinkSk.Tokens) && !expired {
		// not allowed to create this auto-link key because it already exists
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
		_, defined := kals.autoLinkMap[autoLinkSk.Path]
		if defined {
			return
		}
	}

	kald := keyAutoLinkDefinition{
		autoLinkSk: autoLinkSk,
		fields:     fields,
	}
	kals.autoLinkMap[autoLinkSk.Path] = &kald
	ts.populateAutoLink(dataParentSk, kn, &kald)
	autoLinkCreated = true
	return
}

// Removes an auto-link definition from a store key.
//
// See DefineAutoLinkKey for details on treestore auto-links.
//
// An exclusive lock is held during the removal of the auto-link definition. If the
// number of links are high, the operation may take some time to delete.
func (ts *TreeStore) RemoveAutoLinkKey(dataParentSk, autoLinkSk StoreKey) (recordKeyExists, autoLinkRemoved bool) {
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	_, tokenIndex, kn, expired := ts.locateKeyNodeForLock(dataParentSk)
	if tokenIndex >= len(dataParentSk.Tokens) && !expired {
		recordKeyExists = true

		ki := kn.autoLinks
		if ki != nil {
			_, defined := ki.autoLinkMap[autoLinkSk.Path]
			if defined {
				delete(ki.autoLinkMap, autoLinkSk.Path)
				autoLinkRemoved = ts.deleteKeyTreeLocked(autoLinkSk)
			}
		}
	}

	return
}

func (ts *TreeStore) populateAutoLink(dataParentSk StoreKey, dataParentKn *keyNode, kald *keyAutoLinkDefinition) {
	//
	// Iterate all of the unique IDs under recordSk, and establish links for each.
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

// worker - iterates key segments for an auto-link field, filtering to only those that are
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
//	in the auto-link path.
//
//	A subPath can contain nil array elements. Those will match any record key segment.
func (ts *TreeStore) iterateRecordFieldWorker(crs *changedRecordState, subPath SubPath, callback recordDataCallback) {
	// if subPath is empty, return the record unique ID
	if len(subPath) == 0 {
		// only affected when added; the id does not change for removal
		callback(crs.recordKn.key, true)
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
		// Because otherwise the auto-link can refer to records where
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

// recursive worker - iterates the auto-link subpath(s) impacted by a record change
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
func (ts *TreeStore) iterateAffectedFieldSubpaths(crs *changedRecordState, subPaths []SubPath, parent AutoLinkPath, parentAffected bool) {
	leaf := len(subPaths) == 1

	ts.iterateRecordFieldWorker(crs, subPaths[0], func(seg TokenSegment, affected bool) {
		child := append(parent, seg)
		if leaf {
			if affected || parentAffected {
				autoLinkSk := AppendStoreKeySegments(crs.alBaseSk, child...)
				if crs.removal {
					ts.deleteKeyUpToLocked(crs.alBaseSk, autoLinkSk)
				} else {
					ts.setKeyValueExLocked(autoLinkSk, nil, SetExNoValueUpdate|SetExMustNotExist, 0, []StoreAddress{crs.recordKn.address})
				}
			}
		} else {
			ts.iterateAffectedFieldSubpaths(crs, subPaths[1:], child, parentAffected || affected)
		}
	})
}

// worker - given a key of a record that has changed, iterates through every impacted auto-link key
func (ts *TreeStore) processAutoLinkPaths(crs *changedRecordState, fields []SubPath) {
	if len(fields) > 0 {
		ts.iterateAffectedFieldSubpaths(crs, fields, AutoLinkPath{}, false)
	}
}

// worker - starting from a changed record, key segments are walked backwards to find
// auto-link definition(s). For each ald, the auto-link fields are processed, and if impacted
// by the modified record key (or subkey), the auto-link key(s) are updated to reflect
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
				// process this auto-link definition
				crs.alBaseSk = kald.autoLinkSk
				ts.processAutoLinkPaths(&crs, kald.fields)
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
			ts.deleteKeyTreeLocked(kald.autoLinkSk)
		}
		kn.autoLinks = nil
	}
}

// Returns all auto-link definitions defined for the specified data key, or nil if none.
func (ts *TreeStore) GetAutoLinkDefinition(dataParentSk StoreKey) (alds []AutoLinkDefinition) {
	level, tokenIndex, kn, expired := ts.locateKeyNodeForRead(dataParentSk)
	defer ts.completeKeyNodeRead(level)

	if tokenIndex < len(dataParentSk.Tokens) || expired {
		return
	}

	if kn.autoLinks != nil && len(kn.autoLinks.autoLinkMap) > 0 {
		alds = make([]AutoLinkDefinition, 0, len(kn.autoLinks.autoLinkMap))
		for _, kald := range kn.autoLinks.autoLinkMap {
			elem := AutoLinkDefinition{
				AutoLinkSk: kald.autoLinkSk,
				Fields:     kald.fields,
			}
			alds = append(alds, elem)
		}
	}
	return
}
