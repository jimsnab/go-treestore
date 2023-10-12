package treestore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Creates a key from an export format json doc and adds it to the tree store
// at the specified sk. If the key exists, it and its children will be replaced.
func (ts *TreeStore) Import(sk StoreKey, jsonData []byte) (err error) {
	// Parse the json doc
	var en *exportedNode
	if err = json.Unmarshal(jsonData, &en); err != nil {
		return
	}

	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	ts.deferredRefs = []*deferredRef{}

	kn, ll, _ := ts.ensureKey(sk)
	defer ts.completeKeyNodeWrite(ll)

	ts.resetNode(sk, kn)

	if err = ts.restoreKey(sk.Path, sk, kn, en); err != nil {
		return
	}

	for _, dr := range ts.deferredRefs {
		targetSk := MakeStoreKeyFromPath(TokenPath(dr.target))
		_, tokenIndex, kn, _ := ts.locateKeyNodeForLock(targetSk)
		if tokenIndex < len(targetSk.Tokens) {
			invalidAddrHook()
		} else {
			dr.vi.relationships[dr.index] = kn.address
		}
	}
	ts.deferredRefs = nil

	return
}

func (ts *TreeStore) restoreKey(rootPath TokenPath, sk StoreKey, kn *keyNode, en *exportedNode) (err error) {
	if en != nil {
		if en.Expiration != nil {
			kn.expiration = *en.Expiration
		}
		if en.History != nil {
			if len(en.History) == 1 && en.History[0].Timestamp == 0 {
				var vi *valueInstance
				if vi, err = ts.importValue(rootPath, kn.address, en.History[0]); err != nil {
					return
				}
				kn.current = vi
			} else {
				newest := int64(0)
				kn.history = newAvlTree[*valueInstance]()
				for _, ev := range en.History {
					var vi *valueInstance
					if vi, err = ts.importValue(rootPath, kn.address, ev); err != nil {
						return
					}
					if ev.Timestamp > newest {
						kn.current = vi
					}
					kn.history.Set(unixTimestampBytes(ev.Timestamp), vi)
				}
			}
			ts.keys[sk.Path] = kn.address
		}
		kn.metadata = en.Metadata

		if en.Children != nil {
			childLevel := newKeyTree(kn)
			kn.nextLevel = childLevel

			for childSeg, child := range en.Children {
				childSk := AppendStoreKeySegmentStrings(sk, childSeg)
				childKn := ts.appendKeyNode(childLevel, TokenStringToSegment(childSeg))
				if err = ts.restoreKey(rootPath, childSk, childKn, child); err != nil {
					return
				}
			}
		}

		if en.Kals != nil {
			kn.autoLinks = ts.importKals(en.Kals)
		}
	}

	return
}

func (ts *TreeStore) importValue(rootPath TokenPath, selfAddr StoreAddress, ev *exportedValue) (vi *valueInstance, err error) {
	if ev == nil {
		return
	}

	vi = &valueInstance{}

	if ev.Relationships != nil {
		// Recover the relationship array by converting key locations to addresses
		vi.relationships = make([]StoreAddress, 0, len(ev.Relationships))
		for _, rel := range ev.Relationships {
			if rel == ".sentinel" {
				vi.relationships = append(vi.relationships, 1)
			} else if rel == ".invalid" {
				invalidAddrHook()
				vi.relationships = append(vi.relationships, StoreAddress((1<<64)-1))
			} else if rel == ".self" {
				vi.relationships = append(vi.relationships, selfAddr)
			} else {
				var fullPath string
				if strings.HasPrefix(rel, ".rel:") {
					fullPath = fmt.Sprintf("%s/%s", rootPath, rel[5:])
				} else {
					fullPath = rel
				}

				targetSk := MakeStoreKeyFromPath(TokenPath(fullPath))
				_, tokenIndex, kn, _ := ts.locateKeyNodeForLock(targetSk)
				if tokenIndex < len(targetSk.Tokens) {
					// might not be loaded yet - defer
					ts.deferredRefs = append(ts.deferredRefs, &deferredRef{target: TokenPath(fullPath), vi: vi, index: len(vi.relationships)})
					vi.relationships = append(vi.relationships, StoreAddress((1<<64)-1))
				} else {
					vi.relationships = append(vi.relationships, kn.address)
				}
			}
		}
	}

	switch ev.Type {
	case "":
		// string
		vi.value = ev.Value
	case "byte-string":
		vi.value = []byte(ev.Value)
	case "int":
		vi.value, err = strconv.Atoi(ev.Value)
	case "int64":
		vi.value, err = strconv.ParseInt(ev.Value, 10, 64)
	case "uint64":
		vi.value, err = strconv.ParseUint(ev.Value, 10, 64)
	case "bool":
		vi.value, err = strconv.ParseBool(ev.Value)
	case "float64":
		vi.value, err = strconv.ParseFloat(ev.Value, 64)
	case "base64":
		var v []byte
		if v, err = base64.StdEncoding.DecodeString(ev.Value); err != nil {
			return
		}
		vi.value = v
	case "base64-json":
		var v []byte
		if v, err = base64.StdEncoding.DecodeString(ev.Value); err != nil {
			return
		}
		json.Unmarshal(v, &vi.value)
	}

	return
}

func (ts *TreeStore) importKals(ekals []*exportedKal) (kal *keyAutoLinks) {
	if ekals == nil {
		return nil
	}

	kal = &keyAutoLinks{
		autoLinkMap: make(map[TokenPath]*keyAutoLinkDefinition, len(ekals)),
	}

	for _, ekal := range ekals {
		kald := keyAutoLinkDefinition{
			indexSk: MakeStoreKeyFromPath(TokenPath(ekal.IndexKey)),
			fields:  make([]SubPath, 0, len(ekal.Fields)),
		}
		for _, field := range ekal.Fields {
			kald.fields = append(kald.fields, UnescapeSubPath(EscapedSubPath(field)))
		}

		kal.autoLinkMap[kald.indexSk.Path] = &kald
	}
	return
}
