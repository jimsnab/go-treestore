package treestore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type (
	exportedNode struct {
		History    []*exportedValue         `json:"history,omitempty"`
		Metadata   map[string]string        `json:"metadata,omitempty"`
		Expiration *int64                   `json:"expiration,omitempty"`
		Children   map[string]*exportedNode `json:"children,omitempty"`
		Ki         []*exportedKid           `json:"ki,omitempty"`
	}

	exportedValue struct {
		Timestamp     int64    `json:"timestamp"`
		Value         string   `json:"value"`
		Type          string   `json:"type,omitempty"`
		Relationships []string `json:"relationships,omitempty"`
	}

	exportedKid struct {
		IndexKey string   `json:"index_key"`
		Fields   []string `json:"fields"`
	}

	testHook func()
)

var invalidAddrHook testHook = func() {}

// Serialize the tree store into a single JSON doc.
//
// N.B., The document is constructed entirely in memory and will hold an
// exclusive lock during the operation.
func (ts *TreeStore) Export(sk StoreKey) (jsonData []byte, err error) {
	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	level, tokenIndex, kn, expired := ts.locateKeyNodeForReadLocked(sk)
	defer ts.completeKeyNodeRead(level)

	var en *exportedNode
	if tokenIndex >= len(sk.Tokens) && !(expired && kn.nextLevel == nil) {
		if en, err = ts.exportNode(sk, kn); err != nil {
			return
		}
	}

	jsonData, err = json.Marshal(en)
	return
}

// worker that serializes the key node and its children
func (ts *TreeStore) exportNode(rootSk StoreKey, kn *keyNode) (en *exportedNode, err error) {
	now := time.Now().UnixNano()

	en = &exportedNode{
		Metadata: kn.metadata,
		Ki:       ts.exportKi(kn.indicies),
	}

	if kn.expiration != 0 {
		en.Expiration = &kn.expiration
	}

	if kn.history != nil {
		en.History = make([]*exportedValue, 0, kn.history.nodes)

		kn.history.Iterate(func(node *avlNode[*valueInstance]) bool {
			vi := node.value
			var ev *exportedValue
			ev, err = ts.exportValue(rootSk, vi, unixNsFromBytes(node.key))
			if err != nil {
				return false
			}
			en.History = append(en.History, ev)
			return true
		})

		if err != nil {
			return
		}

	} else if kn.current != nil {
		var ev *exportedValue
		if ev, err = ts.exportValue(rootSk, kn.current, 0); err != nil {
			return
		}
		en.History = []*exportedValue{ev}
	}

	if kn.nextLevel != nil {
		en.Children = make(map[string]*exportedNode, kn.nextLevel.tree.nodes)

		kn.nextLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			// don't export expired nodes
			if node.value.expiration > 0 && node.value.expiration < now {
				return true
			}

			var childEn *exportedNode
			if childEn, err = ts.exportNode(rootSk, node.value); err != nil {
				return false
			}

			key := TokenSegmentToString(node.key)
			en.Children[key] = childEn
			return true
		})

		if err != nil {
			return
		}
	}
	return
}

func (ts *TreeStore) exportValue(rootSk StoreKey, vi *valueInstance, timestamp int64) (*exportedValue, error) {
	ev := &exportedValue{
		Timestamp: timestamp,
	}

	if vi.relationships != nil {
		ev.Relationships = make([]string, 0, len(vi.relationships))
		for _, addr := range vi.relationships {
			// Since addresses change on export/import - record relationship with a
			// "dot" directive for special paths, or the absolute path when the
			// relationship points outside the export root.
			if addr == 1 {
				ev.Relationships = append(ev.Relationships, ".sentinel")
			} else {
				toPath, exists := ts.keyFromAddressLocked(addr)
				if !exists {
					invalidAddrHook()
					ev.Relationships = append(ev.Relationships, ".invalid")
				} else if toPath.Path == rootSk.Path {
					ev.Relationships = append(ev.Relationships, ".self")
				} else if strings.HasPrefix(string(toPath.Path), string(rootSk.Path)+"/") {
					ev.Relationships = append(ev.Relationships, fmt.Sprintf(".rel:%s", toPath.Path[len(rootSk.Path)+1:]))
				} else {
					// toPath starts with a slash
					ev.Relationships = append(ev.Relationships, string(toPath.Path))
				}
			}
		}
	}

	switch t := vi.value.(type) {
	case nil:
		// nothing
	case string:
		// common value
		ev.Value = t
	case []byte:
		// make friendly if possible
		friendly := true
		for _, by := range t {
			if by < 32 || by > 127 {
				friendly = false
				break
			}
		}
		if friendly {
			ev.Value = string(t)
			ev.Type = "byte-string"
		} else {
			ev.Value = base64.StdEncoding.EncodeToString(t)
			ev.Type = "base64"
		}
	case int, int64, uint64, bool, float64:
		ev.Value = fmt.Sprintf("%v", t)
		ev.Type = fmt.Sprintf("%T", t)
	default:
		data, err := json.Marshal(t)
		if err != nil {
			return nil, err
		} else {
			ev.Value = base64.StdEncoding.EncodeToString(data)
			ev.Type = "base64-json"
		}
	}

	return ev, nil
}

func (ts *TreeStore) exportKi(ki *keyIndicies) []*exportedKid {
	if ki == nil {
		return nil
	}

	eki := make([]*exportedKid, 0, len(ki.indexMap))
	for _, kid := range ki.indexMap {
		ekid := exportedKid{
			IndexKey: string(kid.indexSk.Path),
			Fields:   make([]string, 0, len(kid.fields)),
		}
		for _, field := range kid.fields {
			ekid.Fields = append(ekid.Fields, string(TokenSetToTokenPath(TokenSet(field))))
		}

		eki = append(eki, &ekid)
	}
	return eki
}
