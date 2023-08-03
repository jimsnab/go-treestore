package treestore

import (
	"fmt"
	"strings"
	"time"
)

type (
	treeStoreDump struct {
		ts        *TreeStore
		used      map[TokenPath]StoreAddress
		addresses map[StoreAddress]*keyNode
		errors    []string
	}
)

// Prints the tree store, returns false if an error was found
func (ts *TreeStore) DiagDump() bool {
	rootSk := StoreKey{
		Tokens: TokenSet{},
		Path:   "",
	}

	treeStoreDump := &treeStoreDump{
		ts:        ts,
		used:      map[TokenPath]StoreAddress{},
		addresses: map[StoreAddress]*keyNode{},
		errors:    []string{},
	}

	al := ts.activeLocks.Load()
	if al != 0 {
		treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("%d active locks != 0", al))
	}

	fmt.Printf("values: %d\n", len(ts.keys))
	treeStoreDump.dumpLevel(ts.dbNode.ownerTree, "", &ts.dbNode, &rootSk)

	if len(treeStoreDump.used) != len(ts.keys) {
		treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("mismatch in %d iterated keys with values versus the key index length %d", len(treeStoreDump.used), len(ts.keys)))
	} else {
		for tp, addr := range treeStoreDump.used {
			indexAddr, exists := ts.keys[tp]
			if !exists {
				treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("key path %s not found in index", tp))
			} else if indexAddr != addr {
				treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("key path %s address %04X has index value %04X", tp, addr, indexAddr))
			}
		}
	}

	if len(treeStoreDump.addresses) != len(ts.addresses) {
		treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("mismatch in %d iterated key node addresses versus the address index length %d", len(treeStoreDump.addresses), len(ts.addresses)))
	} else {
		for addr, kn := range treeStoreDump.addresses {
			indexKn, exists := ts.addresses[addr]
			if !exists {
				treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("key node address %04X not found in address index", addr))
			} else if kn != indexKn {
				treeStoreDump.errors = append(treeStoreDump.errors, fmt.Sprintf("key node address %04X key node %p mismatches indexed value %p", addr, kn, indexKn))
			}
		}
	}

	for _, err := range treeStoreDump.errors {
		fmt.Printf("error: %s\n", err)
	}

	return len(treeStoreDump.errors) == 0
}

func (tsd *treeStoreDump) dumpLevel(level *keyTree, indent string, expectedParent *keyNode, baseSk *StoreKey) {
	if level.parent != expectedParent {
		tsd.errors = append(tsd.errors, fmt.Sprintf("tree level's parent %p is not the expected parent %p", level.parent, expectedParent))
	}

	level.tree.Iterate(func(node *avlNode[*keyNode]) bool {
		kn := node.value

		sk := &StoreKey{
			Tokens: baseSk.Tokens,
		}
		if node.value != &tsd.ts.dbNode {
			sk.Tokens = append(sk.Tokens, node.key)
		}
		sk.Path = TokenSetToTokenPath(sk.Tokens)

		indexAddr, isIndexed := tsd.ts.keys[sk.Path]
		keyText := TokenSegmentToString(node.key)

		if isIndexed && indexAddr != kn.address {
			tsd.errors = append(tsd.errors, fmt.Sprintf("key %s index address is %v but node address is %v", keyText, indexAddr, kn.address))
		}

		if kn.current != nil || kn.history != nil {
			keyText += "  [HAS VALUE]"
			tsd.used[sk.Path] = kn.address
		}

		fmt.Printf("%s%04X %s\n", indent, kn.address, keyText)
		tsd.addresses[kn.address] = kn

		if kn.metadata != nil {
			fmt.Printf("%s  Metadata: %s\n", indent, strings.TrimPrefix(fmt.Sprintf("%v", kn.metadata), "map"))
		}

		if kn.expiration > 0 {
			expirationText := timestampFromUnixNs(kn.expiration).Format(time.RFC3339)
			if kn.expiration < time.Now().UTC().UnixNano() {
				expirationText = expirationText + "  [EXPIRED]"
			}
			fmt.Printf("%s  Expiration: %s\n", indent, expirationText)
		}

		var lastValue *valueInstance

		if kn.history != nil {
			kn.history.Iterate(func(node *avlNode[*valueInstance]) bool {
				vi := node.value
				timestamp := timestampFromUnixNs(unixNsFromBytes(node.key))

				valText := fmt.Sprintf("%v", vi.value)
				fmt.Printf("%s %s := %s\n", indent, timestamp.Format(time.RFC3339), cleanString(valText, 80))
				if len(vi.relationships) > 0 {
					fmt.Printf("%s ->", indent)
					for _, addr := range vi.relationships {
						fmt.Printf(" %04X", addr)
					}
					fmt.Printf("\n")
				}

				lastValue = vi
				return true
			})
		}

		if kn.current != lastValue {
			tsd.errors = append(tsd.errors, fmt.Sprintf("current value %p does not agree with history %p", kn.current, lastValue))
		}

		if kn.current != nil && !isIndexed {
			tsd.errors = append(tsd.errors, fmt.Sprintf("key %s has a value but is not found in index", sk.Path))
		}

		if kn.nextLevel != nil {
			tsd.dumpLevel(kn.nextLevel, indent+"  ", kn, sk)
		}

		return true
	})
}
