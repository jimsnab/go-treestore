package treestore

import "time"

func (kn *keyNode) isExpired() bool {
	if kn.expiration > 0 {
		return kn.expiration < time.Now().UTC().UnixNano()
	} else {
		return false
	}
}

func (kn *keyNode) hasChild() (found bool) {
	if kn.nextLevel != nil {
		kn.nextLevel.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			if !node.value.isExpired() || node.value.hasChild() {
				found = true
				return false
			}

			return true
		})
	}
	return
}

func (kn *keyNode) getParent() (pkn *keyNode) {
	if kn.ownerTree != nil {
		pkn = kn.ownerTree.parent
	}
	return
}

func (kn *keyNode) getTokenSet() (set TokenSet) {
	if kn.ownerTree != nil && kn.ownerTree.parent != kn {
		set = kn.ownerTree.parent.getTokenSet()
	} else {
		set = []TokenSegment{}
	}
	set = append(set, kn.key)

	return
}
