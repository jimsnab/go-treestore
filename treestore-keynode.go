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
