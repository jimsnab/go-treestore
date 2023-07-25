package treestore

import "time"

func (kn *keyNode) isExpired() bool {
	if kn.expiration > 0 {
		return kn.expiration < time.Now().UTC().UnixNano()
	} else {
		return false
	}
}
