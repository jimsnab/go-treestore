package treestore

// Discards all data, completely resetting the treestore instance.
func (ts *TreeStore) Purge() {
	ts2 := NewTreeStore(ts.l, ts.appVersion)

	ts.acquireExclusiveLock()

	ts.dbNode = ts2.dbNode
	ts.dbNodeLevel = ts2.dbNodeLevel
	ts.nextAddress.Store(ts2.nextAddress.Load())
	ts.addresses = ts2.addresses
	ts.keys = ts2.keys
	ts.cas = ts2.cas
	ts.deferredRefs = ts2.deferredRefs

	ts.dbNodeLevel.parent = &ts.dbNode

	ts.l.Warn("database content purged!")
	defer ts.releaseExclusiveLock()
}
