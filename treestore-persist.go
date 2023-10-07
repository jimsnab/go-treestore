package treestore

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/spf13/afero"
)

type (
	diskValue struct {
		Value         any
		Relationships []uint64
		Timestamp     int64
	}
	diskKeyNode struct {
		Key           []byte
		Address       uint64
		ParentAddress uint64
		Values        []diskValue
		Expiration    int64
		Metadata      map[string]string
		Indicies      []diskKid
	}
	diskHeader struct {
		Version            int
		AppVersion         int
		NextAddress        uint64
		SentinelValues     []diskValue
		SentinelMetadata   map[string]string
		SentinelExpiration int64
		Cas                map[StoreAddress]uint64
		// variable number of diskKeyNode structs follow, terminated by a diskKeyNode that has Address of 0
	}
	diskKid struct {
		IndexKey string
		Fields   []string
	}
)

var fs = afero.NewOsFs()

func serializeRelationshipArray(relationships []StoreAddress) []uint64 {
	var rel []uint64
	if relationships != nil {
		rel = make([]uint64, 0, len(relationships))
		for _, addr := range relationships {
			rel = append(rel, uint64(addr))
		}
	}

	return rel
}

func deserializeRelationshipArray(relationships []uint64) []StoreAddress {
	var rel []StoreAddress
	if relationships != nil {
		rel = make([]StoreAddress, 0, len(relationships))
		for _, addr := range relationships {
			rel = append(rel, StoreAddress(addr))
		}
	}

	return rel
}

func saveKeyValues(kn *keyNode) (values []diskValue) {
	if kn.history == nil {
		if kn.current != nil {
			rel := serializeRelationshipArray(kn.current.relationships)
			dv := diskValue{
				Value:         kn.current.value,
				Relationships: rel,
				// Timestamp of 0 indicates no history
			}

			values = []diskValue{dv}
		}
		return
	}

	values = make([]diskValue, 0, kn.history.nodes)

	kn.history.Iterate(func(node *avlNode[*valueInstance]) bool {
		vi := node.value

		rel := serializeRelationshipArray(vi.relationships)

		dv := diskValue{
			Value:         vi.value,
			Relationships: rel,
			Timestamp:     unixNsFromBytes(node.key),
		}

		values = append(values, dv)
		return true
	})

	return
}

func kiToDiskKi(ki *keyIndicies) []diskKid {
	if ki == nil {
		return nil
	}

	dki := make([]diskKid, 0, len(ki.indexMap))
	for _, kid := range ki.indexMap {
		dkid := diskKid{
			IndexKey: string(kid.indexSk.Path),
			Fields:   make([]string, 0, len(kid.fields)),
		}
		for _, field := range kid.fields {
			dkid.Fields = append(dkid.Fields, string(EscapeSubPath(field)))
		}
		dki = append(dki, dkid)
	}

	return dki
}

func diskKiToKi(dki []diskKid) *keyIndicies {
	if dki == nil {
		return nil
	}

	ki := keyIndicies{
		indexMap: make(map[TokenPath]*keyIndexDefinition, len(dki)),
	}

	for _, dkid := range dki {
		kid := keyIndexDefinition{
			indexSk: MakeStoreKeyFromPath(TokenPath(dkid.IndexKey)),
			fields:  make([]SubPath, 0, len(dkid.Fields)),
		}
		for _, field := range dkid.Fields {
			kid.fields = append(kid.fields, UnescapeSubPath(EscapedSubPath(field)))
		}

		ki.indexMap[kid.indexSk.Path] = &kid
	}

	return &ki
}

func saveChildren(parent *keyNode, enc *gob.Encoder) (err error) {
	level := parent.nextLevel
	if level != nil {
		level.tree.Iterate(func(node *avlNode[*keyNode]) bool {
			kn := node.value
			dkn := diskKeyNode{
				Key:           node.key,
				Address:       uint64(kn.address),
				ParentAddress: uint64(parent.address),
				Values:        saveKeyValues(kn),
				Expiration:    kn.expiration,
				Metadata:      kn.metadata,
				Indicies:      kiToDiskKi(kn.indicies),
			}

			if err = enc.Encode(dkn); err != nil {
				return false
			}

			if err = saveChildren(kn, enc); err != nil {
				return false
			}

			return true
		})
	}
	return
}

func (ts *TreeStore) acquireExclusiveLock() {
	ts.keyNodeMu.Lock()

	for {
		n := ts.activeLocks.Add(1)
		if n == 1 {
			break
		}
		ts.keyNodeMu.Unlock()
		ts.activeLocks.Add(-1)
		time.Sleep(time.Millisecond)
		ts.keyNodeMu.Lock()
	}
}

func (ts *TreeStore) releaseExclusiveLock() {
	ts.keyNodeMu.Unlock()
	ts.activeLocks.Add(-1)
}

func (ts *TreeStore) Save(l lane.Lane, fileName string) (err error) {
	var fh afero.File
	if fh, err = fs.Create(fileName); err != nil {
		l.Errorf("failed to create %s: %s", fileName, err.Error())
		return
	}
	defer fh.Close()

	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	hdr := diskHeader{
		Version:            1,
		AppVersion:         ts.appVersion,
		NextAddress:        uint64(ts.nextAddress),
		SentinelValues:     saveKeyValues(&ts.dbNode),
		SentinelMetadata:   ts.dbNode.metadata,
		SentinelExpiration: ts.dbNode.expiration,
	}
	hdr.Cas = ts.cas

	w := bufio.NewWriter(fh)
	enc := gob.NewEncoder(w)

	if err = enc.Encode(hdr); err != nil {
		l.Errorf("failed to encode header for %s: %s", fileName, err.Error())
		return
	}

	if err = saveChildren(&ts.dbNode, enc); err != nil {
		l.Errorf("failed to encode key node for %s: %s", fileName, err.Error())
		return
	}

	if err = enc.Encode(diskKeyNode{}); err != nil {
		l.Errorf("failed to encode termination key node for %s: %s", fileName, err.Error())
		return
	}

	if err = w.Flush(); err != nil {
		l.Errorf("failed to write %s: %s", fileName, err.Error())
		return
	}

	return
}

func loadValues(values []diskValue) (current *valueInstance, history *avlTree[*valueInstance]) {
	if values == nil {
		return
	}

	if len(values) == 1 && values[0].Timestamp == 0 {
		// no history
		current = &valueInstance{
			value:         values[0].Value,
			relationships: deserializeRelationshipArray(values[0].Relationships),
		}
		return
	}

	history = newAvlTree[*valueInstance]()
	for _, value := range values {
		current = &valueInstance{
			value:         value.Value,
			relationships: deserializeRelationshipArray(value.Relationships),
		}
		history.Set(unixTimestampBytes(value.Timestamp), current)
	}

	return
}

func (ts *TreeStore) addKeyToValueIndex(kn *keyNode, keys map[TokenPath]StoreAddress) {
	tokens := ts.getTokenSet(kn)
	keyPath := TokenSetToTokenPath(tokens)
	keys[keyPath] = kn.address
}

func (ts *TreeStore) Load(l lane.Lane, fileName string) (err error) {
	var fh afero.File
	if fh, err = fs.Open(fileName); err != nil {
		l.Errorf("failed to open %s: %s", fileName, err.Error())
		return
	}
	defer fh.Close()

	r := bufio.NewReader(fh)
	dec := gob.NewDecoder(r)

	hdr := diskHeader{}
	if err = dec.Decode(&hdr); err != nil {
		l.Errorf("failed to decode header for %s: %s", fileName, err.Error())
		return
	}

	if hdr.Version != 1 {
		err = fmt.Errorf("unsupported version %d", hdr.Version)
		l.Errorf("failed to load %s: %s", fileName, err.Error())
		return
	}

	if hdr.AppVersion != ts.appVersion {
		err = fmt.Errorf("unsupported app version %d (expected %d)", hdr.AppVersion, ts.appVersion)
		l.Errorf("failed to load %s: %s", fileName, err.Error())
		return
	}

	dbNode := &ts.dbNode
	sentinelCurrentValue, sentinelHistory := loadValues(hdr.SentinelValues)
	dbNode.current = sentinelCurrentValue
	dbNode.history = sentinelHistory
	dbNode.metadata = hdr.SentinelMetadata
	dbNode.expiration = hdr.SentinelExpiration

	addresses := map[StoreAddress]*keyNode{1: dbNode}
	keys := map[TokenPath]StoreAddress{}
	if dbNode.current != nil {
		ts.addKeyToValueIndex(dbNode, keys)
	}

	keyCount := 0
	valueCount := 0
	for {
		dkn := diskKeyNode{}
		if err = dec.Decode(&dkn); err != nil {
			l.Errorf("failed to decode key node for %s: %s", fileName, err.Error())
			return
		}

		if dkn.Address == 0 {
			break
		}

		parent, exists := addresses[StoreAddress(dkn.ParentAddress)]
		if !exists {
			err = fmt.Errorf("key node has bad parent address %X", dkn.ParentAddress)
			l.Errorf("failed to load key node from %s: %s", fileName, err.Error())
			return
		}

		level := parent.nextLevel
		if level == nil {
			level = newKeyTree(parent)
			parent.nextLevel = level
		}

		kn := keyNode{
			key:        dkn.Key,
			address:    StoreAddress(dkn.Address),
			ownerTree:  level,
			expiration: dkn.Expiration,
			metadata:   dkn.Metadata,
			indicies:   diskKiToKi(dkn.Indicies),
		}

		current, history := loadValues(dkn.Values)
		kn.current = current
		kn.history = history

		addresses[kn.address] = &kn
		level.tree.Set(kn.key, &kn)
		keyCount++

		if kn.current != nil {
			ts.addKeyToValueIndex(&kn, keys)
			valueCount++
		}
	}

	ts.acquireExclusiveLock()
	defer ts.releaseExclusiveLock()

	ts.addresses = addresses
	ts.keys = keys
	ts.cas = hdr.Cas

	l.Tracef("treestore: load: keys:%d values:%d appversion:%d", keyCount, valueCount, ts.appVersion)
	return
}
