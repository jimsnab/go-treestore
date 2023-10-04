package treestore

import (
	"bytes"
	"testing"
)

func compareSubPath(t *testing.T, sp1, sp2 SubPath) {
	if len(sp2) != len(sp1) {
		t.Error("length mismatch")
	}

	for i,seg := range sp1 {
		seg2 := sp2[i]
		if !bytes.Equal(seg, seg2) {
			t.Error("segments don't agree")
		}
	}
}

func TestSubPathNil(t *testing.T) {
	sp := SubPath{nil}

	path := EscapeSubPath(sp)
	if path != `\N` {
		t.Error("nil segment escape")
	}

	sp2 := UnescapeSubPath(path)
	compareSubPath(t, sp, sp2)
}

func TestSubPathEmpty(t *testing.T) {
	sp := SubPath{{}}

	path := EscapeSubPath(sp)
	if path != `\E` {
		t.Error("nil segment escape")
	}

	sp2 := UnescapeSubPath(path)
	compareSubPath(t, sp, sp2)
}