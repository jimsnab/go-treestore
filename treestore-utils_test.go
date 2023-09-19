package treestore

import (
	"bytes"
	"testing"
)

func TestTokenEscape(t *testing.T) {
	escaped := EscapeTokenString("")
	if escaped != "" {
		t.Error("empty string")
	}

	escaped = EscapeTokenString("cat")
	if escaped != "cat" {
		t.Error("simple string")
	}

	escaped = EscapeTokenString("cat/dog")
	if escaped != `cat\sdog` {
		t.Error("forward slash string")
	}

	escaped = EscapeTokenString(`cat\dog`)
	if escaped != `cat\Sdog` {
		t.Error("backward slash string")
	}

	escaped = EscapeTokenString(`cat\Sdog`)
	if escaped != `cat\SSdog` {
		t.Error("backward slash string")
	}

	escaped = EscapeTokenString("cat\ndog")
	if escaped != `cat\x0Adog` {
		t.Error("control chars")
	}
}

func TestTokenUnescape(t *testing.T) {
	unescaped := UnescapeTokenString("")
	if unescaped != "" {
		t.Error("empty string")
	}

	unescaped = UnescapeTokenString("cat")
	if unescaped != "cat" {
		t.Error("simple string")
	}

	unescaped = UnescapeTokenString(`cat\sdog`)
	if unescaped != `cat/dog` {
		t.Error("forward slash string")
	}

	unescaped = UnescapeTokenString(`cat\Sdog`)
	if unescaped != `cat\dog` {
		t.Error("backward slash string")
	}

	unescaped = UnescapeTokenString(`cat\dog`)
	if unescaped != `cat\dog` {
		t.Error("malformed string")
	}

	unescaped = UnescapeTokenString(`cat\x0Adog`)
	if unescaped != "cat\ndog" {
		t.Error("malformed string")
	}

	unescaped = UnescapeTokenString(`cat\x0`)
	if unescaped != `cat\x0` {
		t.Error("malformed string")
	}

	unescaped = UnescapeTokenString(`cat\x`)
	if unescaped != `cat\x` {
		t.Error("malformed string")
	}

	unescaped = UnescapeTokenString(`cat\`)
	if unescaped != `cat\` {
		t.Error("malformed string")
	}
}

func TestTokenSegmentToString(t *testing.T) {
	str := TokenSegmentToString(nil)
	if str != "" {
		t.Error("nil segment")
	}

	str = TokenSegmentToString(TokenSegment{})
	if str != "" {
		t.Error("empty segment")
	}

	str = TokenSegmentToString([]byte("cat"))
	if str != "cat" {
		t.Error("non-empty segment")
	}

	str = TokenSegmentToString([]byte(`cat/dog`))
	if str != `cat\sdog` {
		t.Error("slash escaped segment")
	}
}

func TestTokenPath(t *testing.T) {
	tokenPath := MakeTokenPath()
	if tokenPath != "" {
		t.Error("empty path")
	}

	parts := SplitTokenPath(tokenPath)
	if parts == nil || len(parts) != 0 {
		t.Error("empty path split")
	}

	tokenPath = MakeTokenPath("")
	if tokenPath != "/" {
		t.Error("empty segment path")
	}

	parts = SplitTokenPath(tokenPath)
	if parts == nil || len(parts) != 1 || parts[0] != "" {
		t.Error("empty segment path split")
	}

	tokenPath = MakeTokenPath("cat")
	if tokenPath != "/cat" {
		t.Error("simple path")
	}

	parts = SplitTokenPath(tokenPath)
	if parts == nil || len(parts) != 1 || parts[0] != "cat" {
		t.Error("simple path split")
	}

	tokenPath = MakeTokenPath("cat/dog")
	if tokenPath != `/cat\sdog` {
		t.Error("single escape path")
	}

	parts = SplitTokenPath(tokenPath)
	if parts == nil || len(parts) != 1 || parts[0] != `cat/dog` {
		t.Error("single escape split")
	}

	tokenPath = MakeTokenPath("cat/dog", "fox")
	if tokenPath != `/cat\sdog/fox` {
		t.Error("two token path")
	}

	parts = SplitTokenPath(tokenPath)
	if parts == nil || len(parts) != 2 || parts[0] != `cat/dog` || parts[1] != `fox` {
		t.Error("two token split")
	}
}

func TestTokenSet(t *testing.T) {
	tokens := TokenPathToTokenSet("")
	if tokens == nil || len(tokens) != 0 {
		t.Error("nil token path")
	}

	tokenPath := TokenSetToTokenPath(tokens)
	if tokenPath != "" {
		t.Error("nil token set")
	}

	tokens = TokenPathToTokenSet("/")
	if tokens == nil || len(tokens) != 1 || !bytes.Equal(tokens[0], []byte("")) {
		t.Error("empty token path")
	}

	tokenPath = TokenSetToTokenPath(tokens)
	if tokenPath != "/" {
		t.Error("empty token set")
	}

	tokens = TokenPathToTokenSet("foo")
	if tokens == nil || len(tokens) != 1 {
		t.Error("token path without slash")
	}

	tokenPath = TokenSetToTokenPath(tokens)
	if tokenPath != "/foo" {
		t.Error("normalized token path")
	}

	tokens = TokenPathToTokenSet("//")
	if tokens == nil || len(tokens) != 2 || !bytes.Equal(tokens[0], []byte("")) || !bytes.Equal(tokens[1], []byte("")) {
		t.Error("two empty token path segments")
	}

	tokenPath = TokenSetToTokenPath(tokens)
	if tokenPath != "//" {
		t.Error("two empty token set segments")
	}

	tokens = TokenPathToTokenSet(`/cat\Sdog`)
	if tokens == nil || len(tokens) != 1 || !bytes.Equal(tokens[0], []byte(`cat\dog`)) {
		t.Error("one segment path")
	}

	tokenPath = TokenSetToTokenPath(tokens)
	if tokenPath != `/cat\Sdog` {
		t.Error("one segment set")
	}
}

func TestTokenSetWrappers(t *testing.T) {
	sk := MakeStoreKey("cow", "mouse", "pig")
	if sk.Path != "/cow/mouse/pig" {
		t.Error("convenience make token path")
	}

	ts := sk.Tokens
	if ts == nil || len(ts) != 3 || !bytes.Equal(ts[0], []byte("cow")) || !bytes.Equal(ts[1], []byte("mouse")) || !bytes.Equal(ts[2], []byte("pig")) {
		t.Error("convenience make token set")
	}

	parts := SplitStoreKey(sk)
	if parts == nil || len(parts) != 3 || parts[0] != "cow" || parts[1] != "mouse" || parts[2] != "pig" {
		t.Error("convenience split token set")
	}

	sk2 := MakeStoreKeyFromPath("/cow/mouse/pig")
	if sk2.Path != "/cow/mouse/pig" {
		t.Error("convenience make sk from path")
	}
	ts = sk2.Tokens
	if ts == nil || len(ts) != 3 || !bytes.Equal(ts[0], []byte("cow")) || !bytes.Equal(ts[1], []byte("mouse")) || !bytes.Equal(ts[2], []byte("pig")) {
		t.Error("convenience make token set from path")
	}

	sk3 := MakeStoreKeyFromTokenSegments(ts...)
	if sk3.Path != "/cow/mouse/pig" {
		t.Error("convenience make sk from token segments")
	}
	ts = sk3.Tokens
	if ts == nil || len(ts) != 3 || !bytes.Equal(ts[0], []byte("cow")) || !bytes.Equal(ts[1], []byte("mouse")) || !bytes.Equal(ts[2], []byte("pig")) {
		t.Error("convenience make token set from segments")
	}
}

func TestIsPattern(t *testing.T) {
	sample := "cat dog fox mouse cow"

	if isPattern("", sample) {
		t.Error("empty string matches nothing")
	}

	if isPattern("cat", sample) {
		t.Error("partial match is not a match")
	}

	if !isPattern("*", sample) {
		t.Error("wildcard matches all")
	}

	if !isPattern("**", sample) {
		t.Error("extra wildcards match")
	}

	if !isPattern("cat*", sample) {
		t.Error("wildcard prefix matches")
	}

	if !isPattern("*cat*", sample) {
		t.Error("wildcard can match nothing")
	}

	if !isPattern("*cat dog fox mouse cow*", sample) {
		t.Error("wildcard can match nothing 2")
	}

	if !isPattern("cat*cow", sample) {
		t.Error("mid match")
	}

	if !isPattern("cat*ox*cow", sample) {
		t.Error("mid match recursive with multiple matches")
	}

	if isPattern("cat*o*z*cow", sample) {
		t.Error("mid match recursive with multiple mismatches")
	}

	if !isPattern("cat dog fox mouse cow", sample) {
		t.Error("exact match")
	}

	if isPattern("cat dog fox mouse cows", sample) {
		t.Error("exact match plus extra is mismatch")
	}
}

func TestAppendStoreKeySegments(t *testing.T) {
	sk := MakeStoreKey("test")
	sk2 := AppendStoreKeySegments(sk)
	if sk.Path != sk2.Path {
		t.Error("empty fail")
	}

	sk2 = AppendStoreKeySegments(sk, TokenSegment([]byte("fox")))
	if sk2.Path != "/test/fox" {
		t.Error("one fail")
	}

	sk2 = AppendStoreKeySegments(sk, TokenSegment([]byte("fox")), TokenSegment([]byte("cow")))
	if sk2.Path != "/test/fox/cow" {
		t.Error("two fail")
	}
}

func TestAppendStoreKeySegmentStrings(t *testing.T) {
	sk := MakeStoreKey("test")
	sk2 := AppendStoreKeySegmentStrings(sk)
	if sk.Path != sk2.Path {
		t.Error("empty fail")
	}

	sk2 = AppendStoreKeySegmentStrings(sk, "fox")
	if sk2.Path != "/test/fox" {
		t.Error("one fail")
	}

	sk2 = AppendStoreKeySegmentStrings(sk, "fox", "cow")
	if sk2.Path != "/test/fox/cow" {
		t.Error("two fail")
	}
}