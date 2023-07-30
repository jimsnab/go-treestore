package treestore

import (
	"encoding/binary"
	"strings"
	"time"
)

type (
	TokenSegment []byte
	TokenSet     []TokenSegment
	TokenPath    string

	StoreKey struct {
		path   TokenPath
		tokens TokenSet
	}
)

const nsPerSec = (1 /*sec*/ * 1000 /*ms*/ * 1000 /*us*/ * 1000 /*ns*/)

// escapes the forward slash to \s and the backslash to \S
func EscapeTokenString(plainText string) string {
	var sb strings.Builder

	for _, ch := range plainText {
		if ch == '/' {
			sb.WriteString(`\s`)
		} else if ch == '\\' {
			sb.WriteString(`\S`)
		} else {
			sb.WriteRune(ch)
		}
	}

	return sb.String()
}

// unescapes \s to the forward slash and \S to the the backslash
func UnescapeTokenString(tokenText string) string {
	result := strings.ReplaceAll(tokenText, `\s`, `/`)
	result = strings.ReplaceAll(result, `\S`, `\`)
	return result
}

// constructs a token path from a slice of unescaped strings
func MakeTokenPath(parts ...string) TokenPath {
	var sb strings.Builder
	for _, part := range parts {
		sb.WriteRune('/')
		sb.WriteString(EscapeTokenString(part))
	}
	return TokenPath(sb.String())
}

// deconstructs a token path into a slice of unescaped strings
func SplitTokenPath(tokenPath TokenPath) []string {
	if !strings.HasPrefix(string(tokenPath), "/") {
		return []string{}
	}

	parts := strings.Split(string(tokenPath[1:]), "/")
	result := make([]string, len(parts))

	for index, part := range parts {
		result[index] = UnescapeTokenString(part)
	}

	return result
}

// Converts a token segment to an escaped string.
func TokenSegmentToString(segment TokenSegment) string {
	return EscapeTokenString(string(segment))
}

// Converts a token path to a token set used in operations walking the database trees.
// A token path is a forward-slash separated list of escaped token strings.
// See `EscapeTokenString()` and `MakeTokenPath()`
func TokenPathToTokenSet(tokenPath TokenPath) TokenSet {
	if !strings.HasPrefix(string(tokenPath), "/") {
		return TokenSet{}
	}

	parts := strings.Split(string(tokenPath[1:]), "/")
	tokens := make(TokenSet, len(parts))

	for index, part := range parts {
		tokens[index] = TokenSegment(UnescapeTokenString(part))
	}

	return tokens
}

// Converts a token set obtained from a TreeStore to a token path.
// A token path is a forward-slash separated list of escaped token strings.
// The unescaped string segments can be obtained from `SplitTokenPath()`.
func TokenSetToTokenPath(tokens TokenSet) TokenPath {
	if len(tokens) == 0 {
		return ""
	}

	parts := make([]string, len(tokens))

	for index, token := range tokens {
		parts[index] = EscapeTokenString(string(token))
	}

	return TokenPath("/" + strings.Join(parts, "/"))
}

// Makes the structure needed to interact with the TreeStore
func MakeStoreKey(parts ...string) StoreKey {
	tokenPath := MakeTokenPath(parts...)
	tokenSet := TokenPathToTokenSet(tokenPath)

	return StoreKey{
		path:   tokenPath,
		tokens: tokenSet,
	}
}

// Decomposes the TreeStore key structure
func SplitStoreKey(sk StoreKey) []string {
	return SplitTokenPath(sk.path)
}

// Returns the Unix ns tick as a byte array
func currentTickBytes() []byte {
	now := time.Now().UTC().UnixNano()

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(now))

	return b
}

// Makes the byte array equivalent of the Unix ns tick
func tickBytes(tick int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(tick))
	return b
}

// Recovers Unix ns from a tick byte array
func unixNsFromBytes(tick []byte) int64 {
	return int64(binary.BigEndian.Uint64(tick))
}

// Returns a date/time struct from a Unix ns timestamp
func timestampFromUnixNs(tick int64) time.Time {
	return time.Unix(tick/nsPerSec, tick%nsPerSec)
}

// Constrains the length of a string
func stringTruncate(str string, maxChars int) string {
	runes := []rune(str)
	if len(runes) <= maxChars {
		return str
	}

	return string(runes[0:maxChars])
}

// Constrains the length of a string and cuts at a linebreak
func cleanString(str string, maxChars int) string {
	clean := strings.ReplaceAll(str, "\r", "")

	cutPoint := strings.Index(clean, "\n")
	if cutPoint >= 0 {
		clean = clean[0:cutPoint]
	}

	runes := []rune(clean)
	if len(runes) > maxChars {
		clean = string(runes[0:maxChars])
	}

	if clean != str {
		clean += "…"
	}

	return clean
}

func isPattern(pattern, candidate string) bool {
	return isPatternRunes([]rune(pattern), []rune(candidate))
}

func isPatternRunes(pattern, candidate []rune) bool {
	cpos := 0
	ppos := 0

	for {
		if ppos+2 <= len(pattern) && pattern[ppos] == '*' && pattern[ppos+1] == '*' {
			ppos++
		} else {
			break
		}
	}

	for {
		if ppos >= len(pattern) {
			break
		}
		if cpos >= len(candidate) {
			break
		}

		if pattern[ppos] == '*' {
			if ppos+1 >= len(pattern) {
				return true
			}
			for {
				if isPatternRunes(pattern[ppos+1:], candidate[cpos:]) {
					return true
				}
				cpos++
				if cpos >= len(candidate) {
					return false
				}
			}
		} else if pattern[ppos] != candidate[cpos] {
			return false
		}

		ppos++
		cpos++
	}

	if ppos == len(pattern)-1 && pattern[ppos] == '*' {
		return true
	}

	return (ppos == len(pattern) && cpos == len(candidate))
}
