package treestore

import (
	"fmt"
	"strings"
)

type (
	SubPathSegment []byte
	SubPath        []SubPathSegment
	EscapedSubPath string
)

// Utility to convert a plain text segment into an escaped string.
// If the segment is nil, \N is returned.
// If the segment is an empty string, \E is returned.
// If the segment contains slash, backslash, tildem carot or control characters,
// they are converted into a backslash-escaped sequence.
func EscapeSubPathText(plainSeg *string) string {
	if plainSeg == nil {
		return `\N`
	}

	if *plainSeg == "" {
		return `\E`
	}

	var sb strings.Builder

	for _, ch := range *plainSeg {
		if ch == '/' {
			sb.WriteString(`\s`)
		} else if ch == '\\' {
			sb.WriteString(`\S`)
		} else if ch < 32 {
			sb.WriteString(fmt.Sprintf("\\x%02X", ch))
		} else {
			sb.WriteRune(ch)
		}
	}

	return sb.String()
}

// Utility to convert an escaped string to a plain text segment.
func UnescapeSubPathText(escapedSeg string) *string {
	if escapedSeg == `\N` {
		return nil
	}

	if escapedSeg == `\E` {
		str := ""
		return &str
	}

	runes := []rune(escapedSeg)
	var sb strings.Builder
	for pos := 0; pos < len(runes); pos++ {
		ch := runes[pos]
		if ch == '\\' && pos+1 < len(runes) {
			ch2 := runes[pos+1]
			if ch2 == 's' {
				sb.WriteRune('/')
				pos++
			} else if ch2 == 'S' {
				sb.WriteRune('\\')
				pos++
			} else if ch2 == 'x' && pos+3 < len(runes) {
				n := hexToByte(runes[pos+2], runes[pos+3])
				if n < 0 {
					sb.WriteRune(ch)
				} else {
					sb.WriteRune(rune(n))
					pos += 3
				}
			} else {
				sb.WriteRune(ch)
			}
		} else {
			sb.WriteRune(ch)
		}
	}

	str := sb.String()
	return &str
}

// Constructs a subpath from a slice of unescaped strings.
func EscapeSubPathStrings(parts ...*string) EscapedSubPath {
	var sb strings.Builder
	for i, part := range parts {
		if i > 0 {
			sb.WriteRune('/')
		}
		sb.WriteString(EscapeSubPathText(part))
	}
	return EscapedSubPath(sb.String())
}

// Deconstructs an escaped subpath into a slice of unescaped strings.
func SplitSubPath(path EscapedSubPath) []*string {
	if path == "" {
		return []*string{}
	}

	parts := strings.Split(string(path[1:]), "/")
	result := make([]*string, len(parts))

	for index, part := range parts {
		result[index] = UnescapeSubPathText(part)
	}

	return result
}

// Converts a subpath segment to an escaped string.
func EscapeSubPathSegment(segment SubPathSegment) string {
	var strSeg *string
	if segment != nil {
		str := string(segment)
		strSeg = &str
	}
	return EscapeSubPathText(strSeg)
}

// Converts an escaped string to a subpath segment.
func UnescapeSubPathSegment(escapedSeg string) SubPathSegment {
	plainSeg := UnescapeSubPathText(escapedSeg)
	if plainSeg == nil {
		return nil
	} else {
		return SubPathSegment(*plainSeg)
	}
}

// Adds a subpath segment to an existing path
func AppendSubPathSegment(path SubPath, segment SubPathSegment) SubPath {
	return append(path, segment)
}

// Adds an escaped subpath string to an existing path
func AppendSubPathSegmentString(path SubPath, escapedSeg string) SubPath {
	return append(path, UnescapeSubPathSegment(escapedSeg))
}

// Converts an escaped subpath to a subpath segment array.
func UnescapeSubPath(escapedPath EscapedSubPath) SubPath {
	if escapedPath == "" {
		return SubPath{}
	}

	parts := strings.Split(string(escapedPath), "/")
	segments := make(SubPath, len(parts))

	for index, part := range parts {
		segments[index] = UnescapeSubPathSegment(part)
	}

	return segments
}

// Converts a subpath segment array to an escaped subpath.
func EscapeSubPath(segments SubPath) EscapedSubPath {
	if len(segments) == 0 {
		return ""
	}

	parts := make([]string, len(segments))

	for index, seg := range segments {
		parts[index] = EscapeSubPathSegment(seg)
	}

	return EscapedSubPath(strings.Join(parts, "/"))
}

// Makes a subpath from individual escaped segment strings. Specify \N for a nil segment.
// See EscapeSubPathText for full escaping rules.
func MakeSubPath(escapedSegs ...string) SubPath {
	path := make([]SubPathSegment, 0, len(escapedSegs))

	for _, escapedSeg := range escapedSegs {
		path = append(path, UnescapeSubPathSegment(escapedSeg))
	}

	return path
}

func JoinSubPath(sk StoreKey, subpath SubPath) StoreKey {
	tokens := make([]TokenSegment, 0, len(sk.Tokens)+len(subpath))
	tokens = append(tokens, sk.Tokens...)
	for _, seg := range subpath {
		tokens = append(tokens, TokenSegment(seg))
	}

	return StoreKey{
		Path:   TokenSetToTokenPath(tokens),
		Tokens: tokens,
	}
}
