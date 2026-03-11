package archivepath

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var splitPartPattern = regexp.MustCompile(`(?i)^(.*)\.part([0-9]+)(\..*)?$`)

// SplitInfo describes one archive path or key that already contains a part number.
type SplitInfo struct {
	DirPrefix string
	Stem      string
	Suffix    string
	Part      int
	Width     int
}

// ParseSplit reports whether name uses the reserved `.partNNNN` archive naming form.
func ParseSplit(name string) (SplitInfo, bool) {
	dirPrefix, base := splitPath(name)
	match := splitPartPattern.FindStringSubmatch(base)
	if match == nil {
		return SplitInfo{}, false
	}
	part, err := strconv.Atoi(match[2])
	if err != nil {
		return SplitInfo{}, false
	}
	return SplitInfo{
		DirPrefix: dirPrefix,
		Stem:      match[1],
		Suffix:    match[3],
		Part:      part,
		Width:     len(match[2]),
	}, true
}

// FormatSplit inserts a `.partNNNN` segment before the full archive suffix.
func FormatSplit(name string, part int, width int) string {
	if width < 4 {
		width = 4
	}
	dirPrefix, base := splitPath(name)
	root, suffix := splitArchiveBase(base)
	return fmt.Sprintf("%s%s.part%0*d%s", dirPrefix, root, width, part, suffix)
}

// MatchSplit reports whether candidate belongs to the same split archive group.
func MatchSplit(candidate string, want SplitInfo) (SplitInfo, bool) {
	got, ok := ParseSplit(candidate)
	if !ok {
		return SplitInfo{}, false
	}
	if got.DirPrefix != want.DirPrefix || got.Stem != want.Stem || got.Suffix != want.Suffix {
		return SplitInfo{}, false
	}
	return got, true
}

func splitPath(name string) (string, string) {
	index := strings.LastIndexAny(name, `/\`)
	if index < 0 {
		return "", name
	}
	return name[:index+1], name[index+1:]
}

func splitArchiveBase(name string) (string, string) {
	ext := filepath.Ext(name)
	if ext == name {
		return name, ""
	}
	if strings.HasSuffix(name, ".tar"+ext) {
		ext = ".tar" + ext
	}
	return strings.TrimSuffix(name, ext), ext
}
