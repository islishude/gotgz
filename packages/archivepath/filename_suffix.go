package archivepath

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// AddSuffix appends a custom/date suffix before the archive extension.
func AddSuffix(fileName, suffix string) string {
	return AddSuffixAt(fileName, suffix, time.Now())
}

// AddSuffixAt appends a custom/date suffix using now to resolve the built-in
// date value. Supplying the clock makes create target resolution deterministic
// across parsing and execution.
func AddSuffixAt(fileName, suffix string, now time.Time) string {
	if suffix == "" {
		return fileName
	}
	if suffix == "date" {
		suffix = now.Format("20060102")
	}

	ext := filepath.Ext(fileName)
	if ext == filepath.Base(fileName) {
		ext = ""
	}
	dir := filepath.Dir(fileName)
	if strings.HasSuffix(fileName, ".tar"+ext) {
		ext = ".tar" + ext
	}
	file := strings.TrimSuffix(filepath.Base(fileName), ext)
	file = fmt.Sprintf("%s-%s%s", file, suffix, ext)
	return filepath.Join(dir, file)
}
