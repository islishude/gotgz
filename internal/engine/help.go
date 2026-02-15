package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

func AddTarSuffix(fileName, suffix string) string {
	if suffix == "" {
		return fileName
	}
	ext := filepath.Ext(fileName)
	// don't add suffix if the file is a hidden name
	if ext == fileName {
		return fileName
	}
	dir := filepath.Dir(fileName)
	if strings.HasSuffix(fileName, ".tar"+ext) {
		ext = ".tar" + ext
	}
	file := strings.TrimSuffix(filepath.Base(fileName), ext)
	switch suffix {
	case "date":
		file = fmt.Sprintf("%s-%s%s", file, time.Now().Format("20060102"), ext)
	default:
		file = fmt.Sprintf("%s-%s%s", file, suffix, ext)
	}
	return filepath.Join(dir, file)
}
