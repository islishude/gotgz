package archiveutil

import "maps"

// MergeMetadata copies metadata maps with overlay keys taking precedence.
func MergeMetadata(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(overlay))
	maps.Copy(out, base)
	maps.Copy(out, overlay)
	return out
}
