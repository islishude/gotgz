//go:build !linux

package engine

import "os"

func openRegularLocalEntry(record localCreateRecord) (*localEntryHandle, error) {
	info, err := os.Lstat(record.current)
	if err != nil {
		return nil, err
	}
	return newLocalEntryHandle(record, info, nil)
}
