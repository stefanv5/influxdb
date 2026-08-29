//go:build windows
// +build windows

package tsm1

import (
	"golang.org/x/sys/windows"
)

// installNoReplace renames oldname to newname, failing atomically when
// newname already exists: MoveFileEx without MOVEFILE_REPLACE_EXISTING never
// clobbers the target, so there is no Stat/Rename TOCTOU window. A target that
// already exists is reported as an error satisfying os.ErrExist
// (ERROR_FILE_EXISTS / ERROR_ALREADY_EXISTS); all other errors are returned
// as-is.
func installNoReplace(oldname, newname string) error {
	from, err := windows.UTF16PtrFromString(oldname)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(newname)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, 0)
}
