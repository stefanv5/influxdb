//go:build !windows && !plan9
// +build !windows,!plan9

package tsm1

import (
	"errors"

	"golang.org/x/sys/unix"
)

// installNoReplace renames oldname to newname, failing atomically when
// newname already exists: the target is never replaced, so no concurrent
// creator can be clobbered and there is no Stat/Rename TOCTOU window.
//
// On Linux it uses renameat2 with RENAME_NOREPLACE. Where the syscall is
// unavailable (other Unix systems) or the kernel or filesystem does not
// support it, errNoReplaceUnsupported is returned so the caller can fall back
// to its Stat-guarded rename sequence. All other errors (EEXIST, ENOENT, ...)
// are returned as-is; a target that already exists is reported as an error
// satisfying os.ErrExist.
func installNoReplace(oldname, newname string) error {
	err := renameat2NoReplace(oldname, newname)
	if err == nil || errors.Is(err, errNoReplaceUnsupported) {
		return err
	}
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
		// Kernel or filesystem without renameat2/RENAME_NOREPLACE support.
		return errNoReplaceUnsupported
	}
	return err
}
