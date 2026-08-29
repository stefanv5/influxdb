//go:build linux
// +build linux

package tsm1

import "golang.org/x/sys/unix"

// renameat2NoReplace performs the Linux renameat2 syscall with
// RENAME_NOREPLACE, which fails with EEXIST when newname already exists
// instead of replacing it.
func renameat2NoReplace(oldname, newname string) error {
	return unix.Renameat2(unix.AT_FDCWD, oldname, unix.AT_FDCWD, newname, unix.RENAME_NOREPLACE)
}
