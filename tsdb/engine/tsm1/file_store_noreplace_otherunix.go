//go:build !linux && !windows && !plan9
// +build !linux,!windows,!plan9

package tsm1

// renameat2NoReplace has no portable equivalent outside Linux; report the
// platform as unsupported so the caller falls back to its Stat-guarded rename
// sequence.
func renameat2NoReplace(oldname, newname string) error {
	return errNoReplaceUnsupported
}
