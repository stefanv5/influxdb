//go:build plan9
// +build plan9

package tsm1

// installNoReplace has no atomic no-replace rename on Plan 9; report the
// platform as unsupported so the caller falls back to its Stat-guarded rename
// sequence.
func installNoReplace(oldname, newname string) error {
	return errNoReplaceUnsupported
}
