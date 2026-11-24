// Package sysinfo reports disk figures for heartbeats and upload admission.
package sysinfo

import "syscall"

// Disk returns total and unprivileged-available bytes of the filesystem holding path.
func Disk(path string) (total, free int64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(path, &st); err != nil {
		return
	}
	bs := st.Bsize
	return int64(st.Blocks) * bs, int64(st.Bavail) * bs, nil //nolint:gosec // block counts fit in int64
}

// FreeBytes returns the free bytes of the filesystem holding path.
func FreeBytes(path string) (int64, error) {
	_, f, err := Disk(path)
	return f, err
}
