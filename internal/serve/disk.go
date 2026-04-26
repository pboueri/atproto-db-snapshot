package serve

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// diskInfo is a per-mount snapshot.
type diskInfo struct {
	Mount      string `json:"mount"`
	TotalBytes uint64 `json:"total_bytes"`
	FreeBytes  uint64 `json:"free_bytes"`
	UsedBytes  uint64 `json:"used_bytes"`
}

// statfs returns the disk info for the filesystem hosting `path`.
func statfs(path string) (diskInfo, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return diskInfo{}, err
	}
	var st unix.Statfs_t
	if err := unix.Statfs(abs, &st); err != nil {
		return diskInfo{}, err
	}
	bsize := uint64(st.Bsize)
	total := uint64(st.Blocks) * bsize
	// Bavail is "available to non-root", which is what users care about.
	free := uint64(st.Bavail) * bsize
	used := total - free
	return diskInfo{
		Mount:      abs,
		TotalBytes: total,
		FreeBytes:  free,
		UsedBytes:  used,
	}, nil
}
