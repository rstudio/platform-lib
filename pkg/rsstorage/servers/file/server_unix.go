//go:build !windows

// Copyright (C) 2024 by RStudio, PBC.

package file

import (
	"syscall"
)

func Statfs(path string) (*StatfsData, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return nil, err
	}
	return &StatfsData{
		Bsize:  fs.Bsize,
		Blocks: fs.Blocks,
		Bfree:  fs.Bfree,
	}, nil
}
