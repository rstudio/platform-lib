package file

// Copyright (C) 2024 by RStudio, PBC.

import "errors"

func Statfs(path string) (*StatfsData, error) {
	return nil, errors.New("Statfs is not supported on Windows")
}
