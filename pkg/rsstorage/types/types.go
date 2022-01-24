package types

// Copyright (C) 2022 by RStudio, PBC

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
)

// Usage is a container with useful values to
// determine capacity and usage.
type Usage struct {
	SizeBytes       datasize.ByteSize
	FreeBytes       datasize.ByteSize
	UsedBytes       datasize.ByteSize
	CalculationTime time.Duration
}

func (u Usage) String() string {
	return fmt.Sprintf("Size: %s; Free: %s; Used: %s; Time: %s", u.SizeBytes, u.FreeBytes, u.UsedBytes, u.CalculationTime)
}

// ScaleSize will return the size value scaled to a specified unit.
// For example, this can be used to represent 200GB as 200.
func (u Usage) ScaleSize(unit datasize.ByteSize) datasize.ByteSize {
	return datasize.ByteSize(u.SizeBytes) / unit
}

// ScaleFree will return the size value scaled to a specified unit.
// For example, this can be used to represent 200GB as 200.
func (u Usage) ScaleFree(unit datasize.ByteSize) datasize.ByteSize {
	return datasize.ByteSize(u.FreeBytes) / unit
}

// ScaleUsed will return the size value scaled to a specified unit.
// For example, this can be used to represent 200GB as 200.
func (u Usage) ScaleUsed(unit datasize.ByteSize) datasize.ByteSize {
	return datasize.ByteSize(u.UsedBytes) / unit
}

// ChunkNotification that indicates a new chunk is ready. Used for notifying of
// new chunk availability while downloading chunked assets.
type ChunkNotification struct {
	Address string
	Chunk   uint64
	Timeout time.Duration
}
