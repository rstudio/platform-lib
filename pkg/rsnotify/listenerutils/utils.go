package listenerutils

// Copyright (C) 2022 by RStudio, PBC.

import (
	"crypto/md5"
	"encoding/hex"
)

const MaxChannelLen = 63

// Makes a channel name safe. The local listener supports a name of any
// size, but PostgreSQL enforces a max channel name size of 64 bytes.
func SafeChannelName(channel string) string {
	if len(channel) > MaxChannelLen {
		h := md5.New()
		_, err := h.Write([]byte(channel))
		if err != nil {
			// If there was a hashing error, just truncate
			channel = channel[0:MaxChannelLen]
		} else {
			channel = hex.EncodeToString(h.Sum(nil))
		}
	}
	return channel
}
