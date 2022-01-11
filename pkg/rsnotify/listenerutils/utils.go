package listenerutils

/* utils.go
 *
 * Copyright (C) 2021 by RStudio, PBC
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property of
 * RStudio, PBC and its suppliers, if any. The intellectual and technical
 * concepts contained herein are proprietary to RStudio, PBC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and
 * are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained.
 */

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
