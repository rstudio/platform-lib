package listener

/* listenertest.go
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

import "log"

type TestNotification struct {
	Val     string
	GuidVal string
}

func (t *TestNotification) Type() uint8 {
	return 1
}

func (t *TestNotification) Guid() string {
	return t.GuidVal
}

func (t *TestNotification) Data() interface{} {
	return t.Val
}

type TestLogger struct {
	enabled bool
}

func (l *TestLogger) Debugf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (l *TestLogger) Enabled() bool {
	return l.enabled
}
