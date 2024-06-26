package listener

// Copyright (C) 2022 by RStudio, PBC.

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

type TestIPReporter struct {
	Ip string
}

func (l *TestIPReporter) IP() string {
	return l.Ip
}
