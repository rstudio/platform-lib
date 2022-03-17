package listener

// Copyright (C) 2022 by RStudio, PBC.

import (
	"net"
)

type IPReporter interface {
	IP() string
}

type IPCache struct {
	reporter    IPReporter
	cachedValue string
}

// NewIPCache creates a new IPCache.
func NewIPCache(iprep IPReporter) *IPCache {
	return &IPCache{
		reporter: iprep,
	}
}

// Note that this also implements the `IPReporter` interface, so there's really no need
// to have a separate interface for the cache.
func (c *IPCache) IP() string {
	gotIP := c.reporter.IP()
	if gotIP != c.cachedValue {
		// enumerate IPs and determine if cached value is still valid
		if !c.stillValidIP(c.cachedValue) {
			c.cachedValue = gotIP
		}
	}

	return c.cachedValue
}

// Checks if IP is still valid
func (p *IPCache) stillValidIP(IP string) bool {
	var ip net.IP
	if ip = net.ParseIP(IP); ip == nil {
		return false
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, addr := range addrs {
		switch v := addr.(type) {
		case *net.IPNet:
			if v.IP.Equal(ip) {
				return true
			}
		case *net.IPAddr: // only on Windows
			if v.IP.Equal(ip) {
				return true
			}
		}
	}

	return false
}
