package listener

import "net"

// Copyright (C) 2022 by RStudio, PBC.

type IPReporter interface {
	IP() string
}

type IPCache struct {
	reporter    IPReporter
	cachedValue string
}

// Note that this also implements the `IPReporter` interface, so there's really no need
// to have a separate interface for the cache.
func (c *IPCache) IP() string {
	gotIP := c.reporter.IP()
	if gotIP != c.cachedValue {
		// enumerate IPs and determine if cached value is still value
		if !c.stillValidIP(c.cachedValue) {
			c.cachedValue = gotIP
		}
	}

	return c.cachedValue
}

func (p *IPCache) stillValidIP(IP string) bool {
	var ip net.IP
	if ip = net.ParseIP(IP); ip == nil {
		return false
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return false
	}

	for _, inter := range interfaces {
		addrs, err := inter.Addrs()
		if err != nil {
			return false
		}

		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				if v.Contains(ip) {
					return true
				}
			case *net.IPAddr:
				if v.IP.Equal(ip) {
					return true
				}
			}
		}
	}

	return false
}
