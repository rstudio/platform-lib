package listener

// Copyright (C) 2022 by Posit, PBC.

import (
	"gopkg.in/check.v1"
)

type IPReporterSuite struct{}

var _ = check.Suite(&IPReporterSuite{})

func (s *IPReporterSuite) TestNewIPCache(c *check.C) {
	ipRep := &TestIPReporter{}
	result := NewIPCache(ipRep)
	c.Check(result, check.NotNil)
}

func (s *IPReporterSuite) TestIP(c *check.C) {
	cases := []struct {
		ip       string
		expected string
	}{
		{
			ip:       "127.0.0.1",
			expected: "127.0.0.1",
		},
		{
			ip:       "172.16.0.0",
			expected: "127.0.0.1",
		},
		{
			ip:       "127.0.0.9",
			expected: "127.0.0.1",
		},
	}

	ipRep := &TestIPReporter{}
	p := &IPCache{
		reporter: ipRep,
	}
	for _, i := range cases {
		ipRep.Ip = i.ip
		c.Check(p.IP(), check.Equals, i.expected)
	}
}

func (s *IPReporterSuite) TestStillValidIP(c *check.C) {
	p := &IPCache{}

	tests := []struct {
		ip string

		want bool
	}{
		{ip: "", want: false},
		{ip: "0.0.0.0", want: false},
		{ip: "1.1.1.1", want: false},
		{ip: "127.0.0.1", want: true},
		{ip: "192.168.0.1", want: false},
		{ip: "255.255.255.255", want: false},
	}

	for _, test := range tests {
		c.Check(p.stillValidIP(test.ip), check.Equals, test.want)
	}
}
