package postgrespq

// Copyright (C) 2022 by Posit Software, PBC.

func NewPqListenerWithIP(ip string) *PqListener {
	return &PqListener{
		ip: ip,
	}
}
