package pqlistener

// Copyright (C) 2022 by RStudio, PBC.

func NewPqListenerWithIP(ip string) *PqListener {
	return &PqListener{
		ip: ip,
	}
}
