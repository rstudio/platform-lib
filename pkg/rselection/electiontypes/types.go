package electiontypes

// Copyright (C) 2025 by Posit Software, PBC

import (
	"time"

	"github.com/google/uuid"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
)

type AssumeLeader struct {
	SrcAddr string
}

// ClusterNode tracks the latest ping response time for a node
type ClusterNode struct {
	Name   string
	IP     string
	Ping   time.Time
	Leader bool
}

func (c *ClusterNode) Key() string {
	return c.Name + "_" + c.IP
}

type ClusterNotification struct {
	GuidVal     string
	MessageType uint8
	SrcAddr     string
	DstAddr     string
}

const (
	ClusterMessageTypePing          uint8 = 255
	ClusterMessageTypePingResponse  uint8 = 250
	ClusterMessageTypeNodes         uint8 = 252
	ClusterMessageTypeNodesResponse uint8 = 251
)

type ClusterPingRequest struct {
	ClusterNotification
}

func NewClusterPingRequest(srcAddress, dstAddress string) *ClusterPingRequest {
	return &ClusterPingRequest{
		ClusterNotification: ClusterNotification{
			GuidVal:     uuid.New().String(),
			MessageType: ClusterMessageTypePing,
			SrcAddr:     srcAddress,
			DstAddr:     dstAddress,
		},
	}
}

type ClusterPingResponse struct {
	ClusterNotification
	IP string
}

func NewClusterPingResponse(srcAddress, dstAddress, ip string) *ClusterPingResponse {
	return &ClusterPingResponse{
		ClusterNotification: ClusterNotification{
			GuidVal:     uuid.New().String(),
			MessageType: ClusterMessageTypePingResponse,
			SrcAddr:     srcAddress,
			DstAddr:     dstAddress,
		},
		IP: ip,
	}
}

func (c *ClusterPingResponse) Key() string {
	return c.SrcAddr + "_" + c.IP
}

func (n *ClusterNotification) Type() uint8 {
	return n.MessageType
}

func (n *ClusterNotification) Guid() string {
	return n.GuidVal
}

func (n *ClusterNotification) Data() interface{} {
	return nil
}

type ClusterNodesRequest struct {
	ClusterNotification
}

func NewClusterNodesRequest() *ClusterNodesRequest {
	return &ClusterNodesRequest{
		ClusterNotification: ClusterNotification{
			GuidVal:     uuid.New().String(),
			MessageType: ClusterMessageTypeNodes,
		},
	}
}

// ClusterNodesNotification is a notification that indicates nodes in the cluster
type ClusterNodesNotification struct {
	listener.GenericNotification
	Nodes []ClusterNode `json:"nodes"`
}

func NewClusterNodesNotification(nodes []ClusterNode, guid string) *ClusterNodesNotification {
	return &ClusterNodesNotification{
		GenericNotification: listener.GenericNotification{
			NotifyGuid: guid,
			NotifyType: ClusterMessageTypeNodesResponse,
		},
		Nodes: nodes,
	}
}
