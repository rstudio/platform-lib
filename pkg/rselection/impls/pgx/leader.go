package pgxelection

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/rstudio/platform-lib/v2/pkg/rselection"
	"github.com/rstudio/platform-lib/v2/pkg/rselection/electiontypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
)

// ClusterPgStore provides an interface that is used to enumerate a secondary
// list of nodes. Typically, this list comes from a database table and can be used
// to verify that a cluster is healthy.
type ClusterPgStore interface {
	Nodes() (map[string]*electiontypes.ClusterNode, error)
}

type PgxLeader struct {
	store       ClusterPgStore
	awb         broadcaster.Broadcaster
	notify      PgNotifier
	taskHandler rselection.TaskHandler

	// The channel for leader/follower notifications
	chLeader   string
	chFollower string

	// The channel for other messages
	chMessages string

	// The address of this node
	address string

	// Used to shut down this service
	stop chan bool

	// Timeouts
	ping       time.Duration // Interval between pings
	sweep      time.Duration // Interval between node sweeps
	maxPingAge time.Duration // Max ping age before sweeping

	pingSuccess     bool
	pingSuccessLock sync.RWMutex

	// Stores information about active nodes and ping times
	nodes map[string]*electiontypes.ClusterNode
	mutex sync.RWMutex

	// Used for testing
	loopAwareChTEST    chan bool
	pingResponseChTEST chan bool
}

type PgxLeaderConfig struct {
	Store           ClusterPgStore
	Broadcaster     broadcaster.Broadcaster
	Notifier        PgNotifier
	TaskHandler     rselection.TaskHandler
	LeaderChannel   string
	FollowerChannel string
	MessagesChannel string
	Address         string
	StopChan        chan bool
	PingInterval    time.Duration
	SweepInterval   time.Duration
	MaxPingAge      time.Duration
}

func NewPgxLeader(cfg PgxLeaderConfig) *PgxLeader {
	return &PgxLeader{
		store:       cfg.Store,
		awb:         cfg.Broadcaster,
		notify:      cfg.Notifier,
		taskHandler: cfg.TaskHandler,
		chLeader:    cfg.LeaderChannel,
		chFollower:  cfg.FollowerChannel,
		chMessages:  cfg.MessagesChannel,
		address:     cfg.Address,
		stop:        cfg.StopChan,
		ping:        cfg.PingInterval,
		sweep:       cfg.SweepInterval,
		maxPingAge:  cfg.MaxPingAge,
		mutex:       sync.RWMutex{},
	}
}

func (p *PgxLeader) Lead(ctx context.Context) error {
	// Initialize list of nodes
	p.nodes = make(map[string]*electiontypes.ClusterNode)
	nodes, err := p.store.Nodes()
	if err != nil {
		return err
	}

	for _, node := range nodes {
		p.nodes[node.Key()] = node
	}

	pingTicker := time.NewTicker(p.ping)
	sweepTicker := time.NewTicker(p.sweep)

	defer pingTicker.Stop()
	defer sweepTicker.Stop()

	// Start handling leader tasks
	go p.taskHandler.Handle(p.awb)
	defer p.taskHandler.Stop()

	p.lead(ctx, pingTicker.C, sweepTicker.C, p.stop)
	return nil
}

func (p *PgxLeader) lead(ctx context.Context, pingTick, sweepTick <-chan time.Time, stop chan bool) {

	// Listen for ping responses from other nodes
	l := p.awb.Subscribe(electiontypes.ClusterMessageTypePingResponse)
	defer p.awb.Unsubscribe(l)

	// Listen for other ping requests. If an unexpected ping request is received,
	// we must demote the leader since there's another leader.
	leaderPings := p.awb.Subscribe(electiontypes.ClusterMessageTypePing)
	defer p.awb.Unsubscribe(leaderPings)

	// Listen for requests to enumerate nodes in the cluster.
	nodesCh := p.awb.Subscribe(electiontypes.ClusterMessageTypeNodes)
	defer p.awb.Unsubscribe(nodesCh)

	logTicker := time.NewTicker(time.Second * 10)
	defer logTicker.Stop()
	logTickCh := logTicker.C

	// Send a ping synchronously to ensure other nodes know about the new leader
	p.pingNodes(ctx)

	for {

		// In test, we sometimes wait for a loop to ensure that data is flushed
		// before checking values.
		if p.loopAwareChTEST != nil {
			select {
			case p.loopAwareChTEST <- true:
			default:
			}
		}

		select {
		case <-stop:
			return
		case n := <-l:
			if cn, ok := n.(*electiontypes.ClusterPingResponse); ok {
				go p.handlePingResponse(cn)
			}
		case n := <-leaderPings:
			if cn, ok := n.(*electiontypes.ClusterPingRequest); ok {
				go p.handleLeaderPing(cn)
			}
		case n := <-nodesCh:
			// This supports receiving a request to enumerate cluster nodes. The leader
			// responds with a list of nodes on the generic messaging channel.
			if cn, ok := n.(*electiontypes.ClusterNodesRequest); ok {
				go p.handleNodesRequest(ctx, cn)
			}
		case <-pingTick:
			go p.pingNodes(ctx)
		case <-sweepTick:
			p.sweepNodes()
		case vCh := <-p.taskHandler.Verify():
			p.verify(vCh)
		case <-logTickCh:
			slog.Debug(fmt.Sprintf(p.info()))
		}
	}
}

func (p *PgxLeader) info() string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	if len(p.nodes) == 0 {
		return "No cluster nodes recorded by leader"
	}
	nodes := make([]string, 0)
	for _, n := range p.nodes {
		leadCh := " "
		if n.Name == p.address {
			leadCh = "*"
		}
		nodes = append(nodes, fmt.Sprintf("%s (%s)%s", n.Name, n.IP, leadCh))
	}
	sort.Strings(nodes)
	return fmt.Sprintf("Cluster nodes:\n  %s", strings.Join(nodes, "\n  "))
}

// verify ensures that the in-memory node list matches the store list. We do
// this to ensure that the cluster is healthy before running scheduled tasks.
// This helps to prevent split-brain issues in the cluster.
//
//  1. The task handler is ready to run a scheduled task.
//  2. The task handler sends a `chan bool` to its verify channel.
//  3. We receive the `chan bool` here and:
//     a. Verify that the cluster is healthy.
//     b. Respond with `true` over the channel if the cluster is healthy.
//  4. The task handler runs the scheduled task when the cluster is healthy.
func (p *PgxLeader) verify(vCh chan bool) {
	var err error

	// Upon exit, notify channel with `true` or `false` depending upon error status
	defer func(err *error) {
		if *err != nil {
			log.Printf("Error verifying cluster integrity: %s", *err)
			vCh <- false
		} else {
			vCh <- true
		}
	}(&err)

	if p.unsuccessfulPing() {
		err = fmt.Errorf("error pinging follower nodes")
		return
	}

	// Enumerate nodes recorded in database
	nodes, err := p.store.Nodes()
	if err != nil {
		err = fmt.Errorf("error retrieving cluster node list for verification: %s", err)
		return
	}

	// Ensure in-memory node list length matches store
	if len(nodes) != len(p.nodes) {
		err = fmt.Errorf("node list length differs. Store node count %d does not match leader count %d", len(nodes), len(p.nodes))
		return
	}

	// Ensure in-memory node list matches store list
	for _, node := range nodes {
		if _, ok := p.nodes[node.Key()]; !ok {
			err = fmt.Errorf("node %s with IP %s from store not known by leader", node.Name, node.IP)
			return
		}
	}
}

// pingNodes sends a ping request out on the follower channel. All online cluster
// nodes should receive this message and respond with a ping response.
func (p *PgxLeader) pingNodes(ctx context.Context) {
	req := &electiontypes.ClusterNotification{
		GuidVal:     uuid.New().String(),
		MessageType: electiontypes.ClusterMessageTypePing,
		SrcAddr:     p.address,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Printf("Error marshaling notification to JSON: %s", err)
		return
	}

	p.pingSuccessLock.Lock()
	defer p.pingSuccessLock.Unlock()
	p.pingSuccess = true

	slog.Log(ctx, LevelTrace, fmt.Sprintf("Leader pinging nodes on follower channel %s", p.chFollower))
	err = p.notify.Notify(ctx, p.chFollower, b)
	if err != nil {
		p.pingSuccess = false
		log.Printf("Leader error pinging followers: %s", err)
		return
	}

	// This will ensure that the leader is tracked as part of the nodes in the cluster, and it will force
	// duplicate leaders to surrender the position.
	slog.Log(ctx, LevelTrace, fmt.Sprintf("Leader pinging itself on leader channel %s", p.chLeader))
	err = p.notify.Notify(ctx, p.chLeader, b)
	if err != nil {
		p.pingSuccess = false
		log.Printf("Leader error pinging leaders: %s", err)
		return
	}
}

func (p *PgxLeader) handleNodesRequest(ctx context.Context, cn *electiontypes.ClusterNodesRequest) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	slog.Log(ctx, LevelTrace, fmt.Sprintf("Leader received request for nodes"))
	nodes := make([]electiontypes.ClusterNode, 0)
	for _, node := range p.nodes {
		leader := node.Name == p.address && node.IP == p.awb.IP()
		nodes = append(nodes, electiontypes.ClusterNode{
			Name:   node.Name,
			IP:     node.IP,
			Leader: leader,
		})
	}

	// Create and marshal a response
	resp := electiontypes.NewClusterNodesNotification(nodes, cn.Guid())
	b, err := json.Marshal(resp)
	if err != nil {
		log.Printf("Error marshaling notification to JSON: %s", err)
		return
	}

	// Broadcast the response on the generic channel
	err = p.notify.Notify(ctx, p.chMessages, b)
	if err != nil {
		log.Printf("Leader error notifying of cluster nodes: %s", err)
		return
	}
}

func (p *PgxLeader) handleLeaderPing(cn *electiontypes.ClusterPingRequest) {
	// If we received a ping from another leader, then stop leading
	if cn.SrcAddr != p.address {
		slog.Debug(fmt.Sprintf("Leader received ping from another leader. Stopping and moving back to the follower loop."))
		p.stop <- true
	} else {
		resp := electiontypes.NewClusterPingResponse(p.address, cn.SrcAddr, p.awb.IP())
		p.handlePingResponse(resp)
	}
}

// handlePingResponse handles ping responses received on the leader channel.
func (p *PgxLeader) handlePingResponse(cn *electiontypes.ClusterPingResponse) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	slog.Log(context.Background(), LevelTrace, fmt.Sprintf("Leader received ping response from %s", cn.SrcAddr))
	key := cn.Key()
	if _, ok := p.nodes[key]; !ok {
		p.nodes[key] = &electiontypes.ClusterNode{
			Name: cn.SrcAddr,
			IP:   cn.IP,
		}
	}
	p.nodes[key].Ping = time.Now()

	// For unit tests only
	if p.pingResponseChTEST != nil {
		select {
		case p.pingResponseChTEST <- true:
		default:
		}
	}
}

func (p *PgxLeader) Key() string {
	// Note that the Key() can become unreliable when the broadcaster.Broadcaster is experiencing network issues
	return p.address + "_" + p.awb.IP()
}

// sweepNodes periodically checks the in-memory node map and removes nodes that are
// no longer responding to ping requests.
func (p *PgxLeader) sweepNodes() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.unsuccessfulPing() {
		slog.Debug(fmt.Sprintf("Skipping cluster sweep due to unsuccessful pings"))
		return
	}

	for key, node := range p.nodes {
		// Ignore the leader node since we know it is always up
		if key == p.Key() {
			continue
		}

		if node.Ping.Before(time.Now().Add(-p.maxPingAge)) {
			slog.Debug(fmt.Sprintf("Leader sweep removing cluster node %s", key))
			delete(p.nodes, key)
		}
	}
}

func (p *PgxLeader) unsuccessfulPing() bool {
	p.pingSuccessLock.RLock()
	defer p.pingSuccessLock.RUnlock()
	return !p.pingSuccess
}
