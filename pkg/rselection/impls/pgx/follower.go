package pgxelection

// Copyright (C) 2025 by Posit Software, PBC

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rselection/electiontypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
)

const LevelTrace = slog.Level(-8)

type FollowResult bool

const (
	FollowResultPromote FollowResult = true
	FollowResultStop    FollowResult = false
)

type Follower interface {
	Follow() FollowResult
	Promote()
}

// Queue is not provided by the election library. Implement this interface. When new leadership
// is needed, an `electiontypes.AssumeLeader` job will be pushed to the queue.
type Queue interface {
	Push(assumeLeader electiontypes.AssumeLeader) error
}

type PgxFollower struct {
	queue  Queue
	awb    broadcaster.Broadcaster
	notify PgNotifier

	// The channel for leader notifications
	chLeader string

	// The address of this node
	address string

	// Used to shut down this service
	stop chan bool

	// Used to promote to a leader
	promote chan bool

	// Timeouts
	timeout time.Duration

	// Track the last time we logged an error to avoid too much noise
	lastRequestLeaderErr time.Time
}

type PgxFollowerConfig struct {
	Queue         Queue
	Broadcaster   broadcaster.Broadcaster
	Notifier      PgNotifier
	LeaderChannel string
	Address       string
	StopChan      chan bool
	Timeout       time.Duration
}

func NewPgxFollower(cfg PgxFollowerConfig) *PgxFollower {
	return &PgxFollower{
		queue:    cfg.Queue,
		awb:      cfg.Broadcaster,
		notify:   cfg.Notifier,
		chLeader: cfg.LeaderChannel,
		address:  cfg.Address,
		stop:     cfg.StopChan,
		timeout:  cfg.Timeout,
		promote:  make(chan bool),
	}
}

func (p *PgxFollower) Follow() (result FollowResult) {
	l := p.awb.Subscribe(electiontypes.ClusterMessageTypePing)
	defer p.awb.Unsubscribe(l)

	for !func() (end bool) {
		timeout := time.NewTimer(p.timeout)
		defer timeout.Stop()
		select {
		case <-p.stop:
			// Follower has been instructed to stop. This means that service is shutting down.
			end = true
		case <-p.promote:
			// Follower has been instructed to become a leader.
			result = FollowResultPromote
			end = true
		case n := <-l:
			// Follower has received a notification. For example, the follower receives
			// periodic "pings" from the leader.
			if cn, ok := n.(*electiontypes.ClusterPingRequest); ok {
				go p.handleNotify(cn)
			}
		case <-timeout.C:
			// Follower has received no pings for the timeout duration. It is time to
			// ask for a new leader.
			slog.Debug(fmt.Sprintf("Follower '%s' ping receipt timeout. Requesting a new leader", p.address))
			go p.requestLeader()
		}
		return
	}() {
	}
	return
}

func (p *PgxFollower) Promote() {
	p.promote <- true
}

func (p *PgxFollower) handleNotify(cn *electiontypes.ClusterPingRequest) {
	resp := electiontypes.NewClusterPingResponse(p.address, cn.SrcAddr, p.awb.IP())
	b, err := json.Marshal(resp)
	if err != nil {
		log.Printf("Error marshaling notification to JSON: %s", err)
		return
	}
	slog.Log(context.Background(), LevelTrace, fmt.Sprintf("Follower %s responding to ping from leader %s", p.address, cn.SrcAddr))
	err = p.notify.Notify(p.chLeader, b)
	if err != nil {
		log.Printf("Follower error responding to leader ping: %s", err)
	}
}

func (p *PgxFollower) requestLeader() {
	err := p.queue.Push(electiontypes.AssumeLeader{SrcAddr: p.address})
	if err != nil {
		now := time.Now()
		// Limit how often this message logs to avoid too much spam
		if p.lastRequestLeaderErr.IsZero() || p.lastRequestLeaderErr.Add(5*time.Minute).Before(now) {
			log.Printf("Error pushing leader assumption work to queue: %s", err)
			p.lastRequestLeaderErr = now
		}
	}
}
