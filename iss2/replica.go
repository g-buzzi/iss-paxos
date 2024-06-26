package iss2

import (
	"time"

	"github.com/ailidani/paxi"
)

//var ephemeralLeader = flag.Bool("ephemeral_leader", false, "unstable leader, if true paxos replica try to become leader instead of forward requests to current leader")
//var read = flag.String("read", "", "read from \"leader\", \"quorum\" or \"any\" replica")

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*ISS
}

var numBuckets int
var numSegments int
var segmentSize int
var epochSize int
var timeout time.Duration
var heartbeat time.Duration

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID, buckets int, segments int, sizeSegment int, batchTimeout int, heartbeatTimeout int) *Replica {
	numBuckets = buckets
	numSegments = segments
	segmentSize = sizeSegment
	epochSize = numSegments * segmentSize
	timeout = time.Millisecond * time.Duration(batchTimeout)
	heartbeat = time.Millisecond * time.Duration(heartbeatTimeout)
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.ISS = NewISS(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	// FIXME: May be necessary to forward the request in some cases
	r.ISS.HandleRequest(m)
}
