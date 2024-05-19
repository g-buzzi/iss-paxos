package multi_paxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

const (
	HTTPHeaderSlot       = "Slot"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = NewPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	log.Debugf("Replica %s has free ballot %v\n", r.ID(), r.Paxos.FreeBallot())
	if r.Paxos.IsLeader() || r.Paxos.FreeBallot() != -1 {
		log.Debugf("Replica %s decided to handle the request\n", r.ID())
		r.Paxos.HandleRequest(m)
	} else {
		log.Debugf("Replica %s decided to forward the request\n", r.ID())
		go r.Forward(r.Paxos.SomeLeader(), m)
	}
}
