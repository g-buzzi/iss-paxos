package mlpaxos

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

	if r.Paxos.IsLeader() || r.Paxos.FreeBallot() != -1 {
		r.Paxos.HandleRequest(m)
	} else {
		go r.Forward(r.Paxos.SomeLeader(), m)
	}
}

//func (r *Replica) readInProgress(m paxi.Request) (paxi.Value, bool) {
// TODO
// (1) last slot is read?
// (2) entry in log over writen
// (3) value is not overwriten command

// is in progress
//	for i := r.Paxos.slot; i >= r.Paxos.execute; i-- {
//		entry, exist := r.Paxos.log[i]
//		if exist && entry.command.Key == m.Command.Key {
//			return entry.command.Value, true
//		}
//	}
//
// not in progress key
//	return r.Node.Execute(m.Command), false
//}
