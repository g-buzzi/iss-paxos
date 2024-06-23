package iss2

import (
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type work struct {
	id       string
	argument interface{}
}

// Paxos instance
type ISSPaxos struct {
	iss           *ISS
	starterLeader bool
	epoch         int
	segment       int
	BucketGroup   *BucketGroup
	active        bool        // active leader
	ballot        paxi.Ballot // highest ballot number
	slot          int         // highest index of slot used
	heartbeat     chan bool
	timer         *time.Timer
	log           map[int]*entry // Internal log of the segment
	workQueue     chan work      //Ordering of messages that the instace needs to handle

	quorum *paxi.Quorum // phase 1 quorum

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewPaxos creates new paxos instance
func NewISSPaxos(iss *ISS, leader paxi.ID, bucketGroup *BucketGroup, epoch int64, segment int) *ISSPaxos {
	p := &ISSPaxos{
		iss:             iss,
		active:          iss.ID() == leader,
		starterLeader:   iss.ID() == leader,
		BucketGroup:     bucketGroup,
		ballot:          paxi.NewBallot(0, leader),
		segment:         segment,
		epoch:           int(epoch),
		slot:            -1,
		heartbeat:       make(chan bool, 1), //FIXME:size of this shouldn't matter
		workQueue:       make(chan work, 200),
		log:             make(map[int]*entry, segmentSize),
		quorum:          paxi.NewQuorum(),
		Q1:              func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:              func(q *paxi.Quorum) bool { return q.Majority() },
		ReplyWhenCommit: false,
	}
	//log.Debugf("Created node for segment %v of epoch %v, starter leader: %v", segment, epoch, p.starterLeader)
	////log.Debugf("Starting ballot for segment %v of epoch %v is %v", segment, epoch, p.ballot)
	return p
}

func (p *ISSPaxos) run() {
	go p.worker()
	p.timer = time.AfterFunc(heartbeat, func() {
		select {
		case p.heartbeat <- true:
		default:
		}
	})
	go p.heartbeatMonitor()

	if p.starterLeader {
		//log.Debugf("Node is leader of segment %v of epoch %v", p.segment, p.epoch)
		for i := 0; i < segmentSize && p.active; i++ {
			request := p.BucketGroup.get()
			p.workQueue <- work{id: "P2A", argument: request}
		}
	}
}

func (p *ISSPaxos) worker() {
	keepWorker := true
	for keepWorker {
		job := <-p.workQueue
		log.Debugf("Executing %v s=%v", job.id, p.segment)
		switch job.id {
		case "P1A":
			p.P1a()
		case "P2A":
			if job.argument == nil {
				p.P2a(nil)
			} else {
				p.P2a(job.argument.(*paxi.Request))
			}
		case "HP1A":
			p.HandleP1a(job.argument.(P1a))
		case "HP1B":
			p.HandleP1b(job.argument.(P1b))
		case "HP2A":
			p.HandleP2a(job.argument.(P2a))
		case "HP2B":
			p.HandleP2b(job.argument.(P2b))
		case "HP3":
			p.HandleP3(job.argument.(P3))
		case "End":
			keepWorker = false
		default:
			log.Debugf("No known handler for message!!!!!!!!!!!")
		}
	}
	select {
	case p.heartbeat <- false:
	default:
	}
	//close(p.workQueue)
}

func (p *ISSPaxos) heartbeatMonitor() {
	for p.slot < segmentSize-1 || !(p.log[segmentSize-1] != nil && p.log[segmentSize-1].commit) { //FIXME: This condition may not account for non-commited entries

		expired := <-p.heartbeat // Wait for timer to expire

		if !p.IsLeader() && expired {
			p.workQueue <- work{id: "P1A", argument: nil}
		}
		p.ResetTimer()
	}
}

func (p *ISSPaxos) ResetTimer() {
	//if !p.timer.Stop() {
	//	<-p.timer.C
	//}
	p.timer.Reset(heartbeat)
}

// IsLeader indecates if this node is current leader
func (p *ISSPaxos) IsLeader() bool {
	return p.active || p.ballot.ID() == p.iss.ID()
}

// Leader returns leader id of the current ballot
func (p *ISSPaxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *ISSPaxos) Ballot() paxi.Ballot {
	return p.ballot
}

// SetActive sets current paxos instance as active leader
func (p *ISSPaxos) SetActive(active bool) {
	p.active = active
}

// SetBallot sets a new ballot number
func (p *ISSPaxos) SetBallot(b paxi.Ballot) {
	p.ballot = b
}

// P1a starts phase 1 prepare
func (p *ISSPaxos) P1a() {
	if p.active {
		return
	}
	p.ResetTimer()
	p.ballot.Next(p.iss.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.iss.ID())
	p.iss.Broadcast(P1a{Ballot: p.ballot, Epoch: p.epoch, Segment: p.segment})
}

// HandleP1a handles P1a message
func (p *ISSPaxos) HandleP1a(m P1a) {
	//log.Debugf("Replica %s received a message P1a of segment %v of epoch %v", m.Ballot.ID(), p.segment, p.epoch)

	if m.Ballot > p.ballot {
		p.ResetTimer()
		//log.Debugf("Message is valid")
		p.ballot = m.Ballot
		if p.active {
			//log.Debugf("Replica %s is no longer a leader of segment %v of epoch %v", m.Ballot.ID(), p.segment, p.epoch)
			p.active = false
		}
	}

	//log.Debugf("Making the log mapping")
	l := make(map[int]CommandBallot)
	for s := 0; s < segmentSize; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}
	//log.Debugf("Log mapped")
	p.iss.Send(m.Ballot.ID(), P1b{
		Ballot:  p.ballot,
		ID:      p.iss.ID(),
		Log:     l,
		Segment: p.segment,
		Epoch:   p.epoch,
	})
}

// HandleP1b handles P1b message
func (p *ISSPaxos) HandleP1b(m P1b) {
	log.Debugf("Updating Log")
	p.update(m.Log)
	log.Debugf("Got past")
	// old message
	if m.Ballot < p.ballot || p.active {
		return
	}

	// reject message
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
	}

	// ack message
	if m.Ballot.ID() == p.iss.ID() && m.Ballot == p.ballot {
		log.Debugf("Trying Ack")
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			log.Debugf("Got Quorum")
			p.active = true
			for s := 0; s <= p.slot; s++ {
				log.Debugf("Seeing if has to resend slot %v of segment %v, with slot=%v", s, p.segment, p.log[s])
				if p.log[s] == nil || p.log[s].commit || s >= segmentSize {
					log.Debugf("Not resending slot %v of segment %v", s, p.segment)
					//log.Debugf("Skipping log for %v after steal of segment %v in epoch %v", s, p.segment, p.epoch)
					continue
				}
				log.Debugf("Decided to resend slot %v of segment %v", s, p.segment)
				p.log[s].ballot = p.ballot
				p.log[s].quorum = paxi.NewQuorum()
				p.log[s].quorum.ACK(p.iss.ID())
				p.iss.Broadcast(P2a{
					Ballot:  p.ballot,
					Slot:    s,
					Epoch:   p.epoch,
					Segment: p.segment,
					Command: p.log[s].command,
				})
			}
			// propose new commands
			for i := p.slot + 1; i < segmentSize; i++ {
				if p.log[i] != nil {
					continue
				}
				p.workQueue <- work{id: "P2A", argument: nil}
			}
		}
	}
}

// P2a starts phase 2 accept
func (p *ISSPaxos) P2a(r *paxi.Request) {
	if p.slot == segmentSize || !p.IsLeader() {
		if r != nil {
			p.iss.reviveRequest(*r)
		}
		return
	}
	p.slot++
	s := p.slot

	p.ResetTimer()

	var command paxi.Command
	if r != nil {
		command = r.Command
	} else {
		command = paxi.Command{Key: 0, Value: nil, ClientID: "", CommandID: 0}
	}

	p.log[s] = &entry{
		ballot:    p.ballot,
		command:   command,
		request:   r,
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
	}

	//log.Debugf("Log: %v", p.iss.log[s])

	p.log[s].quorum.ACK(p.iss.ID())
	m := P2a{
		Ballot:  p.ballot,
		Slot:    s,
		Epoch:   p.epoch,
		Segment: p.segment,
		Command: command,
	}
	if paxi.GetConfig().Thrifty {
		p.iss.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.iss.Broadcast(m)
	}
}

func (p *ISSPaxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		// FIXME Testing if this works

		//log.Debugf("Calculating slot for p.slot = {%v} and s = {%v} for segment {%v} of epoch {%v}", p.slot, s, p.segment, p.epoch)
		internalSlot := paxi.Max(p.slot, 0)
		if p.slot >= segmentSize {
			internalSlot = segmentSize - 1
		}
		p.slot = paxi.Max(internalSlot, s)
		s = p.slot
		//log.Debugf("It decided on {%v}", p.slot)

		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
			}
		} else {
			p.log[s] = &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  false,
			}
		}
	}
}

// HandleP2a handles P2a message
func (p *ISSPaxos) HandleP2a(m P2a) {
	// //log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	if m.Ballot >= p.ballot {
		p.ResetTimer()
		p.ballot = m.Ballot
		p.active = false
		// update slot number
		// FIXME
		//log.Debugf("Calculating slot for p.slot = {%v} and m.Slot = {%v} for segment {%v} of epoch {%v}", p.slot, m.Slot, p.segment, p.epoch)
		internalSlot := paxi.Max(p.slot, 0)
		if p.slot >= segmentSize {
			internalSlot = segmentSize - 1
		}
		p.slot = paxi.Max(internalSlot, m.Slot)
		//log.Debugf("It decided on {%v}", p.slot)

		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !e.command.Equal(m.Command) && e.request != nil {
					p.iss.reviveRequest(*e.request)
					// p.Retry(*e.request)
					e.request = nil
				}
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command,
				commit:  false,
			}
			//log.Debugf("Log: %v", p.iss.log[m.Slot])
		}
	}

	p.iss.Send(m.Ballot.ID(), P2b{
		Ballot:  p.ballot,
		Slot:    m.Slot,
		Epoch:   p.epoch,
		Segment: p.segment,
		ID:      p.iss.ID(),
	})
}

// HandleP2b handles P2b message
func (p *ISSPaxos) HandleP2b(m P2b) {
	// old message

	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		//log.Debugf("Silent reject")
		return
	}

	// //log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
	}

	// ack message
	// the current slot might still be committed with q2
	// if no q2 can be formed, this slot will be retried when received p2a or p3
	if m.Ballot.ID() == p.iss.ID() && m.Ballot == p.log[m.Slot].ballot {
		p.log[m.Slot].quorum.ACK(m.ID)
		if p.Q2(p.log[m.Slot].quorum) {
			p.log[m.Slot].commit = true
			p.iss.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Epoch:   p.epoch,
				Segment: p.segment,
				Command: p.log[m.Slot].command,
			})

			if p.ReplyWhenCommit {
				r := p.log[m.Slot].request
				r.Reply(paxi.Reply{
					Command:   r.Command,
					Timestamp: r.Timestamp,
				})
			}

			p.iss.logChan <- logUpdate{entry: *p.log[m.Slot],
				slot: (p.epoch * epochSize) + (m.Slot * numSegments) + p.segment}

		}
	}
}

// HandleP3 handles phase 3 commit message
func (p *ISSPaxos) HandleP3(m P3) {
	// //log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	p.ResetTimer()
	// FIXME
	//log.Debugf("Calculating slot for p.slot = {%v} and m.Slot = {%v} for segment {%v} of epoch {%v}", p.slot, m.Slot, p.segment, p.epoch)
	internalSlot := paxi.Max(p.slot, 0)
	if p.slot > segmentSize {
		internalSlot = segmentSize - 1
	}
	p.slot = paxi.Max(internalSlot, m.Slot)
	//log.Debugf("It decided on = {%v}", p.slot)
	//internalSlot := paxi.Max(p.slot, 0)
	//if p.slot > segmentSize {
	//	internalSlot = segmentSize - 1
	//}
	//s := p.slotNumbers[internalSlot]
	//p.slot = paxi.Max(m.Slot, s) / segmentSize //Value gets floored as its a division between ints
	e, exist := p.log[m.Slot]
	if exist { //FIXME
		if !e.command.Equal(m.Command) && e.request != nil {
			// p.Retry(*e.request)
			p.iss.reviveRequest(*e.request)
			e.request = nil
			//log.Debugf("Set request on nil at P3")
		}
	} else {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true
	p.iss.logChan <- logUpdate{entry: *p.log[m.Slot],
		slot: (p.epoch * epochSize) + (m.Slot * numSegments) + p.segment}

	if p.ReplyWhenCommit {
		if e.request != nil {
			e.request.Reply(paxi.Reply{
				Command:   e.request.Command,
				Timestamp: e.request.Timestamp,
			})
		}
	}
}
