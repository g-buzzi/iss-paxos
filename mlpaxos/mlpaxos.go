package mlpaxos

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

// Paxos instance
type Paxos struct {
	paxi.Node

	config []paxi.ID

	log        map[int]*entry // log ordered by slot
	windowSize int            // number of open slots at a single time

	execute int            // next execute slot number
	active  [5]bool        // active leader
	ballots [5]paxi.Ballot // highest ballot number
	slots   [5]int         // highest slot number

	quorums  [5]*paxi.Quorum // phase 1 quorums
	requests []*paxi.Request // phase 1 pending requests

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(*Paxos)) *Paxos {
	p := &Paxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		windowSize:      5,
		slots:           [5]int{-1, -1, -1, -1, -1},
		quorums:         [5]*paxi.Quorum{paxi.NewQuorum(), paxi.NewQuorum(), paxi.NewQuorum(), paxi.NewQuorum(), paxi.NewQuorum()},
		requests:        make([]*paxi.Request, 0),
		Q1:              func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:              func(q *paxi.Quorum) bool { return q.Majority() },
		ReplyWhenCommit: false,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

// IsLeader indecates if this node is a current leader
func (p *Paxos) IsLeader() bool {
	for _, active := range p.active {
		if active {
			return true
		}
	}
	for _, ballot := range p.ballots {
		if ballot.ID() == p.ID() {
			return true
		}
	}
	return false
}

func (p *Paxos) IsActive() bool {
	for _, active := range p.active {
		if active {
			return true
		}
	}
	return false
}

// Leader returns leader id of the current ballot
func (p *Paxos) Leader(slotGroup int) paxi.ID {
	return p.ballots[slotGroup].ID()
}

// Leader returns leader id of the current ballot
func (p *Paxos) SomeLeader() paxi.ID {
	slotGroup := rand.Intn(p.windowSize)
	return p.ballots[slotGroup].ID()
}

// Ballot returns current ballot
func (p *Paxos) Ballot(slotGroup int) paxi.Ballot {
	return p.ballots[slotGroup]
}

// Ballot returns current ballot
func (p *Paxos) FreeBallot() int {
	for slotGroup, ballot := range p.ballots {
		if ballot == 0 || ballot.ID() == p.ID() {
			return slotGroup
		}
	}
	return -1
}

// SetActive sets current paxos instance as active leader
func (p *Paxos) SetActive(slotGroup int, active bool) {
	p.active[slotGroup] = active
}

// SetBallot sets a new ballot number
func (p *Paxos) SetBallot(slotGroup int, b paxi.Ballot) {
	p.ballots[slotGroup] = b
}

// HandleRequest handles request and start phase 1 or phase 2
func (p *Paxos) HandleRequest(r paxi.Request) {
	log.Debugf("Replica %s received %v\n", p.ID(), r)

	if !p.IsActive() {
		p.requests = append(p.requests, &r)
		// current phase 1 pending
		slotGroup := p.FreeBallot()
		if slotGroup != -1 && p.ballots[slotGroup].ID() != p.ID() {
			p.P1a(slotGroup)
		}
		//Ver se precisa de forward
	} else {
		p.P2a(&r)
	}
}

// P1a starts phase 1 prepare
func (p *Paxos) P1a(slotGroup int) {
	if p.active[slotGroup] {
		return
	}
	p.ballots[slotGroup].Next(p.ID())
	p.quorums[slotGroup].Reset()
	p.quorums[slotGroup].ACK(p.ID())
	p.Broadcast(P1a{Ballot: p.ballots[slotGroup], slotGroup: slotGroup})
}

func (p *Paxos) selectSlot() int {
	min := -1
	selectedSlotGroup := 0
	for slotGroup, active := range p.active {
		if active {
			if min == -1 || p.slots[slotGroup] < min {
				min = p.slots[slotGroup]
				selectedSlotGroup = slotGroup
			}
		}
	}
	return selectedSlotGroup
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r *paxi.Request) {
	slotGroup := p.selectSlot()
	p.slots[slotGroup] = p.slots[slotGroup] + p.windowSize
	p.log[p.slots[slotGroup]] = &entry{
		ballot:    p.ballots[slotGroup],
		command:   r.Command,
		request:   r,
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
	}
	p.log[p.slots[slotGroup]].quorum.ACK(p.ID())
	m := P2a{
		Ballot:  p.ballots[slotGroup],
		Slot:    p.slots[slotGroup],
		Command: r.Command,
	}
	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// HandleP1a handles P1a message
func (p *Paxos) HandleP1a(m P1a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	if m.Ballot > p.ballots[m.slotGroup] {
		p.ballots[m.slotGroup] = m.Ballot
		p.active[m.slotGroup] = false
		// TODO use BackOff time or forward
		// forward pending requests to new leader
		// !p.IsLeader() {
		//	p.forward()
		//}
		// if len(p.requests) > 0 {
		// 	defer p.P1a()
		// }
	}

	l := make(map[int]CommandBallot)
	for s := p.execute - (m.slotGroup - (p.execute % p.windowSize)); s <= p.slots[m.slotGroup]; s = s + p.windowSize {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot:    p.ballots[m.slotGroup],
		ID:        p.ID(),
		slotGroup: m.slotGroup,
		Log:       l,
	})
}

func (p *Paxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slots[s%p.windowSize] = paxi.Max(p.slots[s%p.windowSize], s)
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

// HandleP1b handles P1b message
func (p *Paxos) HandleP1b(m P1b) {
	p.update(m.Log)

	// old message
	if m.Ballot < p.ballots[m.slotGroup] || p.active[m.slotGroup] {
		log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	// reject message
	if m.Ballot > p.ballots[m.slotGroup] {
		p.ballots[m.slotGroup] = m.Ballot
		p.active[m.slotGroup] = false // not necessary
		// forward pending requests to new leader
		if !p.IsLeader() {
			p.forward()
		}
		// p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballots[m.slotGroup] {
		p.quorums[m.slotGroup].ACK(m.ID)
		if p.Q1(p.quorums[m.slotGroup]) {
			p.active[m.slotGroup] = true
			// propose any uncommitted entries
			for i := p.execute - (m.slotGroup - (p.execute % p.windowSize)); i <= p.slots[m.slotGroup]; i = i + p.windowSize {
				// TODO nil gap?
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballots[m.slotGroup]
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:  p.ballots[m.slotGroup],
					Slot:    i,
					Command: p.log[i].command,
				})
			}
			// propose new commands
			for _, req := range p.requests {
				p.P2a(req)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}

// HandleP2a handles P2a message
func (p *Paxos) HandleP2a(m P2a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	slotGroup := m.Slot % p.windowSize
	if m.Ballot >= p.ballots[slotGroup] {
		p.ballots[slotGroup] = m.Ballot
		p.active[slotGroup] = false
		// update slot number
		p.slots[slotGroup] = paxi.Max(p.slots[slotGroup], m.Slot)
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !e.command.Equal(m.Command) && e.request != nil {
					p.Forward(m.Ballot.ID(), *e.request)
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
		}
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot: p.ballots[slotGroup],
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

// HandleP2b handles P2b message
func (p *Paxos) HandleP2b(m P2b) {
	// old message
	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	slotGroup := m.Slot % p.windowSize
	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballots[slotGroup] {
		p.ballots[slotGroup] = m.Ballot
		p.active[slotGroup] = false
	}

	// ack message
	// the current slot might still be committed with q2
	// if no q2 can be formed, this slot will be retried when received p2a or p3
	if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
		p.log[m.Slot].quorum.ACK(m.ID)
		if p.Q2(p.log[m.Slot].quorum) {
			p.log[m.Slot].commit = true
			p.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: p.log[m.Slot].command,
			})

			if p.ReplyWhenCommit {
				r := p.log[m.Slot].request
				r.Reply(paxi.Reply{
					Command:   r.Command,
					Timestamp: r.Timestamp,
				})
			} else {
				p.exec()
			}
		}
	}
}

// HandleP3 handles phase 3 commit message
func (p *Paxos) HandleP3(m P3) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	slotGroup := m.Slot % p.windowSize
	p.slots[slotGroup] = paxi.Max(p.slots[slotGroup], m.Slot)

	e, exist := p.log[m.Slot]
	if exist {
		if !e.command.Equal(m.Command) && e.request != nil {
			// p.Retry(*e.request)
			p.Forward(m.Ballot.ID(), *e.request)
			e.request = nil
		}
	} else {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true

	if p.ReplyWhenCommit {
		if e.request != nil {
			e.request.Reply(paxi.Reply{
				Command:   e.request.Command,
				Timestamp: e.request.Timestamp,
			})
		}
	} else {
		p.exec()
	}
}

func (p *Paxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		value := p.Execute(e.command)
		if e.request != nil {
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
			reply.Properties[HTTPHeaderBallot] = e.ballot.String()
			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
}

func (p *Paxos) forward() {
	for _, m := range p.requests {

		p.Forward(p.SomeLeader(), *m)
	}
	p.requests = make([]*paxi.Request, 0)
}
