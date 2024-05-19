package multi_paxos

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
	execute    int            // next execute slot number
	windowSize int
	active     []bool        // active leader of a group i
	ballots    []paxi.Ballot // highest ballot number of a group i
	slots      []int         // highest slot number of a group i

	quorums  []*paxi.Quorum  // phase 1 quorum for a group i
	requests []*paxi.Request // phase 1 pending requests
	ticker   *time.Ticker    // Ticker to deal witch sinchronasation issues

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(*Paxos)) *Paxos {
	windowSize := 2

	p := &Paxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		windowSize:      windowSize,
		active:          make([]bool, windowSize),
		ballots:         make([]paxi.Ballot, windowSize),
		slots:           make([]int, windowSize),
		quorums:         make([]*paxi.Quorum, windowSize),
		requests:        make([]*paxi.Request, 0),
		Q1:              func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:              func(q *paxi.Quorum) bool { return q.Majority() },
		ticker:          time.NewTicker(1000 * time.Millisecond),
		ReplyWhenCommit: false,
	}

	go p.tick()

	for _, opt := range options {
		opt(p)
	}

	for i := 0; i < p.windowSize; i++ {
		p.active[i] = false
		p.slots[i] = i - windowSize
		p.quorums[i] = paxi.NewQuorum()
	}

	return p
}

func (p *Paxos) maxSlot() int {
	maxSlot := -1
	for i := 0; i < p.windowSize; i++ {
		if p.slots[i] > maxSlot {
			maxSlot = p.slots[i]
		}
	}
	return maxSlot
}

func (p *Paxos) tick() {
	previousSlots := make([]int, p.windowSize)
	innactiveSlots := make([]int, p.windowSize)
	for range p.ticker.C {
		maxSlot := p.maxSlot()
		minSlot := maxSlot - (maxSlot % p.windowSize)
		for i := 0; i < p.windowSize; i++ {
			u := p.slots[i] - previousSlots[i]
			if p.ballots[i] == 0 || (u == 0 && innactiveSlots[i] > 2) {
				log.Debugf("Replica %s decided to take over slot group %v\n", p.ID(), i)
				innactiveSlots[i] = 0
				p.P1a(i)
			}
			if u == 0 && p.slots[i] < minSlot {
				innactiveSlots[i] = innactiveSlots[i] + 1
			} else {
				innactiveSlots[i] = 0
			}
			previousSlots[i] = p.slots[i]
			if p.active[i] {
				log.Debugf("Replica %s evalued if it is delayed for slot group %v\n", p.ID(), i)
				if p.slots[i] <= minSlot {
					for slot := p.slots[i]; slot <= minSlot; slot = slot + p.windowSize {
						log.Debugf("Replica %s sent a skip message for group %v\n", p.ID(), i)
						skipRequest := paxi.Request{
							Command: paxi.Command{
								Key:       0,
								Value:     nil,
								ClientID:  "",
								CommandID: 0,
							},
							Properties: make(map[string]string),
							Timestamp:  time.Now().Unix(),
						}
						p.P2a(i, &skipRequest)
					}
				}
			}
		}
	}
}

// IsLeader indecates if this node is current leader of some group
func (p *Paxos) IsActive() bool {
	for i := 0; i < p.windowSize; i++ {
		if p.active[i] {
			return true
		}
	}
	return false
}

// IsLeader indicates if this node is current leader of some group or is trying to become one
func (p *Paxos) IsLeader() bool {
	for i := 0; i < p.windowSize; i++ {
		if p.active[i] || p.ballots[i].ID() == p.ID() {
			return true
		}
	}
	return false
}

func (p *Paxos) FreeBallot() int {
	for i := 0; i < p.windowSize; i++ {
		if p.ballots[i] == 0 {
			return i
		}
	}
	return -1
}

// Leader returns leader id of the current ballot
func (p *Paxos) Leader(slotGroup int) paxi.ID {
	return p.ballots[slotGroup].ID()
}

// SomeLeader returns leader id of the current ballot of some group
func (p *Paxos) SomeLeader() paxi.ID {
	slot := -1
	group := -1
	for i := 0; i < p.windowSize; i++ {
		if p.ballots[i] != 0 && (slot == -1 || slot > p.slots[i]) {
			slot = p.slots[i]
			group = i
		}
	}
	return p.ballots[group].ID()
}

// SelectGroup returns a group that the process may try to steal from
func (p *Paxos) SelectGroup() int {
	for slotGroup, ballot := range p.ballots {
		if ballot == 0 {
			return slotGroup
		}
	}
	return rand.Intn(p.windowSize)
}

// NextGroup for a 2A message
func (p *Paxos) NextGroup() int {
	slot := -1
	nextGroup := -1
	for i := 0; i < p.windowSize; i++ {
		if p.active[i] && (slot == -1 || slot > p.slots[i]) {
			slot = p.slots[i]
			nextGroup = i
		}
	}
	return nextGroup
}

// Ballot returns current ballot
func (p *Paxos) Ballot(slotGroup int) paxi.Ballot {
	return p.ballots[slotGroup]
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
	// log.Debugf("Replica %s received %v\n", p.ID(), r)
	slotGroup := p.NextGroup()
	if slotGroup == -1 {
		p.requests = append(p.requests, &r)
		// current phase 1 pending
		if !p.IsLeader() {
			slotGroup = p.SelectGroup()
			p.P1a(slotGroup)
		}
	} else {
		p.P2a(slotGroup, &r)
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
	m := P1a{
		Ballot:    p.ballots[slotGroup],
		SlotGroup: slotGroup,
	}
	p.Broadcast(m)
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(slotGroup int, r *paxi.Request) {
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
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
	// new leader
	if m.Ballot > p.ballots[m.SlotGroup] {
		p.ballots[m.SlotGroup] = m.Ballot
		p.active[m.SlotGroup] = false
		// TODO use BackOff time or forward
		// forward pending requests to new leader
		freeBallot := p.FreeBallot()
		active := p.IsLeader()
		if !active && freeBallot != -1 && len(p.requests) > 0 {
			log.Debugf("Replica %s decides to try for slot group %v\n", p.ID(), freeBallot)
			p.P1a(freeBallot)
		} else if !active {
			p.forward()
		}
	}

	l := make(map[int]CommandBallot)
	for s := p.execute - (p.execute % p.windowSize) + m.SlotGroup; s <= p.slots[m.SlotGroup]; s = s + p.windowSize {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot:    p.ballots[m.SlotGroup],
		SlotGroup: m.SlotGroup,
		ID:        p.ID(),
		Log:       l,
	})
}

func (p *Paxos) update(slotGroup int, scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slots[slotGroup] = paxi.Max(p.slots[slotGroup], s)
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
	p.update(m.SlotGroup, m.Log)

	// old message
	if m.Ballot < p.ballots[m.SlotGroup] || p.active[m.SlotGroup] {
		// log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	// reject message
	if m.Ballot > p.ballots[m.SlotGroup] {
		p.ballots[m.SlotGroup] = m.Ballot
		p.active[m.SlotGroup] = false // not necessary
		// forward pending requests to new leader
		freeBallot := p.FreeBallot()
		active := p.IsLeader()
		if !active && freeBallot != -1 {
			log.Debugf("Replica %s decides to try for slot group %v\n", p.ID(), freeBallot)
			p.P1a(freeBallot)
		} else if !active {
			p.forward()
		}
		// p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballots[m.SlotGroup] {
		p.quorums[m.SlotGroup].ACK(m.ID)
		if p.Q1(p.quorums[m.SlotGroup]) {
			log.Debugf("Replica %s became a leader for slot group %v\n", p.ID(), m.SlotGroup)
			p.active[m.SlotGroup] = true
			// propose any uncommitted entries
			for i := p.execute - (p.execute % p.windowSize) + m.SlotGroup; i <= p.slots[m.SlotGroup]; i = i + p.windowSize {
				// TODO nil gap?
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballots[m.SlotGroup]
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:  p.ballots[m.SlotGroup],
					Slot:    i,
					Command: p.log[i].command,
				})
			}
			// propose new commands
			for _, req := range p.requests {
				p.P2a(m.SlotGroup, req)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}

// HandleP2a handles P2a message
func (p *Paxos) HandleP2a(m P2a) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
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
	slotGroup := m.Slot % p.windowSize
	// old message
	entry, exist := p.log[m.Slot]
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}

	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
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
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
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
