package iss2

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
}

type InternalMessage interface {
	epoch() int
	segment() int
}

// P1a prepare message
type P1a struct {
	Ballot  paxi.Ballot
	Epoch   int
	Segment int
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v} e={%v} seg={%v}", m.Ballot, m.Epoch, m.Segment)
}

func (m P1a) epoch() int {
	return m.Epoch
}

func (m P1a) segment() int {
	return m.Segment
}

// CommandBallot conbines each command with its ballot number
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot  paxi.Ballot
	Epoch   int
	Segment int
	ID      paxi.ID               // from node id
	Log     map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v e={%v} seg={%v} id=%s log=%v}", m.Ballot, m.Epoch, m.Segment, m.ID, m.Log)
}

func (m P1b) epoch() int {
	return m.Epoch
}

func (m P1b) segment() int {
	return m.Segment
}

// P2a accept message
type P2a struct {
	Ballot  paxi.Ballot
	Slot    int
	Epoch   int
	Segment int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v e=%d seg=%d s=%d cmd=%v }", m.Ballot, m.Epoch, m.Segment, m.Slot, m.Command)
}

func (m P2a) epoch() int {
	return m.Epoch
}

func (m P2a) segment() int {
	return m.Segment
}

// P2b accepted message
type P2b struct {
	Ballot  paxi.Ballot
	ID      paxi.ID // from node id
	Slot    int
	Epoch   int
	Segment int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s e=%d seg=%d s=%d}", m.Ballot, m.ID, m.Epoch, m.Segment, m.Slot)
}

func (m P2b) epoch() int {
	return m.Epoch
}

func (m P2b) segment() int {
	return m.Segment
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Epoch   int
	Segment int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v e=%d seg=%d s=%d cmd=%v}", m.Ballot, m.Epoch, m.Segment, m.Slot, m.Command)
}

func (m P3) epoch() int {
	return m.Epoch
}

func (m P3) segment() int {
	return m.Segment
}
