package iss2

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
)

const numBuckets int = 2
const numSegments int = 2
const segmentSize int = 4
const epochSize int = numSegments * segmentSize
const timeout time.Duration = time.Millisecond * 200    //Timeout for batches
const heartbeat time.Duration = time.Millisecond * 1000 //

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

type ISS struct {
	paxi.Node

	config       []paxi.ID
	currentEpoch int
	segments     []*ISSPaxos
	epochBuffer  []interface{} //holds messages that are from future epochs
	execute      int           // next execute slot number
	slot         int           // highest slot number
	log          map[int]*entry
	buckets      []*Bucket
	logUpdate    chan bool
	epochEnd     chan bool
}

func NewISS(n paxi.Node, options ...func(*ISS)) *ISS {
	iss := &ISS{
		Node:         n,
		config:       paxi.GetConfig().IDs(),
		currentEpoch: 0,
		log:          make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:         -1,
		segments:     make([]*ISSPaxos, numSegments),
		epochBuffer:  make([]interface{}, 0),
		buckets:      make([]*Bucket, numBuckets),
		logUpdate:    make(chan bool, segmentSize*numSegments),
		epochEnd:     make(chan bool),
	}

	iss.orderIDS()

	for _, opt := range options {
		opt(iss)
	}

	for i := 0; i < numBuckets; i++ {
		iss.buckets[i] = NewBucket()
	}

	go iss.runEpoch(iss.currentEpoch)
	go iss.exec()

	return iss
}

// Orders the ids in config ID
func (iss *ISS) orderIDS() {
	slices.Sort(iss.config)
}

func (iss *ISS) runEpoch(epoch int) {
	leaders := iss.selectLeader(epoch)
	for i, leader := range leaders {
		bucketGroup := iss.createBucketGroup(epoch, i)
		slots := iss.getSlotNumbers(epoch, i)
		//log.Debugf("The leader of segment %v of epoch %v is %v", i, iss.currentEpoch, leader)
		iss.segments[i] = NewISSPaxos(iss, leader, bucketGroup, slots, iss.currentEpoch, i)
		go iss.segments[i].run()
	}
	iss.retrieveBuffer()
	<-iss.epochEnd

	iss.currentEpoch += 1

	iss.endEpoch(iss.currentEpoch)
	go iss.runEpoch(iss.currentEpoch)

	// FIX ME: Log trash collection
	//for i := (iss.currentEpoch - 1) * epochSize; i < iss.currentEpoch*epochSize; i++ {
	//	delete(iss.log, i)
	//}
}

func (iss *ISS) endEpoch(epoch int) {
	for _, segment := range iss.segments {
		segment.heartbeat <- false
	}
}

func (iss *ISS) getSlotNumbers(epoch, segment int) []int {
	slots := make([]int, segmentSize)
	start := epochSize * epoch
	for i := 0; i < segmentSize; i++ {
		slots[i] = start + i*numSegments + segment
	}
	return slots
}

func (iss *ISS) selectLeader(epoch int) []paxi.ID {
	//log.Debugf("ISSConfig: %v", iss.config)
	leaders := make([]paxi.ID, numSegments)
	for i := 0; i < numSegments; i++ {
		leaders[i] = iss.config[(i+epoch)%len(iss.config)]
	}
	return leaders
}

func (iss *ISS) createBucketGroup(epoch int, segment int) *BucketGroup {
	bucketGroup := NewBucketGroup()
	for i := segment; i < numBuckets; i = i + numSegments {
		//log.Debugf("Adding bucket %v to the BucketGroup %v in epoch %v", i, segment, epoch)
		bucketGroup.addBucket(iss.buckets[i])
	}
	return bucketGroup
}

func (iss *ISS) HandleRequest(r paxi.Request) {
	hashing := iss.bucketHash(&r.Command.ClientID)
	//log.Debugf("Request %v has been added to bucket %v", r, hashing)
	iss.buckets[hashing].Add(&r)
}

func (iss *ISS) reviveRequest(r paxi.Request) {
	iss.buckets[iss.bucketHash(&r.Command.ClientID)].Add(&r)
}

func (iss *ISS) bucketHash(nodeID *paxi.ID) int64 {
	//bucketHash := (int64(command.ClientID.Node()) >> 32) + int64(command.ClientID.Zone())
	bucketHash := int64(nodeID.Node())
	//log.Debugf("Hashing clientID: %v as number %v", nodeID, bucketHash)
	return bucketHash % int64(numBuckets)
}

func (iss *ISS) segmentHash(slot int, epoch int) int {
	if slot >= (iss.currentEpoch)*epochSize && slot < (iss.currentEpoch+1)*epochSize {
		return slot % numSegments
	} else if slot >= (iss.currentEpoch)*epochSize {
		return -1
	}
	return -2
}

func (iss *ISS) retrieveBuffer() {
	for _, message := range iss.epochBuffer {
		messageType := fmt.Sprintf("%T", message)
		switch messageType { //FIXME:: Check if this is actually working
		case "P1a":
			iss.HandleP1a(message.(P1a))
		case "P1b":
			iss.HandleP1b(message.(P1b))
		case "P2a":
			iss.HandleP2a(message.(P2a))
		case "P2b":
			iss.HandleP2b(message.(P2b))
		case "P3":
			iss.HandleP3(message.(P3))
		}

	}
}

// HandleP1a handles P1a message
func (iss *ISS) HandleP1a(m P1a) {
	if m.Epoch == iss.currentEpoch {
		//log.Debugf("Message %v was sent to segment %v", m, m.Segment)
		iss.segments[m.Segment].HandleP1a(m)
	} else if m.Epoch > iss.currentEpoch {
		//log.Debugf("Message 1A added to buffer off %v", iss.ID())
		iss.epochBuffer = append(iss.epochBuffer, m)
	} else {
		for i := m.Epoch * epochSize; i < (m.Epoch+1)*epochSize; i++ {
			iss.Send(m.Ballot.ID(), P3{
				Ballot:  iss.log[i].ballot,
				Slot:    i,
				Command: iss.log[i].command,
			})
		}
	}
}

// HandleP1a handles P1b message
func (iss *ISS) HandleP1b(m P1b) {
	if m.Epoch == iss.currentEpoch {
		//log.Debugf("Message %v was sent to segment %v", m, m.Segment)
		iss.segments[m.Segment].HandleP1b(m)
	} else if m.Epoch > iss.currentEpoch {
		//log.Debugf("Message 1B added to buffer off %v", iss.ID())
		iss.epochBuffer = append(iss.epochBuffer, m)
	}
}

// HandleP2a handles P2a message
func (iss *ISS) HandleP2a(m P2a) {
	segment := iss.segmentHash(m.Slot, iss.currentEpoch)
	if segment >= 0 {
		//log.Debugf("Message %v was sent to segment %v", m, segment)
		iss.segments[segment].HandleP2a(m)
	} else if segment == -1 {
		//log.Debugf("Message 2A added to buffer off %v", iss.ID())
		iss.epochBuffer = append(iss.epochBuffer, m)
	}
}

// HandleP2b handles P2b message
func (iss *ISS) HandleP2b(m P2b) {
	segment := iss.segmentHash(m.Slot, iss.currentEpoch)
	if segment >= 0 {
		//log.Debugf("Message %v was sent to segment %v", m, segment)
		iss.segments[segment].HandleP2b(m)
	} else if segment == -1 {
		//log.Debugf("Message 2B added to buffer off %v", iss.ID())
		iss.epochBuffer = append(iss.epochBuffer, m)
	}
}

// HandleP3 handles phase 3 commit message
func (iss *ISS) HandleP3(m P3) {
	segment := iss.segmentHash(m.Slot, iss.currentEpoch)
	if segment >= 0 {
		//log.Debugf("Message %v was sent to segment %v", m, segment)
		iss.segments[segment].HandleP3(m)
	} else if segment == -1 {
		//log.Debugf("Message 3 added to buffer off %v", iss.ID())
		iss.epochBuffer = append(iss.epochBuffer, m)
	}
}

func (iss *ISS) exec() {
	for {
		<-iss.logUpdate
		e, exist := iss.log[iss.execute]
		for exist && e.commit {
			value := iss.Execute(e.command)
			//log.Debugf("Executing command %v at slot %v", e.command, iss.execute)
			if e.request != nil {
				reply := paxi.Reply{
					Command:    e.command,
					Value:      value,
					Properties: make(map[string]string),
				}
				reply.Properties[HTTPHeaderSlot] = strconv.Itoa(iss.execute)
				reply.Properties[HTTPHeaderBallot] = e.ballot.String()
				reply.Properties[HTTPHeaderExecute] = strconv.Itoa(iss.execute)
				e.request.Reply(reply)
				e.request = nil
			}
			iss.execute++
			e, exist = iss.log[iss.execute]
			if iss.execute == (iss.currentEpoch+1)*epochSize {
				//log.Debugf("Epoch has come to and end")
				iss.epochEnd <- true
			}
		}
	}
}
