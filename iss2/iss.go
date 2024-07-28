package iss2

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var logRetention = 5

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

type logUpdate struct {
	entry entry
	slot  int
}

type ISS struct {
	paxi.Node

	config         []paxi.ID
	currentEpoch   int64
	segments       []*ISSPaxos
	segmentsMutex  *sync.RWMutex
	epochBuffer    Stack //holds messages that are from future epochs
	execute        int   // next execute slot number
	log            map[int]*entry
	buckets        []*Bucket
	logChan        chan logUpdate
	bufferChan     chan interface{}
	retrieveBuffer chan bool
	epochEnd       chan bool
}

func NewISS(n paxi.Node, options ...func(*ISS)) *ISS {
	iss := &ISS{
		Node:           n,
		config:         paxi.GetConfig().IDs(),
		currentEpoch:   0,
		log:            make(map[int]*entry, paxi.GetConfig().BufferSize),
		segments:       make([]*ISSPaxos, numSegments),
		segmentsMutex:  &sync.RWMutex{},
		epochBuffer:    newStack(),
		buckets:        make([]*Bucket, numBuckets),
		logChan:        make(chan logUpdate, segmentSize*numSegments),
		bufferChan:     make(chan interface{}, segmentSize*numSegments),
		retrieveBuffer: make(chan bool, 10),
		epochEnd:       make(chan bool),
	}

	iss.orderIDS()

	for _, opt := range options {
		opt(iss)
	}

	for i := 0; i < numBuckets; i++ {
		iss.buckets[i] = NewBucket()
	}

	go iss.runEpoch()
	go iss.bufferManager()
	go iss.exec()

	return iss
}

// Orders the ids in config ID
func (iss *ISS) orderIDS() {
	slices.Sort(iss.config)
}

func (iss *ISS) runEpoch() {
	for epoch := 0; true; epoch++ {
		leaders := iss.selectLeader(epoch)

		iss.segmentsMutex.Lock()
		atomic.StoreInt64(&iss.currentEpoch, int64(-1))
		for i, leader := range leaders {
			bucketGroup := iss.createBucketGroup(epoch, i)
			iss.segments[i] = NewISSPaxos(iss, leader, bucketGroup, int64(epoch), i)
			go iss.segments[i].run()
		}
		atomic.StoreInt64(&iss.currentEpoch, int64(epoch))
		iss.segmentsMutex.Unlock()

		iss.retrieveBuffer <- true

		for i := (epoch - logRetention) * epochSize; i < (epoch-logRetention+1)*epochSize; i++ {
			delete(iss.log, i)
		}
		<-iss.epochEnd
		for i := 0; i < numSegments; i++ {
			iss.segments[i].workQueue <- work{id: "End", argument: nil}
		}
	}
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
		bucketGroup.addBucket(iss.buckets[i])
	}
	return bucketGroup
}

func (iss *ISS) HandleRequest(r paxi.Request) {
	hashing := iss.bucketHash(&r.Command.ClientID)
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

func (iss *ISS) bufferManager() {
	for {
		select {
		case added := <-iss.bufferChan:
			internal := added.(InternalMessage)
			epoch := int(atomic.LoadInt64(&iss.currentEpoch))
			if internal.epoch() == epoch {
				switch fmt.Sprintf("%T", added) {
				case "iss2.P1a":
					iss.HandleP1a(added.(P1a))
				case "iss2.P1b":
					iss.HandleP1b(added.(P1b))
				case "iss2.P2a":
					iss.HandleP2a(added.(P2a))
				case "iss2.P2b":
					iss.HandleP2b(added.(P2b))
				case "iss2.P3":
					iss.HandleP3(added.(P3))
				}
			} else if internal.epoch() > epoch {
				iss.epochBuffer.add(added)
			}
		case <-iss.retrieveBuffer:
			rejected := newStack()
			n := iss.epochBuffer.len()
			for i := 0; i < n; i++ {
				message := iss.epochBuffer.pop()
				log.Debugf("Got %v", message)
				log.Debugf("Stack state %v", iss.epochBuffer.stack)
				internal := message.(InternalMessage)
				epoch := int(atomic.LoadInt64(&iss.currentEpoch))
				if internal.epoch() == epoch {
					log.Debugf("Retrieving message from epoch buffer %v", message)
					switch fmt.Sprintf("%T", message) {
					case "iss2.P1a":
						iss.HandleP1a(message.(P1a))
					case "iss2.P1b":
						iss.HandleP1b(message.(P1b))
					case "iss2.P2a":
						iss.HandleP2a(message.(P2a))
					case "iss2.P2b":
						iss.HandleP2b(message.(P2b))
					case "iss2.P3":
						iss.HandleP3(message.(P3))
					}
				} else if internal.epoch() > epoch {
					rejected.add(message)
				}
			}
			iss.epochBuffer = rejected
		}
	}
}

// HandleP1a handles P1a message
func (iss *ISS) HandleP1a(m P1a) {
	iss.segmentsMutex.RLock()
	if m.Epoch == int(iss.currentEpoch) {
		iss.segments[m.Segment].workQueue <- work{id: "HP1A", argument: m}
	} else if m.Epoch > int(iss.currentEpoch) {
		iss.bufferChan <- m
	}
	iss.segmentsMutex.RUnlock()
	//else if m.Epoch >= int(iss.currentEpoch) - logRetention { //Deal with old instances that didn't receive a specific P3 message
	//	for i := m.Epoch * epochSize; i < (m.Epoch+1)*epochSize; i++ {
	//		iss.Send(m.Ballot.ID(), P3{
	//			Ballot:  iss.log[i].ballot,
	//			Slot:    i,
	//			Command: iss.log[i].command,
	//		})
	//	}
	//}
}

// HandleP1a handles P1b message
func (iss *ISS) HandleP1b(m P1b) {
	iss.segmentsMutex.RLock()
	if m.Epoch == int(iss.currentEpoch) {
		iss.segments[m.Segment].workQueue <- work{id: "HP1B", argument: m}
	} else if m.Epoch > int(iss.currentEpoch) {
		iss.bufferChan <- m
	}
	iss.segmentsMutex.RUnlock()
}

// HandleP2a handles P2a message
func (iss *ISS) HandleP2a(m P2a) {
	iss.segmentsMutex.RLock()
	if m.Epoch == int(iss.currentEpoch) {
		iss.segments[m.Segment].workQueue <- work{id: "HP2A", argument: m}
	} else if m.Epoch > int(iss.currentEpoch) {
		iss.bufferChan <- m
	}
	iss.segmentsMutex.Unlock()
}

// HandleP2b handles P2b message
func (iss *ISS) HandleP2b(m P2b) {
	iss.segmentsMutex.RLock()
	if m.Epoch == int(iss.currentEpoch) {
		iss.segments[m.Segment].workQueue <- work{id: "HP2B", argument: m}
	} else if m.Epoch > int(iss.currentEpoch) {
		iss.bufferChan <- m
	}
	iss.segmentsMutex.RUnlock()
}

// HandleP3 handles phase 3 commit message
func (iss *ISS) HandleP3(m P3) {
	iss.segmentsMutex.RLock()
	if m.Epoch == int(iss.currentEpoch) {
		iss.segments[m.Segment].workQueue <- work{id: "HP3", argument: m}
	} else if m.Epoch > int(iss.currentEpoch) {
		iss.bufferChan <- m
	}
	iss.segmentsMutex.RUnlock()
}

func (iss *ISS) exec() {
	for {
		update := <-iss.logChan
		slot := update.slot
		iss.log[slot] = &update.entry
		e, exist := iss.log[iss.execute]
		for exist && e.commit {
			log.Debugf("Executed %v", iss.execute)
			value := iss.Execute(e.command)
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
			if iss.execute == (int(atomic.LoadInt64(&iss.currentEpoch))+1)*epochSize {
				iss.epochEnd <- true
			}
		}
	}
}
