package iss2

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ailidani/paxi"
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
	epochChan      chan int
	messageChan    chan interface{}
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
		epochChan:      make(chan int),
		messageChan:    make(chan interface{}, paxi.GetConfig().BufferSize),
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
	typeMap := map[string]string{
		"iss2.P1a": "HP1A",
		"iss2.P1b": "HP1B",
		"iss2.P2a": "HP2A",
		"iss2.P2b": "HP2B",
		"iss2.P3":  "HP3",
	}
	iss.startEpoch(0)
	for {
		select {
		// Starts a new Epoch
		case <-iss.epochEnd:
			for i := 0; i < numSegments; i++ {
				iss.segments[i].workQueue <- work{id: "End", argument: nil}
			}
			iss.startEpoch(int(iss.currentEpoch) + 1)
		// Deals with new messages
		case m := <-iss.messageChan:
			message := m.(InternalMessage)
			if message.epoch() == int(iss.currentEpoch) {
				command := typeMap[fmt.Sprintf("%T", m)]
				iss.segments[message.segment()].workQueue <- work{id: command, argument: m}
			} else if message.epoch() > int(iss.currentEpoch) {
				iss.bufferChan <- m
			}
		}
	}
}

func (iss *ISS) startEpoch(epoch int) {
	leaders := iss.selectLeader(epoch)

	atomic.StoreInt64(&iss.currentEpoch, int64(-1))
	for i, leader := range leaders {
		bucketGroup := iss.createBucketGroup(epoch, i)
		iss.segments[i] = NewISSPaxos(iss, leader, bucketGroup, int64(epoch), i)
		go iss.segments[i].run()
	}
	atomic.StoreInt64(&iss.currentEpoch, int64(epoch))

	iss.retrieveBuffer <- true

	for i := (epoch - logRetention) * epochSize; i < (epoch-logRetention+1)*epochSize; i++ {
		delete(iss.log, i)
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
				iss.messageChan <- added
				/*
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
				*/
			} else if internal.epoch() > epoch {
				iss.epochBuffer.add(added)
			}
		case <-iss.retrieveBuffer:
			rejected := newStack()
			n := iss.epochBuffer.len()
			for i := 0; i < n; i++ {
				message := iss.epochBuffer.pop()
				//log.Debugf("Got %v", message)
				//log.Debugf("Stack state %v", iss.epochBuffer.stack)
				internal := message.(InternalMessage)
				epoch := int(atomic.LoadInt64(&iss.currentEpoch))
				if internal.epoch() == epoch {
					//log.Debugf("Retrieving message from epoch buffer %v", message)
					iss.messageChan <- message
					/*
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
					*/
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
	iss.messageChan <- m
	/*
		if m.Epoch == int(iss.currentEpoch) {
			iss.segments[m.Segment].workQueue <- work{id: "HP1A", argument: m}
		} else if m.Epoch > int(iss.currentEpoch) {
			iss.bufferChan <- m
		}
	*/

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
	iss.messageChan <- m
	/*
		if m.Epoch == int(iss.currentEpoch) {
			iss.segments[m.Segment].workQueue <- work{id: "HP1B", argument: m}
		} else if m.Epoch > int(iss.currentEpoch) {
			iss.bufferChan <- m
		}
	*/
}

// HandleP2a handles P2a message
func (iss *ISS) HandleP2a(m P2a) {
	iss.messageChan <- m
	/*
		if m.Epoch == int(iss.currentEpoch) {
			iss.segments[m.Segment].workQueue <- work{id: "HP2A", argument: m}
		} else if m.Epoch > int(iss.currentEpoch) {
			iss.bufferChan <- m
		}
	*/
}

// HandleP2b handles P2b message
func (iss *ISS) HandleP2b(m P2b) {
	iss.messageChan <- m
	/*
		if m.Epoch == int(iss.currentEpoch) {
			iss.segments[m.Segment].workQueue <- work{id: "HP2B", argument: m}
		} else if m.Epoch > int(iss.currentEpoch) {
			iss.bufferChan <- m
		}
	*/
}

// HandleP3 handles phase 3 commit message
func (iss *ISS) HandleP3(m P3) {
	iss.messageChan <- m
	/*
		if m.Epoch == int(iss.currentEpoch) {
			iss.segments[m.Segment].workQueue <- work{id: "HP3", argument: m}
		} else if m.Epoch > int(iss.currentEpoch) {
			iss.bufferChan <- m
		}
	*/
}

func (iss *ISS) exec() {
	for {
		update := <-iss.logChan
		slot := update.slot
		iss.log[slot] = &update.entry
		e, exist := iss.log[iss.execute]
		for exist && e.commit {
			//log.Debugf("Executed %v", iss.execute)
			value := iss.Execute(e.command)
			if e.request != nil {
				iss.committ(e.request)
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

func (iss *ISS) committ(r *paxi.Request) {
	hashing := iss.bucketHash(&r.Command.ClientID)
	iss.buckets[hashing].Commit(r)
}
