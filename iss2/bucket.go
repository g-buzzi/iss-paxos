package iss2

import (
	"strconv"
	"sync"

	"github.com/ailidani/paxi"
)

type BucketItem struct {
	request   *paxi.Request
	next      *BucketItem
	previous  *BucketItem
	committed bool
}

func RequestID(req *paxi.Request) string {
	return strconv.Itoa(req.Command.ClientID.Zone()) + "." + strconv.Itoa(req.Command.ClientID.Node()) + "." + strconv.Itoa(req.Command.CommandID) + "." + strconv.Itoa(int(req.Command.Key))
}

type Bucket struct {
	mutex       *sync.Mutex
	group       *BucketGroup
	NumRequests int
	reqIndex    map[string]*BucketItem
	firstReq    *BucketItem
	lastReq     *BucketItem
}

func NewBucket() *Bucket {
	b := &Bucket{
		mutex:       &sync.Mutex{},
		group:       nil,
		NumRequests: 0,
		reqIndex:    make(map[string]*BucketItem),
		firstReq:    nil,
		lastReq:     nil,
	}

	return b
}

func (b *Bucket) Add(req *paxi.Request) {
	defer b.Unlock()

	new := false
	b.Lock()
	requestId := RequestID(req)
	existingItem, exists := b.reqIndex[requestId]
	if exists {
		if !existingItem.committed && existingItem.previous == nil && existingItem.next == nil && existingItem != b.firstReq {
			b.prepend(existingItem)
			new = true
		}
	} else {
		bucketItem := BucketItem{request: req, committed: false}
		b.reqIndex[requestId] = &bucketItem
		b.append(&bucketItem)
		new = true
	}

	if b.group != nil && new {
		select {
		case b.group.getTrigger <- true:
		default:
		}
	}
}

// Must be called while locked
func (b *Bucket) append(bucketItem *BucketItem) {
	if b.firstReq == nil {
		b.firstReq = bucketItem
		b.lastReq = bucketItem
	} else {
		bucketItem.previous = b.lastReq
		b.lastReq.next = bucketItem
		b.lastReq = bucketItem
	}
	b.NumRequests += 1
}

func (b *Bucket) prepend(bucketItem *BucketItem) {
	if b.firstReq == nil {
		b.firstReq = bucketItem
		b.lastReq = bucketItem
	} else {
		bucketItem.next = b.firstReq
		b.firstReq.previous = bucketItem
		b.firstReq = bucketItem
	}
	b.NumRequests += 1
}

func (b *Bucket) Commit(req *paxi.Request) bool {
	b.mutex.Lock()
	defer b.Unlock()
	requestId := RequestID(req)
	bucketItem, ok := b.reqIndex[requestId]
	if ok {
		bucketItem.committed = true
		if bucketItem.previous == nil && bucketItem.next == nil && bucketItem != b.firstReq { //check if it already was in flight
			return ok
		}
		previous := bucketItem.previous
		next := bucketItem.next

		if previous != nil && next != nil {
			previous.next = next
		} else if previous != nil {
			previous.next = nil
			if bucketItem == b.lastReq {
				b.lastReq = previous
			}
		} else if next != nil {
			next.previous = nil
			if bucketItem == b.firstReq {
				b.firstReq = next
			}
		} else {
			b.firstReq = nil
			b.lastReq = nil
		}

		bucketItem.previous = nil
		bucketItem.next = nil
		b.NumRequests -= 1
	} else {
		bucketItem := BucketItem{request: req, committed: true}
		b.reqIndex[requestId] = &bucketItem
	}
	return ok
}

func (b *Bucket) Get() *paxi.Request { //needs to be called while locked
	bucketItem := b.firstReq
	if bucketItem != nil {
		//delete(b.reqIndex, RequestID(bucketItem.request)) (no longer necessary, since clients can send to multiple processess)
		next := bucketItem.next
		if next != nil {
			next.previous = nil
		}
		b.firstReq = next
		bucketItem.next = nil
		b.NumRequests -= 1
	}
	//log.Debugf("Removed request: %v to bucket", RequestID(bucketItem.request))
	return bucketItem.request
}

func (b *Bucket) Lock() {
	//log.Debugf("Locking %v", b)
	b.mutex.Lock()
}

func (b *Bucket) Unlock() {
	//log.Debugf("Unlocking %v", b)
	b.mutex.Unlock()
}
