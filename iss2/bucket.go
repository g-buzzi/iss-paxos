package iss2

import (
	"strconv"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type BucketItem struct {
	request  *paxi.Request
	next     *BucketItem
	previous *BucketItem
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

	bucketItem := BucketItem{request: req}
	b.Lock()
	log.Debugf("Added request: %v to bucket", RequestID(req))
	_, exists := b.reqIndex[RequestID(req)]
	if exists {
		b.mutex.Unlock()
		return
	}
	b.reqIndex[RequestID(req)] = &bucketItem
	if b.NumRequests > 0 {
		bucketItem.previous = b.lastReq
		b.lastReq.next = &bucketItem
		b.lastReq = &bucketItem
	} else {
		b.firstReq = &bucketItem
		b.lastReq = &bucketItem
	}
	b.NumRequests += 1
	b.Unlock()
	if b.group != nil {
		select {
		case b.group.getTrigger <- true:
		default:
		}
	}
}

func (b *Bucket) Remove(req *paxi.Request) bool {
	b.mutex.Lock()
	bucketItem, ok := b.reqIndex[RequestID(req)]
	if ok {
		delete(b.reqIndex, RequestID(req))
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
		b.NumRequests -= 1
	}
	b.mutex.Unlock()
	return ok
}

func (b *Bucket) Get() *paxi.Request { //needs to be called while locked
	bucketItem := b.firstReq
	if bucketItem != nil {
		delete(b.reqIndex, RequestID(bucketItem.request))
		next := bucketItem.next
		if next != nil {
			next.previous = nil
		}
		b.firstReq = next
		b.NumRequests -= 1
	}
	log.Debugf("Removed request: %v to bucket", RequestID(bucketItem.request))
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
