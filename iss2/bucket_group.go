package iss2

import (
	"time"

	"github.com/ailidani/paxi"
)

type BucketGroup struct {
	timer         *time.Timer
	buckets       []*Bucket
	lastBucket    int
	getTrigger    chan bool
	totalRequests int
}

func NewBucketGroup() *BucketGroup {
	bg := &BucketGroup{
		timer:         nil,
		buckets:       make([]*Bucket, 0),
		lastBucket:    -1,
		getTrigger:    make(chan bool, 1),
		totalRequests: 0,
	}

	return bg
}

func (bg *BucketGroup) selectRequest() *paxi.Request {
	var req *paxi.Request
	for i := 0; i < len(bg.buckets); i++ {
		//Evita pegar sempre do primeiro bucket
		bucketNum := (i + bg.lastBucket + 1) % len(bg.buckets)
		bucket := bg.buckets[bucketNum]
		if bucket.NumRequests >= 1 {
			req = bucket.Get()
			bg.lastBucket = bucketNum
			break
		}
	}
	return req
}

func (bg *BucketGroup) get() *paxi.Request {
	bg.lockBuckets()

	bg.CountRequests()
	var request *paxi.Request
	if int(bg.totalRequests) >= 1 {
		request = bg.selectRequest()
	} else {
		request = nil
	}

	if request != nil || timeout == 0 {
		bg.unlockBuckets()
		return request
	}

	bg.timer = time.AfterFunc(timeout, func() {
		select {
		case bg.getTrigger <- false:
		default:
		}
	})
	for _, bucket := range bg.buckets {
		bucket.group = bg
	}
	bg.unlockBuckets()

	gotRequest := <-bg.getTrigger
	bg.timer = nil

	bg.lockBuckets()
	for _, bucket := range bg.buckets {
		bucket.group = nil
	}
	if gotRequest {
		request = bg.selectRequest()
	} else {
		request = nil
	}
	bg.unlockBuckets()

	return request
}

/*
	func (bg *BucketGroup) get() *paxi.Request {
		bg.lockBuckets()

		bg.CountRequests()

		if int(bg.totalRequests) >= 1 {
			return bg.selectRequest()
		}

		log.Debugf("Waiting on bucketGroup %v for a request", bg)
		bg.timer = time.AfterFunc(timeout, func() { bg.getTrigger <- false })
		for _, bucket := range bg.buckets {
			bucket.group = bg
		}
		bg.unlockBuckets()

		gotRequest := <-bg.getTrigger
		bg.timer = nil
		log.Debugf("Got a request %v on BucketGroup %v", gotRequest, bg)
		bg.lockBuckets()
		for _, bucket := range bg.buckets {
			bucket.group = nil
		}
		if gotRequest {
			return bg.selectRequest()
		}
		bg.unlockBuckets()
		log.Debugf("Returning nothing on BucketGroup %v", bg)
		return nil
	}
*/
func (bg *BucketGroup) addBucket(bucket *Bucket) {
	bg.buckets = append(bg.buckets, bucket)
}

/*
// Only executes when buckets already locked
func (bg *BucketGroup) selectRequest() *paxi.Request {
	defer bg.unlockBuckets()
	var req *paxi.Request
	for i := 0; i < len(bg.buckets); i++ {
		//Evita pegar sempre do primeiro bucket
		bucketNum := (i + bg.lastBucket + 1) % len(bg.buckets)
		bucket := bg.buckets[bucketNum]
		if bucket.NumRequests >= 1 {
			req = bucket.Get()
			bg.lastBucket = bucketNum
		}
	}
	return req
}
*/

func (bg *BucketGroup) lockBuckets() {
	for _, bucket := range bg.buckets {
		bucket.Lock()
	}
}

func (bg *BucketGroup) unlockBuckets() {
	for _, bucket := range bg.buckets {
		bucket.Unlock()
	}
}

func (bg *BucketGroup) CountRequests() {
	bg.totalRequests = 0
	for _, bucket := range bg.buckets {
		bg.totalRequests += bucket.NumRequests
	}
}
