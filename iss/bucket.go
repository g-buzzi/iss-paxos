// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iss

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Represents a client request and stores request-specific metadata.
type BucketItem struct {
	Request paxi.Request

	// Digest of the request.
	Digest []byte

	// Flag indicating whether the request signature has been verified.
	Verified bool

	// Request is "in flight", i.e., has been added to or observed in some (protocol-specific) proposal message.
	// This flag tracks duplication.
	// A request should be marked as in flight upon being either added to or upon encountered in a proposal message.
	// If a request is already proposed it should not be found in any other proposal message.
	InFlight bool

	// Requests are stored in a doubly linked list in the bucket for constant-time (random-access) adding and removing
	// while still being able to use it as a FIFO queue.
	// We do not encapsulate the Request in a container "Element" struct to avoid allocations of those elements.
	// Both Next and Prev must be set to nil when the request is not in a bucket.
	Next *BucketItem
	Prev *BucketItem
}

func newBucketItem(req *paxi.Request) BucketItem {
	return BucketItem{
		Request:  *req,
		Digest:   Digest(req), //to implement
		Verified: false,
		InFlight: false,
		Next:     nil,
		Prev:     nil,
	}
}

func Digest(req *paxi.Request) []byte {
	buffer := make([]byte, 0, 0)
	sn := make([]byte, 4)
	binary.LittleEndian.PutUint32(sn, uint32(req.Command.CommandID))
	buffer = append(buffer, sn...)
	id := make([]byte, 4)
	clID := req.Command.ClientID
	binary.LittleEndian.PutUint32(id, uint32(int64(clID.Zone())<<32+int64(clID.Node())<<32))
	buffer = append(buffer, id...)
	return buffer
}

// Represents a single bucket of client requests.
// The contents of the Bucket must be kept consistent with Buffers. Therefore, requests are added here
// only after being successfully added to Buffers. Moreover, requests removed from here must be removed
// from Buffers as well.
type Bucket struct {
	// Any modification (or read of potentially concurrently modified values)
	// of the request buffer requires acquiring this lock.
	// Using this syntax, one can use .Lock() and .Unlock() methods directly on the Bucket.
	sync.Mutex

	// The bucket id.
	// Currently only used for printing debug messages.
	id int

	// BucketGroup waiting to cut a batch of requests (also) from this Bucket.
	// If no BucketGroup is waiting for this Bucket, group nil.
	// Used for notifying the BucketGroup waiting to cut a batch about request additions.
	// Modifications of this field also require the Bucket to be locked.
	Group *BucketGroup

	// Number of requests currently in the bucket.
	numRequests int

	// Index of requests in this bucket by their request ID (client ID and client sequence number)
	// Always contains a superset of the requests in the doubly linked list and is garbage-collected
	// when watermarks are advanced.
	reqIndex map[int64]*BucketItem

	// Start of a doubly linked list of requests in the bucket.
	// Necessary for constant-time adding and removing, while still being able to traverse in consistent order.
	// If set to nil, no requests are in the bucket (and LastRequest also must be nil).
	FirstRequest *BucketItem

	// End of the doubly linked list of request.
	// Pointer to the end necessary for using the list as a FIFO queue.
	// If set to nil, no requests are in the bucket (and FirstRequest also must be nil).
	LastRequest *BucketItem
}

func NewBucket(id int) *Bucket {
	return &Bucket{
		id: id,
	}
}

// Get the ID of the bucket that corresponds to its position in the Buckets slice.
// Currently only used for printing debug messages.
func (b *Bucket) GetId() int {
	return b.id
}

// Counts all the requests in the bucket and returns their number.
func (b *Bucket) Len() int {
	return b.numRequests
}

// Wrapper for addNoLock() that acquires the bucket lock.
func (b *Bucket) AddRequest(req *paxi.Request) (*BucketItem, bool) {
	bucketItem := newBucketItem(req)

	b.Lock()
	defer b.Unlock()

	return b.addNoLock(&bucketItem)
}

// Adds multiple requests.
// Batching wrapper for multiple calls to addNoLock(), only acquiring the bucket lock once.
func (b *Bucket) AddRequests(reqs []*paxi.Request) ([]*paxi.Request, []*paxi.Request, []*paxi.Request) {
	b.Lock()
	defer b.Unlock()

	added := make([]*paxi.Request, 0, len(reqs))
	toRetry := make([]*paxi.Request, 0, len(reqs))
	failed := make([]*paxi.Request, 0, len(reqs))

	for _, req := range reqs {
		request, retry := b.addNoLock(req)

		if request != nil {
			added = append(added, request)
		} else if retry {
			toRetry = append(toRetry, req)
		} else {
			failed = append(failed, req)
		}
	}

	return added, toRetry, failed
}

// Look up request in the Bucket's request index.
// Note that the request might be present in the index while already removed from the doubly-linked list.
// This is intended and prevents (wrongly) re-adding requests that have already been removed.
//func (b *Bucket) lookup(reqMsg *pb.ClientRequest, digest []byte)

// Adds a new client request to the Bucket.
// If a BucketGroup is waiting for Requests being added to this Bucket,
// notifies the BucketGroup about a the addition.
// This might make the BucketGroup cut a new batch.
// The caller must provide the digest of the request and a flag whether the request signature has been verified.
// addNoLock() returns a pointer to the added request (or to the present request if the request has already been present).
// If adding the request fails, nil is returned and the bool flag indicates whether retrying to add the request
// after verifying its signature is meaningful.
// ATTENTION: The bucket needs to be locked when calling this method!
func (b *Bucket) addNoLock(newReq *BucketItem) (*BucketItem, bool) {
	// Convenience variable
	//reqID := struct{
	//	ClId int32
	//	ClSn int32
	//}{reqMsg.RequestId.ClientId, reqMsg.RequestId.ClientSn}
	clID := newReq.Request.Command.ClientID
	clSN := newReq.Request.Command.CommandID
	reqID := int64(clID.Zone())<<32 + int64(clID.Node())<<32 + int64(clSN)

	// Look up request (in the bucket)
	oldReq, ok := b.reqIndex[reqID]

	// If Request with same digest already is in the bucket, return that Request
	if ok && bytes.Compare(oldReq.Digest, newReq.Digest) == 0 {

		//logger.Trace().
		//	Int("bucketId", b.id).
		//	Int("len", b.Len()).
		//	Int32("clId", clID).
		//	Int32("clSn", clSN).
		//	Msg("Returning existing request from bucket.")

		return oldReq, false

		// If a verified request with a different digest is present in a bucket, ignore the new request.
		// If the already present request was not verified, it might have been submitted by a faulty client,
		// and the new request might be the correct one (that's why we need the other branches too).
	} else if ok && (oldReq.Verified) {
		return nil, false

		// The request already present has not yet been verified (and verification is enabled).
	} else if ok {

		// If the new request is verified, replace old request.
		// (No need to check the old request's signature.)
		if newReq.Verified {

			//	logger.Trace().
			//	Int("bucketId", b.id).
			//		Int("len", b.Len()).
			//		Int32("clId", clID).
			//		Int32("clSn", clSN).
			//		Msg("Replacing existing request in bucket.")

			b.removeNoLock(oldReq)
			b.append(newReq)
			b.reqIndex[reqID] = newReq

			return newReq, false

			// If the new request is not verified, request retrying with verified request
		} else {

			//	logger.Trace().
			//		Int("bucketId", b.id).
			//		Int("len", b.Len()).
			//		Int32("clId", clID).
			//		Int32("clSn", clSN).
			//		Msg("Retry adding request.")

			return nil, true
		}

		// If no request with the given ID is present in the bucket
	} else {

		// Request retrial if only verified requests are allowed to go in the bucket and request is not verified.
		if !newReq.Verified {
			return nil, true

			// If request is either already verified or no verification is required to add reqeusts to the backet,
			// add the new request and notify BucketGroup.
		} else {
			//logger.Trace().
			//	Int("bucketId", b.id).
			//	Int("len", b.Len()).
			//	Int32("clId", clID).
			//	Int32("clSn", clSN).
			//	Msg("Adding new request to bucket.")

			b.append(newReq)
			b.reqIndex[reqID] = newReq

			// If the Bucket is part of a BucketGroup that is waiting for more requests to arrive,
			// notify the BucketGroup.
			if b.Group != nil {
				b.Group.RequestAdded()
			}

			return newReq, false
		}
	}
}

// Append request to the end of the doubly linked list.
// ATTENTION: The bucket must be locked when calling this function!
func (b *Bucket) append(r *BucketItem) {

	// r.Prev and r.Next must be nil here! (Just a sanity check.)
	if r.Prev != nil || r.Next != nil {
		//logger.Fatal().
		//	Int32("clId", r.Msg.RequestId.ClientId).
		//	Int32("clSn", r.Msg.RequestId.ClientSn).
		//	Int("bucketId", b.id).
		//	Msg("Adding request that is already added somewhere to bucket.")
	}

	// Append request to list
	if b.FirstRequest == nil {
		b.FirstRequest = r
	} else {
		b.LastRequest.Next = r
		r.Prev = b.LastRequest
	}
	b.LastRequest = r
	b.numRequests++
}

func (b *Bucket) Prepend(req *BucketItem) {
	b.Lock()
	defer b.Unlock()

	// Sanity check: The request must already be present in the bucket's index
	clID := req.Request.Command.ClientID
	clSN := req.Request.Command.CommandID
	reqID := int64(clID.Zone())<<32 + int64(clID.Node())<<32 + int64(clSN)
	if _, ok := b.reqIndex[reqID]; !ok {
		panic("Trying to re-insert (prepend) request that is not in the index.")
	}

	// Only prepend request if it is not already inserted in the list
	if req.Prev == nil && req.Next == nil && b.FirstRequest != req {

		if b.FirstRequest == nil {
			b.FirstRequest = req
			b.LastRequest = req
		} else {
			b.FirstRequest.Prev = req
			req.Next = b.FirstRequest
			b.FirstRequest = req
		}

		// Update the bucket's request counter.
		b.numRequests++

		// Notify bucket group if a batch is being cut.
		if b.Group != nil {
			b.Group.RequestAdded()
		}
	}
}

// Adds request to the start of the bucket.
// Required for resurrecting requests.
// ATTENTION! The requests must not be already present in the bucket, otherwise PrependMultiple() corrupts the Bucket state.
// TODO: Batch the additions, passing a slice of requests instead of going one-by-one
func (b *Bucket) PrependMultiple(reqs []*BucketItem) {

	// Return if nothing is to be prepended
	if len(reqs) == 0 {
		return
	}

	// Prepare doubly linked list of Requests to be prepended to the Bucket.
	for i := 0; i < len(reqs)-1; i++ {
		reqs[i].Next = reqs[i+1]
		reqs[i+1].Prev = reqs[i]
	}
	// First and last request of the newly created chain.
	start := reqs[0]
	end := reqs[len(reqs)-1]

	// Hook the prepared chain of Requests to the start of the (locked) bucket
	b.Lock()
	defer b.Unlock()

	if b.FirstRequest == nil {
		b.FirstRequest = start
		b.LastRequest = end
	} else {
		b.FirstRequest.Prev = end
		end.Next = b.FirstRequest
		b.FirstRequest = start
	}

	// Update the bucket's request counter.
	b.numRequests += len(reqs)
}

// Removes the first up to n Requests from the Bucket and appends them to dest.
// Returns the resulting slice obtained by appending the Requests to dest.
// ATTENTION: Bucket must be LOCKED when calling this method.
func (b *Bucket) RemoveFirst(n int, dest []*paxi.Request) []*paxi.Request {

	// While there are still Requests in the bucket and the limit has not been reached.
	for ; b.numRequests > 0 && n > 0; n-- {

		if b.FirstRequest == nil {
			log.Debugf("Error FirstRequest is nil!")
		}
		// Move the first request from the bucket into the destination slice.
		dest = append(dest, &b.FirstRequest.Request)
		b.removeNoLock(b.FirstRequest)
	}

	return dest
}

// Removes each Request in req from the Bucket if it is present, but NOT from the index (see removeNoLock()).
func (b *Bucket) Remove(reqs []*paxi.Request) {
	b.Lock()
	defer b.Unlock()

	for _, req := range reqs {
		clID := req.Command.ClientID
		clSN := req.Command.CommandID
		reqID := int64(clID.Zone())<<32 + int64(clID.Node())<<32 + int64(clSN)
		bucketItem := b.reqIndex[reqID]
		b.removeNoLock(bucketItem)
	}
}

// Removes a request from the bucket without acquiring the bucket lock.
// ATTENTION: Does not (and must not) remove the request from the index.
//
//	The index can be cleaned up only after the client watermarks have been updated,
//	to prevent the situation where, in the same epoch, a request is received from a leader,
//	added to the bucket, committed and removed from the bucket, and then added again after a late reception
//	from the client.
//
// ATTENTION: Bucket must be LOCKED when calling this method.
func (b *Bucket) removeNoLock(req *BucketItem) {

	// Convenience variable
	//reqID := struct{
	//	ClId int32
	//	ClSn int32
	//}{req.Msg.RequestId.ClientId, req.Msg.RequestId.ClientSn}
	clID := req.Request.Command.ClientID
	clSN := req.Request.Command.CommandID

	// Do not remove requests if they are not in the bucket
	// (Note that the Prev and Next fields need to be consistently set to nil on request removal for this to work.)
	if req.Next == nil && req.Prev == nil && b.FirstRequest != req { // No need to check b.LastRequest
		log.Debugf("Not removing request. Request not in bucket")
		/*
			logger.Trace().Int("bucketId", b.id).
				Int32("clId", clID).
				Int32("clSn", clSN).
				Msg("Not removing request. Request not in bucket.")
		*/
		return
	}

	log.Debugf("Removing request from bucket.")
	/*
		logger.Trace().
			Int("bucketId", b.id).
			Int("len", b.Len()).
			Int32("clId", clID).
			Int32("clSn", clSN).
			Msg("Removing request from bucket.")
	*/

	// Remove request from the doubly linked list of requests.
	if req.Next != nil { // Request is not last in the list
		req.Next.Prev = req.Prev
	} else { // Request is last in the list
		b.LastRequest = req.Prev
	}
	if req.Prev != nil { // Request is not first in the list
		req.Prev.Next = req.Next
	} else { // Request is first in the list
		b.FirstRequest = req.Next
	}

	// Mark request as not in a bucket.
	req.Prev = nil
	req.Next = nil

	// Decrement number of requests in batch.
	b.numRequests--
}
