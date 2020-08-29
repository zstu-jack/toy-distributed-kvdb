package raftkv

import (
	"fmt"
	"labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var clientIdVar int32 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader    int
	clientId      int32
	lastRequestId int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (c *Clerk) ToString() string {
	return fmt.Sprintf("[Clerk] lastLeader=%v, clientId=%v, lastReqId=%v", c.lastLeader, c.clientId, c.lastRequestId)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.lastRequestId = 0 // seq, in case kv-server consume a request multi-times.  (record clerk's id and last request
	ck.clientId = atomic.AddInt32(&clientIdVar, 1)
	DPrintf(ck.ToString())
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddInt32(&ck.lastRequestId, 1)

	// try each server
	// Idempotence of get operation
	for {
		srv := ck.servers[ck.lastLeader]
		var reply GetReply
		DPrintf("client ---> %v", args.ToString())
		ok := srv.Call("KVServer.Get", &args, &reply)
		DPrintf("client ---> %v", reply.ToString())
		if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
			return reply.Value
		}
		//if reply.WrongLeader == true{
		//	ck.lastLeader = (ck.lastLeader+1) % len(ck.servers)
		//	continue
		//}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}

	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddInt32(&ck.lastRequestId, 1)

	//!!! Check RequestId Before Applying Instead of Before Committ.

	// Success when received reply.
	// Receive Success Msg if Applied of requestId is less than MaxRequestId.
	// One Clerk has its own RequestId.
	for {
		srv := ck.servers[ck.lastLeader]
		var reply PutAppendReply

		args.ToString()
		ok := srv.Call("KVServer.PutAppend", &args, &reply)
		reply.ToString()
		DPrintf(reply.ToString())
		if ok && reply.WrongLeader == false && reply.Err == OK {
			return
		}
		//if reply.WrongLeader {
		//	ck.lastLeader = (ck.lastLeader+1) % len(ck.servers)
		//	continue
		//}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		// redundant: one clerk, one requestId.
		//if ok && reply.Err == NeedNewReq{
		//	args.RequestId = atomic.AddInt32(&ck.lastRequestId, 1)
		//}
		time.Sleep(100 * time.Millisecond)
	}
}

// Clerk to re-send :
// 		Your solution needs to handle the case in which a leader has called Start() for a Clerk's RPC, but loses its leadership before the request is committed to the log.
// 		In this case you should arrange for the Clerk to re-send the request to other servers until it finds the new leader.
//       	- by noticing that a [different request has appeared at the index returned by Start(), or that Raft's term has changed.]
//Kv-server:
// request: Start: 		index,term,isLeader
// response:ApplyMsg:	CommandValid bool, Command      interface{}, CommandIndex int
// both are async. check whether is a same command when applied at some index.  (store some information in Command

//If a [leader fails just after committing an entry to the Raft log], the [Clerk may not receive a reply], and thus may re-send the request to another leader.
// 		Each call to Clerk.Put() or Clerk.Append() should result in just a single execution,
// 		so you will have to ensure that the re-send doesn't result in the servers executing the request twice.
//			- by set requestId for a clerk (ClientId)

// one kv-service node <----> a raft

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
