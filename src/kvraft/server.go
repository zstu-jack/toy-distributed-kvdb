package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"net/http"
	_ "net/http"       // for debug
	_ "net/http/pprof" // for debug
	"raft"
	"sync"
	"time"
)

const Debug = 0
const WaitRspTimeOut = 200

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientId  int32
	RequestId int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	databases    map[string]string
	clientMaxSeq map[int32]int32 // ClientId  -> max requestId
	replyChan    sync.Map        // [int32]chan raft.ApplyMsg // requestId -> chan
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// index, term, isLeader
	op := Op{OpType: "Get", Key: args.Key, Value: "", ClientId: args.ClientId, RequestId: args.RequestId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// use raft index. (unique)
	ch := make(chan GetReply, 100)
	kv.replyChan.Store(index, ch)

	DPrintf("Server(%v) <---- %v", kv.me, args.ToString())

	select {
	// Waiting for Command Applied
	case applyReply := <-ch:
		reply.WrongLeader = applyReply.WrongLeader
		reply.Err = applyReply.Err
		reply.Value = applyReply.Value
		if applyReply.command.RequestId != args.RequestId || applyReply.command.ClientId != args.ClientId {
			reply.Err = NeedNewReq
			reply.WrongLeader = true
		}
		// access databases.
	case <-time.After(time.Duration(WaitRspTimeOut) * time.Millisecond):
		reply.WrongLeader = false
		reply.Err = "TimeOut"

		kv.mu.Lock()
		kv.replyChan.Delete(index)
		kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan GetReply, 100)
	kv.replyChan.Store(index, ch)

	DPrintf("Server(id=%v) index=%v isLeader=%v <---- %v", kv.me, index, isLeader, args.ToString())

	select {
	// Wait For Command Been Applied
	case applyReply := <-ch:
		reply.Err = applyReply.Err
		reply.WrongLeader = applyReply.WrongLeader
		// retry if not same term(Not the expected commit).
		if applyReply.command.RequestId != args.RequestId || applyReply.command.ClientId != args.ClientId {
			reply.Err = NeedNewReq
			reply.WrongLeader = true
		}
		return
	case <-time.After(time.Duration(WaitRspTimeOut) * time.Millisecond):
		DPrintf("Wait For Command Reply TimeOut")
		reply.WrongLeader = true
		reply.Err = TimeOut

		kv.mu.Lock()
		kv.replyChan.Delete(index)
		kv.mu.Unlock()
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) snapshot(index int) {
	if kv.maxraftstate == -1 || kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientMaxSeq) // avoid multi-times apply
	e.Encode(kv.databases)
	snapshot := w.Bytes()
	kv.rf.TakeSnapshot(index, snapshot) // do snapshot
}

func (kv *KVServer) readSnapshot() { // done snapshot. notify by raft.
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) <= 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.clientMaxSeq) != nil ||
		d.Decode(&kv.databases) != nil {
		log.Panic("?????")
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	go func() {
		// for debug
		DPrintf("%v", http.ListenAndServe("localhost:6060", nil))
		// http://localhost:6060/debug/pprof
		// http://localhost:6060/debug/pprof/goroutine?debug=2
	}()

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.databases = make(map[string]string)
	kv.clientMaxSeq = make(map[int32]int32)

	kv.readSnapshot()

	go func() {
		for {
			applyMsg := <-kv.applyCh
			kv.mu.Lock()
			if applyMsg.CommandValid {

				// DPrintf(applyMsg.ToString())
				getReply := GetReply{WrongLeader: false, Err: OK, Value: "", command: applyMsg.Command.(Op)}

				// BUG: apply even if there exist no client.
				cmd := applyMsg.Command.(Op)
				op := applyMsg.Command.(Op)
				DPrintf("commandIndex=%v clientId=%v, requestId=%v Optype=%v", applyMsg.CommandIndex, applyMsg.Command.(Op).ClientId, applyMsg.Command.(Op).RequestId, op.OpType)
				if op.OpType == "Get" {
					value, ok := kv.databases[op.Key]
					if ok {
						getReply.Value = value
					} else {
						getReply.Value = ""
						getReply.Err = ErrNoKey
					}
				} else {
					maxReqId, ok := kv.clientMaxSeq[cmd.ClientId]
					if !ok || cmd.RequestId > maxReqId {
						DPrintf("ok = %v, ReqId=%v MaxId=%v replyClientId=%v, argClientId=%v", ok, cmd.RequestId, maxReqId, cmd.ClientId, cmd.ClientId)

						if op.OpType == "Put" {
							kv.databases[op.Key] = op.Value
						} else {
							_, ok := kv.databases[op.Key]
							if ok {
								kv.databases[op.Key] = kv.databases[op.Key] + op.Value
							} else {
								kv.databases[op.Key] = op.Value
							}
						}
						kv.clientMaxSeq[cmd.ClientId] = cmd.RequestId
					}
				}

				// notify
				//kv.replyChan.Range(func(key, value interface{}) bool {
				//	fmt.Println(key, reflect.TypeOf(value))
				//	return true
				//})
				interfaceCh, ok := kv.replyChan.Load(applyMsg.CommandIndex)
				if ok {
					ch := interfaceCh.(chan GetReply)
					ch <- getReply // notify channel
				}
				kv.replyChan.Delete(applyMsg.CommandIndex)
				kv.snapshot(applyMsg.CommandIndex) // do snapshot
			} else {
				if cmd, ok := applyMsg.Command.(string); ok {
					if cmd == "ReadSnapshot" { // done snapshot
						kv.readSnapshot()
					}
				}
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
