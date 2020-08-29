package raftkv

import "fmt"

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NeedNewReq = "NeedNewReq"
	TimeOut    = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int32
	ClientId  int32
}

func (args *PutAppendArgs) ToString() string {
	return fmt.Sprintf("PutAppendArgs [ReqId=%v][ClientId=%v][Key=%v][Value=%v][Op=%v]\n", args.RequestId, args.ClientId, args.Key, args.Value, args.Op)
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

func (args *PutAppendReply) ToString() string {
	return fmt.Sprintf("PutAppendReply [wrong=%v][err=%v]\n", args.WrongLeader, args.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int32
	ClientId  int32
}

func (args *GetArgs) ToString() string {

	return fmt.Sprintf("GetArgs [ReqId=%v][ClientId=%v][Key=%v]\n", args.RequestId, args.ClientId, args.Key)
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

func (args *GetReply) ToString() string {
	return fmt.Sprintf("GetReply [wrong=%v][err=%v][value=%v]\n", args.WrongLeader, args.Err, args.Value)
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
