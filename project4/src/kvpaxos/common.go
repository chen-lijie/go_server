package kvpaxos

import "hash/fnv"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	PUT      = "Put"
	GET      = "Get"
	COUNT    = "Count"
	PUTHASH  = "PutHash"
	DUMP     = "Dump"
	DEL      = "Delete"
)

type Err string

type PutArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	DoHash    bool // For PutHash
	ClientID  int64
	ClientSeq int
	//Id     int64
	//Seq    int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key       string
	Unique    int64
	ClientID  int64
	ClientSeq int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
