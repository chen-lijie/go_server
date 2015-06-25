package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"sync"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	mu       sync.Mutex
	seq      int
	clientID int64
	ch       map[int]chan int
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seq = 0
	ck.clientID = nrand()
	ck.ch = make(map[int]chan int)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++

	args := &GetArgs{}
	args.Key = key
	args.ClientSeq = ck.seq
	args.ClientID = ck.clientID
	for {
		for _, kvserver := range ck.servers {
			var reply GetReply
			ok := call(kvserver, "KVPaxos.Get", args, &reply)
			//if ok {
			if ok && reply.Err == OK {
				return reply.Value
				//} else {
				//continue
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
}
func (ck *Clerk) Del(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++

	args := &GetArgs{}
	args.Key = key
	args.ClientSeq = ck.seq
	args.ClientID = ck.clientID
	for {
		for _, kvserver := range ck.servers {
			var reply GetReply
			ok := call(kvserver, "KVPaxos.Del", args, &reply)
			//if ok {
			if ok && reply.Err == OK {
				return reply.Value
				//} else {
				//continue
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Count() string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++

	args := &GetArgs{}
	args.ClientSeq = ck.seq
	args.ClientID = ck.clientID
	for {
		for _, kvserver := range ck.servers {
			var reply GetReply
			ok := call(kvserver, "KVPaxos.Count", args, &reply)
			//if ok {
			if ok && reply.Err == OK {
				return reply.Value
				//} else {
				//continue
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Dump() string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++

	args := &GetArgs{}
	args.ClientSeq = ck.seq
	args.ClientID = ck.clientID
	for {
		for _, kvserver := range ck.servers {
			var reply GetReply
			ok := call(kvserver, "KVPaxos.Dump", args, &reply)
			//if ok {
			if ok && reply.Err == OK {
				return reply.Value
				//} else {
				//continue
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
}


//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++

	args := &PutArgs{}
	args.Key = key
	args.Value = value
	args.DoHash = dohash
	args.ClientID = ck.clientID
	args.ClientSeq = ck.seq
	for {
		for _, kvserver := range ck.servers {
			var reply PutReply
			ok := call(kvserver, "KVPaxos.Put", args, &reply)
			//if ok {
			if ok && reply.Err == OK {
				return reply.PreviousValue
				//} else {
				//continue
			}
			//time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
