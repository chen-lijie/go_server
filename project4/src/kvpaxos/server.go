package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
	"encoding/json"
)

const Debug = 0

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
	ClientID  int64
	ClientSeq int
	Type      string
	Key       string
	Value     string
	DoHash    bool
}

type OpResult struct {
	ClientSeq     int
	PreviousValue string
	//Type          string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq          int
	database     map[string]string
	clientLastOp map[int64]OpResult
}

func (kv *KVPaxos) WaitDecided(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		decided, value := kv.px.Status(seq)
		if decided {
			return value
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) WaitAllDone(startnum int, seqnum int) string {
	result := ""
	for i := startnum; i <= seqnum; i++ {
		dv := kv.px.DecideList[i]
		decided, _ := dv.(Op)

		previous := ""
		if decided.Type == GET {
			previous = kv.database[decided.Key]
		} else if decided.Type == COUNT {
			previous = strconv.Itoa(len(kv.database))
		} else if decided.Type == DEL {
			v, e := kv.database[decided.Key]
			if !e {
				previous = v
			} else {
				delete(kv.database, decided.Key)
				previous = v
			}
		} else if decided.Type == DUMP {
			items := make([][]string, len(kv.database))
			i := 0
			for k, v := range kv.database {
				items[i] = make([]string,2)
				items[i][0]=k
				items[i][1]=v
				i++
			}
			response,_ := json.Marshal(items)
			previous = string(response)
		}
		if decided.Type == PUT {
			kv.database[decided.Key] = decided.Value
		} else if decided.Type == GET {
		} else if decided.Type == COUNT {
		}
		kv.clientLastOp[decided.ClientID] = OpResult{decided.ClientSeq, previous}
		kv.px.Done(i)
		result = previous
	}
	return result
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ClientSeq == kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Value = kv.clientLastOp[args.ClientID].PreviousValue
		reply.Err = OK
		return nil
	} else if args.ClientSeq < kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Err = "Something Wrong."
		return nil
	}

	getop := Op{args.ClientID, args.ClientSeq, GET, args.Key, "", false}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, getop)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue != getop {
				seqnum++
			} else {
				break
			}
		}
	}

	kv.seq = seqnum + 1
	reply.Value = kv.WaitAllDone(startnum, seqnum)
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) Del(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ClientSeq == kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Value = kv.clientLastOp[args.ClientID].PreviousValue
		reply.Err = OK
		return nil
	} else if args.ClientSeq < kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Err = "Something Wrong."
		return nil
	}

	delop := Op{args.ClientID, args.ClientSeq, DEL, args.Key, "", false}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, delop)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue != delop {
				seqnum++
			} else {
				break
			}
		}
	}

	kv.seq = seqnum + 1
	reply.Value = kv.WaitAllDone(startnum, seqnum)
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) Count(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ClientSeq == kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Value = kv.clientLastOp[args.ClientID].PreviousValue
		reply.Err = OK
		return nil
	} else if args.ClientSeq < kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Err = "Something Wrong."
		return nil
	}

	countop := Op{args.ClientID, args.ClientSeq, COUNT, "", "", false}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, countop)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue != countop {
				seqnum++
			} else {
				break
			}
		}
	}

	kv.seq = seqnum + 1
	reply.Value = kv.WaitAllDone(startnum, seqnum)
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) Dump(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ClientSeq == kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Value = kv.clientLastOp[args.ClientID].PreviousValue
		reply.Err = OK
		return nil
	} else if args.ClientSeq < kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Err = "Something Wrong."
		return nil
	}

	dumpop := Op{args.ClientID, args.ClientSeq, DUMP, "", "", false}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, dumpop)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue != dumpop {
				seqnum++
			} else {
				break
			}
		}
	}

	kv.seq = seqnum + 1
	reply.Value = kv.WaitAllDone(startnum, seqnum)
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ClientSeq == kv.clientLastOp[args.ClientID].ClientSeq {
		reply.PreviousValue = kv.clientLastOp[args.ClientID].PreviousValue
		reply.Err = OK
		return nil
	} else if args.ClientSeq < kv.clientLastOp[args.ClientID].ClientSeq {
		reply.Err = "Something Wrong."
		return nil
	}

	putop := Op{args.ClientID, args.ClientSeq, PUT, args.Key, args.Value, args.DoHash}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, putop)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue != putop {
				seqnum++
			} else {
				break
			}
		}
	}
	kv.seq = seqnum + 1
	reply.PreviousValue = kv.WaitAllDone(startnum, seqnum)
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) Kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.seq = 0
	kv.database = make(map[string]string)
	kv.clientLastOp = make(map[int64]OpResult)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.Kill()
			}
		}
	}()

	return kv
}