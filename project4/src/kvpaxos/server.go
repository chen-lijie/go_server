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
	ClientID  int
	ClientSeq int
	Type      string
	Key       string
	Value     string
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

func (kv *KVPaxos) WaitAllDone(startnum int, seqnum int) (string, bool) {
	result := ""
	success := false
	for i := startnum; i <= seqnum; i++ {
		dv := kv.px.DecideList[i]
		decided, _ := dv.(Op)

		if decided.Type == GET {
			res, exist := kv.database[decided.Key]
			if exist {
				result = res
				success = true
			} else {
				success = false
			}
		} else if decided.Type == INSERT {
			_, exist := kv.database[decided.Key]
			if exist {
				success = false
			} else {
				kv.database[decided.Key] = decided.Value
				success = true
			}
			result = ""
		} else if decided.Type == UPDATE {
			_, exist := kv.database[decided.Key]
			if exist {
				kv.database[decided.Key] = decided.Value
				success = true
			} else {
				success = false
			}
			result = ""
		} else if decided.Type == DELETE {
			res, exist := kv.database[decided.Key]
			if exist {
				delete(kv.database, decided.Key)
				result = res
				success = true
			} else {
				result = ""
				success = false
			}
		} else if decided.Type == COUNT {
			result = strconv.Itoa(len(kv.database))
			success = true
		} else if decided.Type == DUMP {
			items := make([][]string, len(kv.database))
			i := 0
			for k, v := range kv.database {
				items[i] = make([]string,2)
				items[i][0]=k
				items[i][1]=v
				i++
			}
			response, _ := json.Marshal(items)
			result = string(response)
			success = true
		}
		kv.px.Done(i)
	}
	return result, success
}

func (kv *KVPaxos) Insert(key string, value string, id int, seq int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{id, seq, INSERT, key, value}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, op)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue == op {
				break
			}
			seqnum++
		}
	}

	kv.seq = seqnum + 1
	_, ok := kv.WaitAllDone(startnum, seqnum)
	return ok
}

func (kv *KVPaxos) Update(key string, value string, id int, seq int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{id, seq, UPDATE, key, value}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, op)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue == op {
				break
			}
			seqnum++
		}
	}

	kv.seq = seqnum + 1
	_, ok := kv.WaitAllDone(startnum, seqnum)
	return ok
}

func (kv *KVPaxos) Delete(key string, id int, seq int) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{id, seq, DELETE, key, ""}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, op)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue == op {
				break
			}
			seqnum++
		}
	}

	kv.seq = seqnum + 1
	value, ok := kv.WaitAllDone(startnum, seqnum)
	return value, ok
}

func (kv *KVPaxos) Count(id int, seq int) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{id, seq, COUNT, "", ""}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, op)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue == op {
				break
			}
			seqnum++
		}
	}

	kv.seq = seqnum + 1
	value, _ := kv.WaitAllDone(startnum, seqnum)
	thankstofhq, _ := strconv.Atoi(value)
	return thankstofhq
}

func (kv *KVPaxos) Dump(id int, seq int) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{id, seq, DUMP, "", ""}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, op)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue == op {
				break
			}
			seqnum++
		}
	}

	kv.seq = seqnum + 1
	value, _ := kv.WaitAllDone(startnum, seqnum)
	return value
}

func (kv *KVPaxos) Get(key string, id int, seq int) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{id, seq, GET, key, ""}
	seqnum := kv.seq
	startnum := seqnum
	for {
		kv.px.Start(seqnum, op)
		ddvalue := kv.WaitDecided(seqnum)
		if decidedvalue, ok := ddvalue.(Op); ok {
			if decidedvalue == op {
				break
			}
			seqnum++
		}
	}

	kv.seq = seqnum + 1
	value, ok := kv.WaitAllDone(startnum, seqnum)
	return value, ok
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
	kv.unreliable = false
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
				} else if kv.unreliable && (rand.Int63()%1000) < 100 {
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
