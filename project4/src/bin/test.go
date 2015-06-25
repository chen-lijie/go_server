package main

import "kvpaxos"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"


func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "kv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func cleanup(kva []*kvpaxos.KVPaxos) {
	for i := 0; i < len(kva); i++ {
		if kva[i] != nil {

			kva[i].Kill()
		}
	}
}


func main() {
	runtime.GOMAXPROCS(4)

	const nservers = 3
	var kva []*kvpaxos.KVPaxos = make([]*kvpaxos.KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		kvh[i] = port("basic", i)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = kvpaxos.StartServer(kvh, i)
	}

	ck := kvpaxos.MakeClerk(kvh)
	var cka [nservers]*kvpaxos.Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = kvpaxos.MakeClerk([]string{kvh[i]})
	}

	ck.Put("clj", "fhq")
	fmt.Println(ck.Get("clj"))
	ck.Put("alj", "alj")
	fmt.Println(ck.Get("alj"))	
	fmt.Println(ck.Dump())
	fmt.Println(ck.Count())
	fmt.Println(ck.Del("alj"))
	fmt.Println(ck.Count())
	time.Sleep(1 * time.Second)
}
