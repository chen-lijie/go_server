package kvpaxos

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "math/rand"

func check(t *testing.T, ck *Clerk, key string, value string) {
  v := ck.Get(key)
//	fmt.Println("finish getting check")
  if v != value {
    t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
  }
}

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

func cleanup(kva []*KVPaxos) {
  for i := 0; i < len(kva); i++ {
    if kva[i] != nil {
      kva[i].Kill()
    }
  }
}

func TestUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const nservers = 3
  var kva []*KVPaxos = make([]*KVPaxos, nservers)
  var kvh []string = make([]string, nservers)
  defer cleanup(kva)

  for i := 0; i < nservers; i++ {
    kvh[i] = port("un", i)
  }
  for i := 0; i < nservers; i++ {
    kva[i] = StartServer(kvh, i)
    kva[i].unreliable = true
  }

  ck := MakeClerk(kvh)
  var cka [nservers]*Clerk
  for i := 0; i < nservers; i++ {
    cka[i] = MakeClerk([]string{kvh[i]})
  }

  fmt.Printf("Test: Basic put/get, unreliable ...\n")

  ck.Put("a", "aa")
  check(t, ck, "a", "aa")

  cka[1].Put("a", "aaa")

  check(t, cka[2], "a", "aaa")
  check(t, cka[1], "a", "aaa")
  check(t, ck, "a", "aaa")

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Sequence of puts, unreliable ...\n")

  for iters := 0; iters < 6; iters++ {
  const ncli = 5
    var ca [ncli]chan bool
    for cli := 0; cli < ncli; cli++ {
      ca[cli] = make(chan bool)
      go func(me int) {
        ok := false
        defer func() { ca[me] <- ok }()
        sa := make([]string, len(kvh))
        copy(sa, kvh)
        for i := range sa {
          j := rand.Intn(i+1)
          sa[i], sa[j] = sa[j], sa[i]
        }
        myck := MakeClerk(sa)
        key := strconv.Itoa(me)
        pv := myck.Get(key)
        ov := myck.Put(key, "0")
        if ov != pv {
          t.Fatalf("For key %s wrong value; expected %s but got %s",key ,pv , ov)
        }
        ov = myck.Put(key, "1")
        pv = "0";
        if ov != pv {
          t.Fatalf("wrong value; expected %s but got %s", pv, ov)
        }
        ov = myck.Put(key, "2")
        pv = "1"
        if ov != pv {
          t.Fatalf("wrong value; expected %s", pv)
        }
        nv := "2"
        time.Sleep(100 * time.Millisecond)
        if myck.Get(key) != nv {
          t.Fatalf("wrong value")
        }
        if myck.Get(key) != nv {
          t.Fatalf("wrong value")
        }
        ok = true
      }(cli)
    }
    for cli := 0; cli < ncli; cli++ {
      x := <- ca[cli]
      if x == false {
        t.Fatalf("failure")
      }
    }
  }

  fmt.Printf("  ... Passed\n")

  fmt.Printf("Test: Concurrent clients, unreliable ...\n")

  for iters := 0; iters < 20; iters++ {
    const ncli = 15
    var ca [ncli]chan bool
    for cli := 0; cli < ncli; cli++ {
      ca[cli] = make(chan bool)
      go func(me int) {
        defer func() { ca[me] <- true }()
        sa := make([]string, len(kvh))
        copy(sa, kvh)
        for i := range sa {
          j := rand.Intn(i+1)
          sa[i], sa[j] = sa[j], sa[i]
        }
        myck := MakeClerk(sa)
        if (rand.Int() % 1000) < 500 {
          myck.Put("b", strconv.Itoa(rand.Int()))
        } else {
          myck.Get("b")
        }
      }(cli)
    }
    for cli := 0; cli < ncli; cli++ {
      <- ca[cli]
    }

    var va [nservers]string
    for i := 0; i < nservers; i++ {
      va[i] = cka[i].Get("b")
      if va[i] != va[0] {
        t.Fatalf("mismatch; 0 got %v, %v got %v", va[0], i, va[i])
      }
    }
  }

  fmt.Printf("  ... Passed\n")

  time.Sleep(1 * time.Second)
}
