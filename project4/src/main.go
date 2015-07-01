package main

import (
	"fmt"
	"net/http"
	"time"
	"io/ioutil"
	"encoding/json"
	"sync"
	"strconv"
	"os"
	"strings"
)
import "kvpaxos"
import "runtime"

type DataManager struct{
	seq int
	me int
	mu sync.Mutex
}

var kvp *kvpaxos.KVPaxos

func NewDataManager(me int) *DataManager{
	var m *DataManager = new(DataManager)
	m.seq = 0
	m.me = me
	return m
}

func (m * DataManager) Insert(key string,value string) bool {
	m.mu.Lock()
	tmp := m.seq
	m.seq += 1
	m.mu.Unlock()

	return kvp.Insert(key, value, tmp, m.me)
}

func (m * DataManager) Delete(key string) (string, bool) {
	m.mu.Lock()
	tmp := m.seq
	m.seq += 1
	m.mu.Unlock()

	v, present := kvp.Delete(key, tmp, m.me)
	if !present {
		return v,false
	}
	return v,true
}

func (m * DataManager) Get(key string) (string, bool) {
	m.mu.Lock()
	tmp := m.seq
	m.seq += 1
	m.mu.Unlock()

	v, present := kvp.Get(key, tmp, m.me)
	if !present {
		return v,false
	}
	return v,true
}

func (m * DataManager) Update(key string,value string) bool {
	m.mu.Lock()
	tmp := m.seq
	m.seq += 1
	m.mu.Unlock()

	present := kvp.Update(key, value, tmp, m.me)
	if !present {
		return false
	}
	return true
}

func (m * DataManager) CountKey() int {
	m.mu.Lock()
	tmp := m.seq
	m.seq += 1
	m.mu.Unlock()

	v := kvp.Count(tmp, m.me)
	return v
}

func (m * DataManager) DumpArray() string {
	m.mu.Lock()
	tmp := m.seq
	m.seq += 1
	m.mu.Unlock()

	s := kvp.Dump(tmp, m.me)
	return s
}


var datamanager *DataManager

func NotImplementedHandler(w http.ResponseWriter, r *http.Request) {
	//this is not a valid json so that the user's code is broken instead of failing silently

	// Now, it's used to check alive.
	w.WriteHeader(200)
	fmt.Fprintln(w, "Sorry, this feature has not been implemented yet")
}
func InsertHandler(w http.ResponseWriter, r * http.Request) {
	if r.ParseForm() != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, "fail to parse URL")
		return
	}
	key_arr,key_ok := r.Form["key"]
	value_arr,value_ok := r.Form["value"]
	var key string
	var value string
	if !key_ok || len(key_arr) != 1 {
		key_ok = false
	}else{
		key = key_arr[0]
	}
	if !value_ok || len(value_arr) != 1 {
		value_ok = false
	}else{
		value = value_arr[0]
	}
	if !key_ok  || !value_ok {
		w.WriteHeader(400)
		if !key_ok {
			fmt.Fprintln(w, "key not present")
		}
		if !value_ok {
			fmt.Fprintln(w, "value not present")
		}
		return
	}
	success := datamanager.Insert(key,value)
	response,_ := json.Marshal(map[string]string{
		"success":strconv.FormatBool(success),
	})
	fmt.Fprintln(w, string(response))
}

func UpdateHandler(w http.ResponseWriter, r * http.Request) {
	if r.ParseForm() != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, "fail to parse URL")
		return
	}
	key_arr,key_ok := r.Form["key"]
	value_arr,value_ok := r.Form["value"]
	var key string
	var value string
	if !key_ok || len(key_arr) != 1 {
		key_ok = false
	}else{
		key = key_arr[0]
	}
	if !value_ok || len(value_arr) != 1 {
		value_ok = false
	}else{
		value = value_arr[0]
	}
	if !key_ok  || !value_ok {
		w.WriteHeader(400)
		if !key_ok {
			fmt.Fprintln(w, "key not present")
		}
		if !value_ok {
			fmt.Fprintln(w, "value not present")
		}
		return
	}
	success := datamanager.Update(key,value)
	response,_ := json.Marshal(map[string]string{
		"success":strconv.FormatBool(success),
	})
	fmt.Fprintln(w, string(response))
}

func GetHandler(w http.ResponseWriter, r * http.Request) {
	if r.ParseForm() != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, "fail to parse URL")
		return
	}
	key_arr,key_ok := r.Form["key"]
	var key string
	if !key_ok || len(key_arr) != 1 {
		key_ok = false
	}else{
		key = key_arr[0]
	}
	if !key_ok {
		w.WriteHeader(400)
		if !key_ok {
			fmt.Fprintln(w, "key not present")
		}
		return
	}
	value,success := datamanager.Get(key)
	response,_ := json.Marshal(map[string]string{
		"success":strconv.FormatBool(success),
		"value":value,
	})
	fmt.Fprintln(w, string(response))
}

func DeleteHandler(w http.ResponseWriter, r * http.Request) {
	if r.ParseForm() != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, "fail to parse URL")
		return
	}
	key_arr,key_ok := r.Form["key"]
	var key string
	if !key_ok || len(key_arr) != 1 {
		key_ok = false
	}else{
		key = key_arr[0]
	}
	if !key_ok {
		w.WriteHeader(400)
		if !key_ok {
			fmt.Fprintln(w, "key not present")
		}
		return
	}
	value,success := datamanager.Delete(key)
	response,_ := json.Marshal(map[string]string{
		"success":strconv.FormatBool(success),
		"value":value,
	})
	fmt.Fprintln(w, string(response))
}

func CountKeyHandler(w http.ResponseWriter, r * http.Request) {
	nkeys := datamanager.CountKey()
	response,_ := json.Marshal(map[string]string{
		"result":strconv.Itoa(nkeys),
	})
	fmt.Fprintln(w, string(response))
}

func DumpMapHandler(w http.ResponseWriter, r * http.Request) {
	response := datamanager.DumpArray()
	fmt.Fprintln(w, response)
}

func DumpHandler(w http.ResponseWriter, r * http.Request) {
	response := datamanager.DumpArray()
	fmt.Fprintln(w, response)
}

var shutdownchan = make(chan int)

func ShutdownHandler(w http.ResponseWriter, r * http.Request) {
	w.WriteHeader(200)
	shutdownchan <- 0
}

func main() {
	runtime.GOMAXPROCS(2)

	configstr, err:=ioutil.ReadFile("conf/settings.conf")
	if err != nil{
		fmt.Println("cannot find config file")
		panic(err)
	}
	var conf map[string]interface{}
	json.Unmarshal([]byte(configstr), &conf)

	if len(os.Args) != 2 {
		fmt.Println("Usage: main nodeid (from 0)")
		return
	}

	me, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Usage: main nodeid (from 0)")
		return
	}

	datamanager = NewDataManager(me)
	name := fmt.Sprintf("n%02d", me + 1)
	httpport := conf["port"].(string)
	ip := strings.Split(conf[name].(string),":")[0]
	address := ip + ":" + httpport

	nservers := len(conf) - 1

	var kvh []string = make([]string, nservers)
	for i := 0; i < nservers; i++ {
		kvh[i] = conf[fmt.Sprintf("n%02d",i+1)].(string)
	}

	//kvp = kvpaxos.StartServer(kvh, me)
	kvp = kvpaxos.StartServerUseTCP(kvh, me, true)

	s := &http.Server{
		Addr:	address,
		ReadTimeout:	10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	s.SetKeepAlivesEnabled(false)
	http.HandleFunc("/",NotImplementedHandler)
	http.HandleFunc("/kv/insert",InsertHandler)
	http.HandleFunc("/kv/delete",DeleteHandler)
	http.HandleFunc("/kv/get",GetHandler)
	http.HandleFunc("/kv/update",UpdateHandler)
	http.HandleFunc("/kvman/countkey",CountKeyHandler)
	http.HandleFunc("/kvman/dumpmap",DumpMapHandler)
	http.HandleFunc("/kvman/dump",DumpHandler)
	http.HandleFunc("/kvman/shutdown",ShutdownHandler)
	go func(){
		err := s.ListenAndServe()
		if err != nil {
			shutdownchan <- 0
		}
	}()
	<-shutdownchan
}
