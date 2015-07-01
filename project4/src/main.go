package main

import (
	"fmt"
	"net/http"
	"time"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"os"
	"strings"
	crand "crypto/rand"
	mrand "math/rand"
	"math/big"
)
import "kvpaxos"
import "runtime"

type DataManager struct{
}

var kvp *kvpaxos.KVPaxos

func NewDataManager(me int) *DataManager{
	var m *DataManager = new(DataManager)
	return m
}

func (m * DataManager) Insert(key string,value string, session_id string) bool {
	return kvp.Insert(key, value, session_id)
}

func (m * DataManager) Delete(key string, session_id string) (string, bool) {
	v, present := kvp.Delete(key, session_id)
	if !present {
		return v,false
	}
	return v,true
}

func (m * DataManager) Get(key string, session_id string) (string, bool) {
	v, present := kvp.Get(key, session_id)
	if !present {
		return v,false
	}
	return v,true
}

func (m * DataManager) Update(key string, value string, session_id string) bool {
	present := kvp.Update(key, value, session_id)
	if !present {
		return false
	}
	return true
}

func (m * DataManager) CountKey(session_id string) int {
	v := kvp.Count(session_id)
	return v
}

func (m * DataManager) DumpArray(session_id string) string {
	s := kvp.Dump(session_id)
	return s
}

func getSessionId(r * http.Request) string {
	id_arr,id_ok := r.Form["session"]
	if id_ok && len(id_arr)==1 {
		return id_arr[0]
	}else{
		randid := mrand.Int63()
		return strconv.FormatInt(randid,10)
	}
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
	success := datamanager.Insert(key,value,getSessionId(r))
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
	success := datamanager.Update(key,value,getSessionId(r))
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
	value,success := datamanager.Get(key,getSessionId(r))
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
	value,success := datamanager.Delete(key,getSessionId(r))
	response,_ := json.Marshal(map[string]string{
		"success":strconv.FormatBool(success),
		"value":value,
	})
	fmt.Fprintln(w, string(response))
}

func CountKeyHandler(w http.ResponseWriter, r * http.Request) {
	nkeys := datamanager.CountKey(getSessionId(r))
	response,_ := json.Marshal(map[string]string{
		"result":strconv.Itoa(nkeys),
	})
	fmt.Fprintln(w, string(response))
}

func DumpMapHandler(w http.ResponseWriter, r * http.Request) {
	response := datamanager.DumpArray(getSessionId(r))
	fmt.Fprintln(w, response)
}

func DumpHandler(w http.ResponseWriter, r * http.Request) {
	response := datamanager.DumpArray(getSessionId(r))
	fmt.Fprintln(w, response)
}

var shutdownchan = make(chan int)

func ShutdownHandler(w http.ResponseWriter, r * http.Request) {
	w.WriteHeader(200)
	shutdownchan <- 0
}

func main() {
	bigseed,_ := crand.Int(crand.Reader, big.NewInt(9223372036854775807))
	mrand.Seed(bigseed.Int64())
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
