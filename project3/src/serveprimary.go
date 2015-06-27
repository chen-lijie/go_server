package main

import (
	"fmt"
	"net/http"
	"time"
	"io/ioutil"
	"strings"
	"encoding/json"
	"sync"
	"strconv"
	"os"
	"net/url"
	"net"
)

var is_primary bool
var partneraddress string

type DataManager struct{
	userdata map[string]string
	lock sync.RWMutex
}

func NewDataManager() *DataManager{
	var m *DataManager = new(DataManager)
	m.userdata = make(map[string]string)
	return m
}

func (m * DataManager) Insert(key string,value string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	_,present := m.userdata[key]
	if present {
		return false
	}
	if is_primary {
		response,err := http.Get("http://" + partneraddress + "/kv/insert?key=" + url.QueryEscape(key) + "&value=" + url.QueryEscape(value))
		if err != nil {
			return false
		} else {
			ioutil.ReadAll(response.Body)
		}
		response.Body.Close()
	}
	m.userdata[key]=value
	return true
}

func (m * DataManager) Delete(key string) (string,bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	v,present := m.userdata[key]
	if !present {
		return v,false
	}
	if is_primary {
		response,err := http.Get("http://" + partneraddress + "/kv/delete?key=" + url.QueryEscape(key))
		if err != nil {
			return "",false
		} else {
			ioutil.ReadAll(response.Body)
		}
		response.Body.Close()
	}
	delete(m.userdata,key)
	return v,true
}

func (m * DataManager) Get(key string) (string,bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	v,present := m.userdata[key]
	if !present {
		return v,false
	}
	return v,true
}

func (m * DataManager) Update(key string,value string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	_,present := m.userdata[key]
	if !present {
		return false
	}
	if is_primary {
		response,err := http.Get("http://" + partneraddress + "/kv/update?key=" + url.QueryEscape(key) + "&value=" + url.QueryEscape(value))
		if err != nil {
			return false
		} else {
			ioutil.ReadAll(response.Body)
		}
		response.Body.Close()
	}
	m.userdata[key]=value
	return true
}

func (m * DataManager) CountKey() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.userdata)
}

func (m * DataManager) DumpMap() string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	response,_ := json.Marshal(m.userdata)
	return string(response)
}

func (m * DataManager) DumpArray() string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	items := make([][]string, len(m.userdata))
	i := 0
	for k, v := range m.userdata {
		items[i] = make([]string,2)
		items[i][0]=k
		items[i][1]=v
		i++
	}
	response,_ := json.Marshal(items)
	return string(response)
}

func (m * DataManager) Load(dump map[string]string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.userdata = dump
}


var datamanager = NewDataManager()

func NotImplementedHandler(w http.ResponseWriter, r *http.Request) {
	//this is not a valid json so that the user's code is broken instead of failing silently
	w.WriteHeader(501)
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
	response := datamanager.DumpMap()
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
	if len(os.Args)>=2 && os.Args[1]=="backup" {
		is_primary = false
	} else {
		is_primary = true
	}
	configstr,err:=ioutil.ReadFile("conf/settings.conf")
	if err != nil{
		fmt.Println("cannot find config file")
		panic(err)
	}
	type servercfg struct{
		Primary, Backup, Port string
	}
	dec := json.NewDecoder(strings.NewReader(string(configstr)))
	var config servercfg;
	err = dec.Decode(&config)
	if err != nil {
		fmt.Println("failed to parse config file")
		panic(err)
	}
	var listenaddress string
	var listenip string
	if is_primary {
		listenip = config.Primary
		partneraddress = config.Backup + ":" + config.Port
	} else {
		listenip = config.Backup
		partneraddress = config.Primary + ":" + config.Port
	}
	listenaddress = listenip + ":" + config.Port

	//make sure only one instance of the server is running
	listener, err := net.Listen("tcp", listenip+":9931")
	if err != nil {
		return
	} else {
		defer listener.Close()
	}

	backupdump,err := http.Get("http://" + partneraddress + "/kvman/dumpmap")
	if err == nil {
		content, _ := ioutil.ReadAll(backupdump.Body)
		dumpdata := map[string]string{}
		json.Unmarshal(content,&dumpdata)
		datamanager.Load(dumpdata)
		backupdump.Body.Close()
	}
	s := &http.Server{
		Addr:	listenaddress,
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
