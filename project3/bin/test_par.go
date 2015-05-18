package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"os/exec"
	"net/url"
	"strings"
	"net/http"
)

var server_url string

func system(s string) {
	cmd := exec.Command(s);
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	fmt.Printf("in all caps: %q\n", out.String())
}

func add(key,value string) ([]byte){
	res, err := http.PostForm(server_url + "/kv/insert",
	url.Values{"key": {key}, "value": {value}})
	if err != nil {
		panic(err)
	}
	robots, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		panic(err)
	}
	return robots
}

func get(key string) ([]byte) {
	res, err := http.PostForm(server_url + "/kv/get?",
	url.Values{"key": {key} })
	if err != nil {
		panic(err)
	}
	robots, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		panic(err)
	}
	return robots
}

var failed bool

func addAndGet(key,value string,ch chan int){
	add(key,value)
	result := get(key)
	var f interface{}
	err := json.Unmarshal(result, &f)
	if err != nil{
		panic(err)
	}
	m := f.(map[string]interface{})
	ok := m["success"].(string)
	if ok != "true" {
		failed = true
	}
	ch<-1
}

func main() {
	configstr,err:=ioutil.ReadFile("../conf/settings.conf")
	if err != nil{
		fmt.Println("cannot find config file")
		panic(err)
	}
	dec := json.NewDecoder(strings.NewReader(string(configstr)))

	type servercfg struct{
		Primary, Backup, Port string
	}
	var config servercfg;
	err = dec.Decode(&config)
	if err != nil {
		fmt.Println("failed to parse config file")
		panic(err)
	}
	server_url = "http://" + config.Primary + ":" + config.Port

	//system("pwd")
	//system("../bin/stopserver -p")
	//system("../bin/stopserver -b")
	//system("../bin/startserver -b")
	//system("../bin/startserver -p")

	fmt.Println(server_url)
	ch := make(chan int)

	for i:=0;i<10000;i++ {
		go addAndGet(string(i),string(i),ch)
	}

	for i:=0;i<10000;i++ {
		<-ch
	}

	if failed {
		fmt.Println("failed!")
	}
}