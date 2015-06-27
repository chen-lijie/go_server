#!/bin/sh
export GOPATH=`pwd`
go build -o bin/main src/main.go
chmod +x bin/startserver
chmod +x bin/stopserver
chmod +x bin/test
