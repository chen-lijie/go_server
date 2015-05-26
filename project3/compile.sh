#!/bin/sh
go build -o bin/serveprimary src/serveprimary.go
cp bin/serveprimary bin/servebackup
chmod +x bin/startserver
chmod +x bin/stopserver
chmod +x bin/test
