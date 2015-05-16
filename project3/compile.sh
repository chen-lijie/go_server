#!/bin/sh
go build -o bin/serveprimary src/serveprimary.go
chmod +x bin/startserver
chmod +x bin/stopserver
