package main

import (
	"bytes"
	"fmt"
	"os/exec"
)

func system(s string) {
	cmd := exec.Command(s);
	cmd.Run()
}

func main() {
	system()
}