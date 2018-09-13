package main

import (
	"gopkg.in/src-d/go-cli.v0"
)

const (
	borgesName        string = "borges"
	borgesDescription string = "Fetches, organizes and stores repositories."
)

var (
	version string
	build   string
)

var app = cli.New(borgesName, version, build, borgesDescription)

func main() {
	app.RunMain()
}
