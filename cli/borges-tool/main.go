package main

import (
	"gopkg.in/src-d/go-cli.v0"
)

const (
	name        string = "borges-tool"
	description string = "Utilities to deal with borges pipelines"
)

var (
	version string
	build   string
)

var app = cli.New(name, version, build, description)

func main() {
	app.RunMain()
}
