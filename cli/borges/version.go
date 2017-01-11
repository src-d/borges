package main

import "fmt"

const (
	versionCmdName      = "version"
	versionCmdShortDesc = "print version"
	versionCmdLongDesc  = versionCmdShortDesc
)

type versionCmd struct{}

func (c *versionCmd) Execute(args []string) error {
	fmt.Printf("%s - %s (build %s)\n", name, version, build)
	return nil
}
