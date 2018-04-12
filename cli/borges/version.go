package main

import "fmt"

const (
	versionCmdName      = "version"
	versionCmdShortDesc = "print version"
	versionCmdLongDesc  = versionCmdShortDesc
)

var versionCommand = &versionCmd{simpleCommand: newSimpleCommand(
	versionCmdName,
	versionCmdShortDesc,
	versionCmdLongDesc,
)}

type versionCmd struct {
	simpleCommand
}

func (c *versionCmd) Execute(args []string) error {
	println("VERSION")
	fmt.Printf("%s - %s (build %s)\n", borgesName, version, build)
	return nil
}

func init() {
	_, err := parser.AddCommand(
		versionCommand.name,
		versionCommand.shortDescription,
		versionCommand.longDescription,
		versionCommand)

	if err != nil {
		panic(err)
	}
}
