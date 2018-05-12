package main

import "gopkg.in/src-d/go-log.v0"

const (
	updateCmdName      = "update"
	updateCmdShortName = "update repositories processed previously"
	updateCmdLongDesc  = ""
)

// updateCommand is a producer subcommand.
var updateCommand = &updateCmd{producerSubcmd: newProducerSubcmd(
	updateCmdName,
	updateCmdShortName,
	updateCmdLongDesc,
)}

type updateCmd struct {
	producerSubcmd
}

func (c *updateCmd) Execute(args []string) error {
	log.Warningf("update command is not implemented yet")
	return nil
}
