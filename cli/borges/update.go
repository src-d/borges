package main

import "gopkg.in/src-d/go-log.v1"

const (
	updateCmdName      = "update"
	updateCmdShortName = "update repositories processed previously"
	updateCmdLongDesc  = "This producer reads the database looking for jobs that require an update and queues new jobs for them."
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
