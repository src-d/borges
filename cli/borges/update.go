package main

import log "gopkg.in/src-d/go-log.v0"

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
	l, err := loggerFactory.New()
	if err != nil {
		return err
	}

	l = l.New(log.Fields{"command": updateCmdName})
	l.Warningf("Update command is not implemented yet")
	return nil
}
