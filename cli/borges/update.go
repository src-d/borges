package main

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
	log.Warn("Update command is not implemented yet")
	return nil
}
