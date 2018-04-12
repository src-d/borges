package main

const (
	republishCmdName      = "republish"
	republishCmdShortName = "requeue jobs from buried queues"
	republishCmdLongDesc  = ""
)

// republishCommand is a producer subcommand.
var republishCommand = &republishCmd{producerSubcmd: newProducerSubcmd(
	republishCmdName,
	republishCmdShortName,
	republishCmdLongDesc,
)}

type republishCmd struct {
	producerSubcmd
}

func (c *republishCmd) Execute(args []string) error {
	log.Warn("Republish command is not implemented yet")
	return nil
}
