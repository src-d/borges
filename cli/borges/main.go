package main

import (
	"os"

	"github.com/inconshreveable/log15"
	"github.com/jessevdk/go-flags"
)

const (
	name string = "borges"
	desc string = "Fetches, organizes and stores repositories."
)

var (
	version string
	build   string
	logger  log15.Logger
)

func init() {
	logger = log15.New("module", name)
}

func main() {
	parser := flags.NewParser(nil, flags.Default)
	parser.LongDescription = desc

	if _, err := parser.AddCommand(versionCmdName, versionCmdShortDesc,
		versionCmdLongDesc, &versionCmd{}); err != nil {
		panic(err)
	}

	if _, err := parser.AddCommand(consumerCmdName, consumerCmdShortDesc,
		consumerCmdLongDesc, &consumerCmd{}); err != nil {
		panic(err)
	}

	if _, err := parser.AddCommand(producerCmdName, producerCmdShortDesc,
		producerCmdLongDesc, &producerCmd{}); err != nil {
		panic(err)
	}

	if _, err := parser.Parse(); err != nil {
		if err, ok := err.(*flags.Error); ok {
			if err.Type == flags.ErrHelp {
				os.Exit(0)
			}

			parser.WriteHelp(os.Stdout)
		}

		os.Exit(1)
	}
}
