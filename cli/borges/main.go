package main

import (
	"fmt"
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
	log     log15.Logger
)

type cmd struct {
	Queue    string `long:"queue" default:"borges" description:"queue name"`
	LogLevel string `short:"" long:"loglevel" description:"max log level enabled" default:"info"`
	LogFile  string `short:"" long:"logfile" description:"path to file where logs will be stored" default:""`
}

func (c *cmd) ChangeLogLevel() {
	lvl, err := log15.LvlFromString(c.LogLevel)
	if err != nil {
		panic(fmt.Sprintf("unknown level name %q", c.LogLevel))
	}

	handlers := []log15.Handler{log15.CallerFileHandler(log15.StdoutHandler)}
	if c.LogFile != "" {
		handlers = append(handlers,
			log15.CallerFileHandler(log15.Must.FileHandler(c.LogFile, log15.LogfmtFormat())))
	}
	log15.Root().SetHandler(log15.LvlFilterHandler(lvl, log15.MultiHandler(handlers...)))
}

func init() {
	log = log15.New("module", name)
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
