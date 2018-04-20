package main

import (
	"os"

	"github.com/inconshreveable/log15"
	"github.com/jessevdk/go-flags"
)

const (
	borgesName        string = "borges"
	borgesDescription string = "Fetches, organizes and stores repositories."
)

var (
	version string
	build   string
	log     log15.Logger
)

var parser = flags.NewParser(nil, flags.Default)

func init() {
	log = log15.New("module", borgesName)
	parser.LongDescription = borgesDescription
}

func main() {
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
