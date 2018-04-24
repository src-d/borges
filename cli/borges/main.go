package main

import (
	"os"

	"github.com/jessevdk/go-flags"
	log "gopkg.in/src-d/go-log.v0"
)

const (
	borgesName        string = "borges"
	borgesDescription string = "Fetches, organizes and stores repositories."
)

var (
	version       string
	build         string
	loggerFactory *log.LoggerFactory
)

var parser = flags.NewParser(nil, flags.Default)

func init() {
	loggerFactory = &log.LoggerFactory{Fields: `{"module":"` + borgesName + `"}`}
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
