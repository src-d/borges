package main

import (
	"os"

	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/core-retrieval.v0"
)

const (
	fileCmdName      = "file"
	fileCmdShortDesc = "produce jobs from file"
	fileCmdLongDesc  = ""
)

// fileCommand is a producer subcommand.
var fileCommand = &fileCmd{producerSubcmd: newProducerSubcmd(
	fileCmdName,
	fileCmdShortDesc,
	fileCmdLongDesc,
)}

type fileCmd struct {
	producerSubcmd

	filePositionalArgs `positional-args:"true" required:"1"`
}

type filePositionalArgs struct {
	File string `positional-arg-name:"path"`
}

func (c *fileCmd) Execute(args []string) error {
	if err := c.producerSubcmd.init(); err != nil {
		return err
	}
	defer c.broker.Close()

	return c.generateJobs(c.jobIter)
}

func (c *fileCmd) jobIter() (borges.JobIter, error) {
	storer := storage.FromDatabase(core.Database())
	f, err := os.Open(c.File)
	if err != nil {
		return nil, err
	}
	return borges.NewLineJobIter(f, storer), nil
}
