package main

import (
	"os"

	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"
)

const (
	fileCmdName      = "file"
	fileCmdShortDesc = "produce jobs from file"
	fileCmdLongDesc  = "This producer reads from a file one repository URL per line, generates a job and queues it."
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
	File string `positional-arg-name:"path" description:"file with repositories to pack, one per line"`
}

func (c *fileCmd) Execute(args []string) error {
	if err := c.producerSubcmd.init(); err != nil {
		return err
	}
	defer c.broker.Close()

	return c.generateJobs(c.jobIter)
}

func (c *fileCmd) jobIter() (borges.JobIter, error) {
	db, err := c.openDatabase()
	if err != nil {
		return nil, err
	}

	storer := storage.FromDatabase(db)
	f, err := os.Open(c.File)
	if err != nil {
		return nil, err
	}
	return borges.NewLineJobIter(f, storer), nil
}
