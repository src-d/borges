package main

import (
	"os"

	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"

	cli "gopkg.in/src-d/go-cli.v0"
)

func init() {
	producerCommandAdder.AddCommand(&fileCmd{}, setPrioritySettings)
}

type fileCmd struct {
	cli.Command `name:"file" short-description:"produce jobs from file" long-description:"This producer reads from a file one repository URL per line, generates a job and queues it."`
	producerOpts

	PositionalArgs struct {
		File string `positional-arg-name:"path" description:"file with repositories to pack, one per line"`
	} `positional-args:"true" required:"1"`
}

func (c *fileCmd) Execute(args []string) error {
	if err := c.producerOpts.init(); err != nil {
		return err
	}
	defer c.broker.Close()

	return c.generateJobs(c.jobIter)
}

func (c *fileCmd) jobIter() (borges.JobIter, error) {
	db, err := c.OpenDatabase()
	if err != nil {
		return nil, err
	}

	storer := storage.FromDatabase(db)
	f, err := os.Open(c.PositionalArgs.File)
	if err != nil {
		return nil, err
	}
	return borges.NewLineJobIter(f, storer), nil
}
