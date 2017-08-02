package main

import (
	"github.com/src-d/borges"

	"gopkg.in/src-d/core-retrieval.v0"
)

const (
	consumerCmdName      = "consumer"
	consumerCmdShortDesc = "consume jobs from a queue a process them."
	consumerCmdLongDesc  = ""
)

type consumerCmd struct {
	cmd
	WorkersCount int `long:"workers" default:"8" description:"number of workers"`
}

func (c *consumerCmd) Execute(args []string) error {
	c.ChangeLogLevel()
	c.startProfilingHTTPServerMaybe(c.ProfilerPort)

	b := core.Broker()
	defer b.Close()
	q, err := b.Queue(c.Queue)
	if err != nil {
		return err
	}

	wp := borges.NewArchiverWorkerPool(
		log,
		core.ModelRepositoryStore(),
		core.RootedTransactioner(),
		borges.NewTemporaryCloner(core.TemporaryFilesystem()),
		core.Locking(),
	)
	wp.SetWorkerCount(c.WorkersCount)

	ac := borges.NewConsumer(q, wp)
	ac.Start()

	return nil
}
