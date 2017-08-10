package main

import (
	"time"

	"github.com/src-d/borges"

	"gopkg.in/src-d/core-retrieval.v0"
)

const (
	consumerCmdName      = "consumer"
	consumerCmdShortDesc = "consume jobs from a queue and process them"
	consumerCmdLongDesc  = ""
)

type consumerCmd struct {
	cmd
	WorkersCount int    `long:"workers" default:"8" description:"number of workers"`
	Timeout      string `long:"timeout" default:"10h" description:"deadline to process a job"`
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

	timeout, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return err
	}

	wp := borges.NewArchiverWorkerPool(
		log,
		core.ModelRepositoryStore(),
		core.RootedTransactioner(),
		borges.NewTemporaryCloner(core.TemporaryFilesystem()),
		core.Locking(),
		timeout,
	)
	wp.SetWorkerCount(c.WorkersCount)

	ac := borges.NewConsumer(q, wp)
	ac.Start()

	return nil
}
