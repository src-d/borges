package main

import (
	"time"

	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/core-retrieval.v0"
	log "gopkg.in/src-d/go-log.v0"
)

const (
	consumerCmdName      = "consumer"
	consumerCmdShortDesc = "consume jobs from a queue and process them"
	consumerCmdLongDesc  = ""
)

var consumerCommand = &consumerCmd{command: newCommand(
	consumerCmdName,
	consumerCmdShortDesc,
	consumerCmdLongDesc,
)}

type consumerCmd struct {
	command
	WorkersCount int    `long:"workers" default:"8" description:"number of workers"`
	Timeout      string `long:"timeout" default:"10h" description:"deadline to process a job"`
}

func (c *consumerCmd) Execute(args []string) error {
	c.init()

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

	l, err := loggerFactory.New()
	if err != nil {
		return err
	}

	l = l.New(log.Fields{"command": consumerCmdName})
	wp := borges.NewArchiverWorkerPool(
		l,
		storage.FromDatabase(core.Database()),
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

func init() {
	_, err := parser.AddCommand(
		consumerCommand.Name(),
		consumerCommand.ShortDescription(),
		consumerCommand.LongDescription(),
		consumerCommand)

	if err != nil {
		panic(err)
	}
}
