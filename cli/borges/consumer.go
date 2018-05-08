package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/src-d/borges"
	"github.com/src-d/borges/lock"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/core-retrieval.v0"
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
	Locking string `long:"locking" env:"CONFIG_LOCKING" default:"local:" description:"locking service configuration"`
	Workers int    `long:"workers" default:"8" description:"number of workers"`
	Timeout string `long:"timeout" default:"10h" description:"deadline to process a job"`
}

func (c *consumerCmd) Execute(args []string) error {
	c.init()

	locking, err := lock.New(c.Locking)
	if err != nil {
		return err
	}

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
		log.WithField("command", consumerCmdName),
		storage.FromDatabase(core.Database()),
		core.RootedTransactioner(),
		borges.NewTemporaryCloner(core.TemporaryFilesystem()),
		locking,
		timeout,
	)
	wp.SetWorkerCount(c.Workers)

	ac := borges.NewConsumer(q, wp)

	var term = make(chan os.Signal)
	var done = make(chan struct{})
	go func() {
		select {
		case <-term:
			log.Info("signal received, stopping...")
			ac.Stop()
		case <-done:
		}
	}()
	signal.Notify(term, syscall.SIGTERM, os.Interrupt)

	err = ac.Start()
	close(done)
	ac.Stop()

	return err
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
