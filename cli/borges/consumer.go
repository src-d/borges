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
	"gopkg.in/src-d/go-log.v0"
	"gopkg.in/src-d/go-queue.v1"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
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

	b, err := queue.NewBroker(c.Broker)
	if err != nil {
		return err
	}

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
			log.Infof("signal received, stopping...")
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
