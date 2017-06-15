package main

import (
	"github.com/src-d/borges"

	rcore "srcd.works/core-retrieval.v0"
	"srcd.works/core.v0"
	"srcd.works/framework.v0/queue"
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
	b, err := queue.NewBroker(c.Broker)
	if err != nil {
		return err
	}

	defer b.Close()
	q, err := b.Queue(c.Queue)
	if err != nil {
		return err
	}

	wp := borges.NewArchiverWorkerPool(
		core.ModelRepositoryStore(),
		rcore.RootedTransactioner(),
		core.TemporaryFilesystem(),
		c.startNotifier, c.stopNotifier, c.warnNotifier)
	wp.SetWorkerCount(c.WorkersCount)

	ac := borges.NewConsumer(q, wp)
	ac.Notifiers.QueueError = c.queueErrorNotifier
	ac.Start()

	return nil
}

func (c *consumerCmd) startNotifier(ctx *borges.WorkerContext, j *borges.Job) {
	log.Debug("job started", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID)
}

func (c *consumerCmd) stopNotifier(ctx *borges.WorkerContext, j *borges.Job, err error) {
	if err != nil {
		log.Error("job errored", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID, "error", err)
	} else {
		log.Info("job done", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID)
	}
}

func (c *consumerCmd) warnNotifier(ctx *borges.WorkerContext, j *borges.Job, err error) {
	log.Warn("job warning", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID, "error", err)
}

func (c *consumerCmd) queueErrorNotifier(err error) {
	log.Error("queue error", "error", err)
}
