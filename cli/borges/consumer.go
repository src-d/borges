package main

import (
	"srcd.works/borges"

	"srcd.works/framework.v0/queue"
)

const (
	consumerCmdName      = "consumer"
	consumerCmdShortDesc = "consume jobs from a queue a process them."
	consumerCmdLongDesc  = ""
)

type consumerCmd struct {
	BeanstalkURL  string `long:"beanstalk" default:"127.0.0.1:11300" description:"beanstalk url server"`
	BeanstalkTube string `long:"tube" default:"borges" description:"beanstalk tube name"`
	WorkersCount  int    `long:"workers" default:"8" description:"number of workers"`
}

func (c *consumerCmd) Execute(args []string) error {
	b, err := queue.NewBeanstalkBroker(c.BeanstalkURL)
	if err != nil {
		return err
	}

	defer b.Close()
	q, err := b.Queue(c.BeanstalkTube)
	if err != nil {
		return err
	}

	wp := borges.NewArchiverWorkerPool(c.startNotifier, c.stopNotifier, c.warnNotifier)
	wp.SetWorkerCount(c.WorkersCount)

	ac := borges.NewConsumer(q, wp)
	ac.Notifiers.QueueError = c.queueErrorNotifier
	ac.Start()

	return nil
}

func (c *consumerCmd) startNotifier(ctx *borges.WorkerContext, j *borges.Job) {
	logger.Debug("job started", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID)
}

func (c *consumerCmd) stopNotifier(ctx *borges.WorkerContext, j *borges.Job, err error) {
	if err != nil {
		logger.Error("job errored", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID, "error", err)
	} else {
		logger.Info("job done", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID)
	}
}

func (c *consumerCmd) warnNotifier(ctx *borges.WorkerContext, j *borges.Job, err error) {
	logger.Warn("job warning", "WorkerID", ctx.ID, "RepositoryID", j.RepositoryID, "error", err)
}

func (c *consumerCmd) queueErrorNotifier(err error) {
	logger.Error("queue error", "error", err)
}
