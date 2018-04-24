package main

import (
	"time"

	"github.com/src-d/borges"
	"gopkg.in/src-d/framework.v0/queue"
	log "gopkg.in/src-d/go-log.v0"
)

const (
	republishCmdName      = "republish"
	republishCmdShortDesc = "requeue jobs from buried queues"
	republishCmdLongDesc  = ""
)

// republishCommand is a producer subcommand.
var republishCommand = &republishCmd{producerSubcmd: newProducerSubcmd(
	republishCmdName,
	republishCmdShortDesc,
	republishCmdLongDesc,
)}

type republishCmd struct {
	producerSubcmd

	Time string `long:"time" short:"t" default:"0" description:"elapsed time between republish triggers"`
}

func (c *republishCmd) Execute(args []string) error {
	lapse, err := time.ParseDuration(c.Time)
	if err != nil {
		return err
	}

	if err := c.producerSubcmd.init(); err != nil {
		return err
	}
	defer c.broker.Close()

	l, err := loggerFactory.New()
	if err != nil {
		return err
	}

	l = l.New(log.Fields{"command": republishCmdName})

	l.New(log.Fields{"time": c.Time}).Infof("starting republishing jobs...")

	l.Debugf("republish task triggered ")
	if err := c.queue.RepublishBuried(republishCondition); err != nil {
		l.Error(err, "error republishing buried jobs")
	}

	if lapse != 0 {
		c.runPeriodically(l, lapse)
	}

	l.Infof("stopping republishing jobs")
	return nil
}

func republishCondition(job *queue.Job) bool {
	// Althoug the job has the temporary error tag, it must be checked
	// that the retries is equals to zero. The reason for this is that
	// a job can panic during a retry process, so it can be tagged as
	// temporary error and a number of retries greater than zero reveals
	// that fact.
	return job.ErrorType == borges.TemporaryError && job.Retries == 0
}

func (c *republishCmd) runPeriodically(l log.Logger, lapse time.Duration) {
	ticker := time.Tick(lapse)
	for range ticker {
		l.Debugf("republish task triggered ")
		if err := c.queue.RepublishBuried(republishCondition); err != nil {
			l.Error(err, "error republishing buried jobs")
		}
	}
}
