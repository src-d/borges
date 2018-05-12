package main

import (
	"time"

	"github.com/src-d/borges"

	"gopkg.in/src-d/go-log.v0"
	"gopkg.in/src-d/go-queue.v1"
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

	log.With(log.Fields{"time": c.Time}).Infof("starting republishing jobs...")

	log.Debugf("republish task triggered ")
	if err := c.queue.RepublishBuried(republishCondition); err != nil {
		log.Errorf(err, "error republishing buried jobs")
	}

	if lapse != 0 {
		c.runPeriodically(lapse)
	}

	log.Infof("stopping republishing jobs")
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

func (c *republishCmd) runPeriodically(lapse time.Duration) {
	ticker := time.Tick(lapse)
	for range ticker {
		log.Debugf("republish task triggered ")
		if err := c.queue.RepublishBuried(republishCondition); err != nil {
			log.Errorf(err, "error republishing buried jobs")
		}
	}
}
