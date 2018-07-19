package main

import (
	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/go-cli.v0"
)

func init() {
	producerCommandAdder.AddCommand(&mentionsCmd{}, setPrioritySettings)
}

// mentionsCommand is a producer subcommand.
type mentionsCmd struct {
	cli.Command `name:"mentions" short-description:"produce jobs from mentions" long-description:"This producer reads from a queue with repository mentions. Mentions can be generated with the rovers project. For each one of them, it generates a job and queues it."`
	producerOpts

	QueueMentions   string `long:"queue-mentions" env:"BORGES_QUEUE_MENTIONS" default:"rovers" description:"queue name used to obtain mentions if the source type is 'mentions'"`
	RepublishBuried bool   `long:"republish-buried" env:"BORGES_REPUBLISH_BURIED" description:"republishes again all buried jobs before starting to listen for mentions, used with --source=mentions"`
}

func (c *mentionsCmd) Execute(args []string) error {
	if err := c.producerOpts.init(); err != nil {
		return err
	}
	defer c.broker.Close()

	return c.generateJobs(c.jobIter)
}

func (c *mentionsCmd) jobIter() (borges.JobIter, error) {
	storer := storage.FromDatabase(c.database)
	q, err := c.broker.Queue(c.QueueMentions)
	if err != nil {
		return nil, err
	}

	if c.RepublishBuried {
		if err := q.RepublishBuried(); err != nil {
			return nil, err
		}
	}
	return borges.NewMentionJobIter(q, storer), nil
}
