package main

import (
	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"
)

const (
	mentionsCmdName      = "mentions"
	mentionsCmdShortDesc = "produce jobs from mentions"
	mentionsCmdLongDesc  = "This producer reads from a queue with repository mentions. Mentions can be generated with the rovers project. For each one of them, it generates a job and queues it."
)

var mentionsCommand = &mentionsCmd{producerSubcmd: newProducerSubcmd(
	mentionsCmdName,
	mentionsCmdShortDesc,
	mentionsCmdLongDesc,
)}

// mentionsCommand is a producer subcommand.
type mentionsCmd struct {
	producerSubcmd

	QueueMentions   string `long:"queue-mentions" env:"BORGES_QUEUE_MENTIONS" default:"rovers" description:"queue name used to obtain mentions if the source type is 'mentions'"`
	RepublishBuried bool   `long:"republish-buried" env:"BORGES_REPUBLISH_BURIED" description:"republishes again all buried jobs before starting to listen for mentions, used with --source=mentions"`
}

func (c *mentionsCmd) Execute(args []string) error {
	if err := c.producerSubcmd.init(); err != nil {
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
