package main

import (
	"srcd.works/borges"

	"srcd.works/framework/queue"
)

const (
	producerCmdName      = "producer"
	producerCmdShortDesc = "create new jobs and put them into the queue"
	producerCmdLongDesc  = ""
)

type producerCmd struct {
	BeanstalkURL  string `long:"beanstalk" default:"127.0.0.1:11300" description:"beanstalk url server"`
	BeanstalkTube string `long:"tube" default:"borges" description:"beanstalk tube name"`

	p *borges.Producer
}

func (c *producerCmd) Execute(args []string) error {
	b, err := queue.NewBeanstalkBroker(c.BeanstalkURL)
	if err != nil {
		return err
	}

	defer b.Close()
	q, err := b.Queue(c.BeanstalkTube)
	if err != nil {
		return err
	}
	c.p = borges.NewProducer(q)
	c.p.Notifiers.Done = c.notifier
	c.p.Start()
	return nil
}

func (c *producerCmd) notifier(j *borges.Job, err error) {
	if err != nil {
		logger.Error("job queue error", "RepositoryID", j.RepositoryID, "error", err)
	} else {
		logger.Info("job queued", "RepositoryID", j.RepositoryID, "error", err)
	}
}
