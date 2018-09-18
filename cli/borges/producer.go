package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/src-d/borges"

	"github.com/jessevdk/go-flags"
	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
	"gopkg.in/src-d/go-queue.v1"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
)

var producerCommandAdder = app.AddCommand(&producerCmd{})

type producerCmd struct {
	cli.Command `name:"producer" short-description:"create new jobs and put them into the queue" long-description:""`
}

type producerOpts struct {
	queueOpts
	databaseOpts

	database *sql.DB
	broker   queue.Broker
	queue    queue.Queue

	QueuePriority uint8 `long:"queue-priority" env:"BORGES_QUEUE_PRIORITY" default:"4" description:"priority used to enqueue jobs, goes from 0 (lowest) to :MAX: (highest)"`
	JobsRetries   int   `long:"job-retries" env:"BORGES_JOB_RETRIES" default:"5" description:"number of times a falied job should be processed again before reject it"`
}

func (c *producerOpts) init() error {
	err := checkPriority(c.QueuePriority)
	if err != nil {
		return err
	}

	c.broker, err = queue.NewBroker(c.Broker)
	if err != nil {
		return err
	}

	c.queue, err = c.broker.Queue(c.Queue)
	if err != nil {
		return err
	}

	c.database, err = c.openDatabase()
	if err != nil {
		return err
	}

	return nil
}

type getIterFunc func() (borges.JobIter, error)

func (c *producerOpts) generateJobs(getIter getIterFunc) error {
	ji, err := getIter()
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(ji, &err)

	p := borges.NewProducer(ji, c.queue, queue.Priority(c.QueuePriority), c.JobsRetries)

	p.Start()

	return err
}

// Changes the priority description and default on runtime as it is not
// possible to create a dynamic tag
func setPrioritySettings(c *flags.Command) {
	options := c.Options()

	for _, o := range options {
		if o.LongName == "queue-priority" {
			o.Default[0] = strconv.Itoa((int(queue.PriorityNormal)))
			o.Description = strings.Replace(
				o.Description, ":MAX:", strconv.Itoa(int(queue.PriorityUrgent)), 1)
		}
	}
}

func checkPriority(prio uint8) error {
	if prio > uint8(queue.PriorityUrgent) {
		return fmt.Errorf("Priority must be between 0 and %d", queue.PriorityUrgent)
	}

	return nil
}
