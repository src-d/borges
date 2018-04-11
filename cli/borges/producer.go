package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	flags "github.com/jessevdk/go-flags"
	"github.com/src-d/borges"
	"github.com/src-d/borges/storage"

	"gopkg.in/src-d/core-retrieval.v0"
	"gopkg.in/src-d/framework.v0/queue"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
)

const (
	producerCmdName      = "producer"
	producerCmdShortDesc = "create new jobs and put them into the queue"
	producerCmdLongDesc  = ""
)

type producerCmd struct {
	cmd
	Source            string `long:"source" default:"mentions" description:"source to produce jobs from (mentions, file)"`
	MentionsQueue     string `long:"mentionsqueue" default:"rovers" description:"queue name used to obtain mentions if the source type is 'mentions'"`
	File              string `long:"file" description:"path to a file to read URLs from, used with --source=file"`
	RepublishMentions bool   `long:"republish-mentions" description:"republishes again all buried mentions before starting to listen for new mentions, used with --source=mentions"`
	RepublishJobs     bool   `long:"republish-jobs" description:"republish failed jobs on the main queue"`
	Priority          uint8  `long:"priority" default:"4" description:"priority used to enqueue jobs, goes from 0 (lowest) to :MAX: (highest)"`
	JobsRetries       int    `long:"job-retries" default:"5" description:"number of times a falied job should be processed again before reject it"`
}

// Changes the priority description and default on runtime as it is not
// possible to create a dynamic tag
func setPrioritySettings(c *flags.Command) {
	options := c.Options()

	for _, o := range options {
		if o.LongName == "priority" {
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

func (c *producerCmd) Execute(args []string) error {
	c.init()

	err := checkPriority(c.Priority)
	if err != nil {
		return err
	}

	b := core.Broker()
	defer b.Close()
	q, err := b.Queue(c.Queue)
	if err != nil {
		return err
	}

	if c.RepublishJobs {
		q.RepublishBuried(jobCondition)
	}

	ji, err := c.jobIter(b)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(ji, &err)

	p := borges.NewProducer(log, ji, q,
		queue.Priority(c.Priority), c.JobsRetries)

	p.Start()

	return err
}

func jobCondition(job *queue.Job) bool {
	// Althoug the job has the temporary error tag, it must be checked
	// that the retries is equals to zero. The reason for this is that
	// a job can panic during a retry process, so it can be tagged as
	// temporary error and a number of retries greater than zero reveals
	// that fact.
	return job.ErrorType == borges.TemporaryError && job.Retries == 0
}

func (c *producerCmd) jobIter(b queue.Broker) (borges.JobIter, error) {
	storer := storage.FromDatabase(core.Database())

	switch c.Source {
	case "mentions":
		q, err := b.Queue(c.MentionsQueue)
		if err != nil {
			return nil, err
		}

		if c.RepublishMentions {
			if err := q.RepublishBuried(mentionCondition); err != nil {
				return nil, err
			}
		}
		return borges.NewMentionJobIter(q, storer), nil
	case "file":
		f, err := os.Open(c.File)
		if err != nil {
			return nil, err
		}
		return borges.NewLineJobIter(f, storer), nil
	default:
		return nil, fmt.Errorf("invalid source: %s", c.Source)
	}
}

func mentionCondition(*queue.Job) bool { return true }
