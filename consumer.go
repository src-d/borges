package borges

import (
	"time"

	"srcd.works/framework.v0/queue"
)

// Consumer consumes jobs from a queue and uses multiple workers to process
// them.
type Consumer struct {
	Notifiers struct {
		QueueError func(error)
	}
	WorkerPool *WorkerPool
	Queue      queue.Queue

	running bool
	close   bool
	quit    chan struct{}
	iter    queue.JobIter
}

// NewConsumer creates a new consumer.
func NewConsumer(queue queue.Queue, pool *WorkerPool) *Consumer {
	return &Consumer{
		WorkerPool: pool,
		Queue:      queue,
	}
}

// IsRunning returns true if the consumer is running.
func (c *Consumer) IsRunning() bool {
	return c.running
}

// Start initializes the consumer and starts it, blocking until it is stopped.
func (c *Consumer) Start() {
	c.close = false
	c.running = true
	defer func() { c.running = false }()
	c.quit = make(chan struct{})
	defer func() { close(c.quit) }()
	for {
		if err := c.consumeQueue(c.Queue); err != nil {
			c.notifyQueueError(err)
			c.closeIter()
		}

		if c.close {
			break
		}

		c.backoff()
	}

	return
}

// Stop stops the consumer. Note that it does not close the underlying queue
// and worker pool. It blocks until the consumer has actually stopped.
func (c *Consumer) Stop() {
	c.close = true
	c.closeIter()
	<-c.quit
}

func (c *Consumer) backoff() {
	time.Sleep(time.Second * 5)
}

func (c *Consumer) reject(j *queue.Job, origErr error) {
	c.notifyQueueError(origErr)
	if err := j.Reject(false); err != nil {
		c.notifyQueueError(err)
	}
}

func (c *Consumer) consumeQueue(q queue.Queue) error {
	var err error
	c.iter, err = c.Queue.Consume()
	if err != nil {
		return err
	}

	if err := c.consumeJobIter(c.iter); err != nil {
		if err == queue.ErrAlreadyClosed {
			c.iter = nil
			if c.close {
				return nil
			}

			return err
		}

		c.closeIter()
		return err
	}

	return nil
}

func (c *Consumer) consumeJobIter(iter queue.JobIter) error {
	for {
		j, err := iter.Next()
		if err == queue.ErrEmptyJob {
			c.notifyQueueError(err)
			continue
		}

		if err != nil {
			return err
		}

		if err := c.consumeJob(j); err != nil {
			c.notifyQueueError(err)
		}
	}
}

func (c *Consumer) consumeJob(j *queue.Job) error {
	job := &Job{}
	if err := j.Decode(job); err != nil {
		c.reject(j, err)
		return err
	}

	c.WorkerPool.Do(&WorkerJob{job, j})
	return nil
}

func (c *Consumer) closeIter() {
	if c.iter == nil {
		return
	}

	if err := c.iter.Close(); err != nil {
		c.notifyQueueError(err)
	}

	c.iter = nil
}

func (c *Consumer) notifyQueueError(err error) {
	if c.Notifiers.QueueError == nil {
		return
	}

	c.Notifiers.QueueError(err)
}
