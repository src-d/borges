package borges

import (
	"sync"
	"time"

	"srcd.works/framework/queue"
)

// Consumer consumes jobs from a queue and uses multiple workers to process
// them.
type Consumer struct {
	Notifiers struct {
		QueueError func(error)
	}
	*WorkerPool

	queue     queue.Queue
	running   bool
	startOnce *sync.Once
	stopOnce  *sync.Once
}

// NewConsumer creates a new consumer.
func NewConsumer(queue queue.Queue, pool *WorkerPool) *Consumer {
	return &Consumer{
		WorkerPool: pool,
		queue:      queue,
		startOnce:  &sync.Once{},
		stopOnce:   &sync.Once{},
	}
}

// IsRunning returns true if the consumer is running.
func (c *Consumer) IsRunning() bool {
	return c.running
}

// Start initializes the consumer and starts it.
func (c *Consumer) Start() {
	c.startOnce.Do(c.start)
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(c.stop)
}

func (c *Consumer) backoff() {
	time.Sleep(time.Second * 5)
}

func (c *Consumer) reject(j *queue.Job, origErr error) {
	c.notifyQueueError(origErr)
	if err := j.Reject(true); err != nil {
		c.notifyQueueError(err)
	}
}

func (c *Consumer) start() {
	c.running = true
	defer func() { c.running = false }()
	for {
		iter, err := c.queue.Consume()
		if err != nil {
			c.notifyQueueError(err)
			c.backoff()
			continue
		}

		j, err := iter.Next()
		if err != nil {
			c.notifyQueueError(err)
			c.backoff()
			if err := iter.Close(); err != nil {
				c.notifyQueueError(err)
			}

			continue
		}

		if !c.running {
			err = ErrAlreadyStopped.New("cannot deliver job")
			c.notifyQueueError(err)
			break
		}

		job := &Job{}
		if err := j.Decode(job); err != nil {
			c.reject(j, err)
			continue
		}

		c.Do(&WorkerJob{job, j})
	}

	return
}

func (c *Consumer) stop() {
	c.running = false
	c.WorkerPool.Close()
}

func (c *Consumer) notifyQueueError(err error) {
	if c.Notifiers.QueueError == nil {
		return
	}

	c.Notifiers.QueueError(err)
}
