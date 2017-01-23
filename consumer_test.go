package borges

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"srcd.works/framework.v0/queue"
)

func TestConsumerSuite(t *testing.T) {
	suite.Run(t, new(ConsumerSuite))
}

type ConsumerSuite struct {
	BaseQueueSuite
}

func (s *ConsumerSuite) newConsumer() *Consumer {
	wp := NewWorkerPool(func(*WorkerContext, *Job) error { return nil })
	return NewConsumer(s.queue, wp)
}

func (s *ConsumerSuite) TestConsumer_StartStop_EmptyQueue() {
	assert := assert.New(s.T())
	c := s.newConsumer()
	c.WorkerPool.SetWorkerCount(1)
	go c.Start()

	time.Sleep(time.Millisecond * 100)
	assert.True(c.IsRunning())
	c.Stop()
	assert.False(c.IsRunning())
}

func (s *ConsumerSuite) TestConsumer_StartStop() {
	assert := assert.New(s.T())
	c := s.newConsumer()

	processed := 0
	done := make(chan struct{}, 1)
	c.WorkerPool.do = func(*WorkerContext, *Job) error {
		processed++
		if processed > 1 {
			assert.Fail("too many jobs processed")
			done <- struct{}{}
		}

		done <- struct{}{}
		return nil
	}

	c.Notifiers.QueueError = func(err error) {
		assert.Fail("no error expected:", err.Error())
	}

	for i := 0; i < 1; i++ {
		job := queue.NewJob()
		assert.NoError(job.Encode(&Job{RepositoryID: uint64(i)}))
		assert.NoError(s.queue.Publish(job))
	}

	c.WorkerPool.SetWorkerCount(1)
	go c.Start()

	assert.NoError(timeoutChan(done, time.Second*10))
	c.Stop()
	assert.False(c.IsRunning())
	assert.Equal(1, processed)
}

func timeoutChan(done chan struct{}, d time.Duration) error {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	select {
	case <-done:
		return nil
	case <-ticker.C:
		return fmt.Errorf("timeout")
	}
}
