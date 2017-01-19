package borges

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"srcd.works/framework/queue"
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
	c.SetWorkerCount(1)
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
	c.do = func(*WorkerContext, *Job) error {
		processed++
		if processed > 1 {
			assert.Fail("too many jobs processed")
		}

		c.Stop()
		return nil
	}

	done := make(chan struct{}, 1)
	relevantQueueErrorCount := 0
	c.Notifiers.QueueError = func(err error) {
		fmt.Println("processed", processed, "errCount", relevantQueueErrorCount)
		if processed != 1 {
			return
		}

		done <- struct{}{}
		assert.True(ErrAlreadyStopped.Is(err))

	}

	for i := 0; i < 2; i++ {
		job := queue.NewJob()
		assert.NoError(job.Encode(&Job{RepositoryID: uint64(i)}))
		assert.NoError(s.queue.Publish(job))
	}

	c.SetWorkerCount(1)
	go c.Start()

	assert.NoError(timeoutChan(done, time.Second*2))
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
