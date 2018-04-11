package borges

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/framework.v0/queue"
)

func TestConsumerSuite(t *testing.T) {
	suite.Run(t, new(ConsumerSuite))
}

type ConsumerSuite struct {
	BaseQueueSuite
}

func (s *ConsumerSuite) newConsumer() *Consumer {
	wp := NewWorkerPool(log15.New(), func(log15.Logger, *Job) error { return nil })
	return NewConsumer(s.queue, wp)
}

func (s *ConsumerSuite) TestConsumer_StartStop_FailedJob() {
	require := require.New(s.T())
	expectedError := errors.New("SOME ERROR")

	c := s.newConsumer()

	id, err := uuid.NewV4()
	require.NoError(err)

	var processedId uuid.UUID

	processed := 0
	done := make(chan struct{}, 1)
	c.WorkerPool.do = func(log log15.Logger, j *Job) error {
		defer func() { done <- struct{}{} }()
		processed++
		if processed == 2 {
			processedId = j.RepositoryID
			return nil
		}

		return expectedError
	}

	for i := 0; i < 1; i++ {
		job, err := queue.NewJob()
		require.NoError(err)
		require.NoError(job.Encode(&Job{RepositoryID: id}))
		require.NoError(s.queue.Publish(job))
	}

	c.WorkerPool.SetWorkerCount(1)
	go c.Start()

	require.NoError(timeoutChan(done, time.Second*10))
	require.Equal(1, processed)

	require.Error(timeoutChan(done, time.Second*5))
	require.Equal(1, processed)

	testCondition := func(*queue.Job) bool { return true }
	err = s.queue.RepublishBuried(testCondition)
	require.NoError(err)

	require.NoError(timeoutChan(done, time.Second*10))
	require.Equal(2, processed)
	require.Equal(id, processedId)

	c.Stop()
}

func (s *ConsumerSuite) TestConsumer_StartStop_EmptyQueue() {
	c := s.newConsumer()
	c.WorkerPool.SetWorkerCount(1)
	go c.Start()

	time.Sleep(time.Millisecond * 100)
	c.Stop()
}

func (s *ConsumerSuite) TestConsumer_StartStop() {
	assert := assert.New(s.T())
	c := s.newConsumer()

	processed := 0
	done := make(chan struct{}, 1)
	c.WorkerPool.do = func(log15.Logger, *Job) error {
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
		job, err := queue.NewJob()
		assert.NoError(err)

		id, err := uuid.NewV4()
		assert.NoError(err)

		assert.NoError(job.Encode(&Job{RepositoryID: id}))
		assert.NoError(s.queue.Publish(job))
	}

	c.WorkerPool.SetWorkerCount(1)
	go c.Start()

	assert.NoError(timeoutChan(done, time.Second*10))
	c.Stop()
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
