package borges

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"srcd.works/framework.v0/queue"
)

const (
	testBeanstalkAddress = "localhost:11300"
	testQueue            = "borges_test_tube"
)

type BaseQueueSuite struct {
	suite.Suite
	broker queue.Broker
	queue  queue.Queue
}

func (s *BaseQueueSuite) SetupSuite() {
	assert := assert.New(s.T())
	s.connectQueue()
	assert.NoError(s.broker.Close())
}

func (s *BaseQueueSuite) SetupTest() {
	assert := require.New(s.T())
	s.connectQueue()
	assert.NoError(drainQueue(s.queue))
}

func (s *BaseQueueSuite) TearDownTest() {
	assert := assert.New(s.T())
	assert.NoError(s.broker.Close())
}

func (s *BaseQueueSuite) connectQueue() {
	assert := assert.New(s.T())
	var err error
	s.broker, err = queue.NewBeanstalkBroker(testBeanstalkAddress)
	assert.NoError(err)
	s.queue, err = s.broker.Queue(testQueue)
	assert.NoError(err)
}

func drainQueue(q queue.Queue) error {
	iter, err := q.Consume()
	if err != nil {
		return err
	}

	for {
		j, err := iter.Next()
		if err != nil {
			break
		}

		if err := j.Ack(); err != nil {
			return err
		}
	}

	return nil
}
