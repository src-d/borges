package borges

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"srcd.works/framework.v0/queue"
)

const (
	testBrokerURI   = "amqp://localhost:5672"
	testQueuePrefix = "borges_test_queue"
)

type BaseQueueSuite struct {
	suite.Suite
	broker    queue.Broker
	queue     queue.Queue
	queueName string
}

func (s *BaseQueueSuite) SetupSuite() {
	s.queueName = fmt.Sprintf("%s_%d", testQueuePrefix, time.Now().UnixNano())
	s.connectQueue()
}

func (s *BaseQueueSuite) SetupTest() {
	s.connectQueue()
}

func (s *BaseQueueSuite) TearDownTest() {
	assert := assert.New(s.T())
	assert.NoError(s.broker.Close())
}

func (s *BaseQueueSuite) connectQueue() {
	assert := assert.New(s.T())
	var err error
	s.broker, err = queue.NewBroker(testBrokerURI)
	assert.NoError(err)
	s.queue, err = s.broker.Queue(s.queueName)
	assert.NoError(err)
}
