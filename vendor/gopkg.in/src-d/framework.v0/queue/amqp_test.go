package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestAMQPSuite(t *testing.T) {
	suite.Run(t, new(AMQPSuite))
}

type AMQPSuite struct {
	QueueSuite
}

func (s *AMQPSuite) SetupSuite() {
	s.BrokerURI = testAMQPURI
}

func TestNewAMQPBroker_bad_url(t *testing.T) {
	assert := assert.New(t)

	b, err := NewAMQPBroker("badurl")
	assert.Error(err)
	assert.Nil(b)
}

func sendJobs(assert *assert.Assertions, n int, p Priority, q Queue) {
	for i := 0; i < n; i++ {
		j, err := NewJob()
		assert.NoError(err)
		j.SetPriority(p)
		err = j.Encode(i)
		assert.NoError(err)
		err = q.Publish(j)
		assert.NoError(err)
	}
}

func TestAMQPPriorities(t *testing.T) {
	assert := assert.New(t)

	broker, err := NewAMQPBroker(testAMQPURI)
	assert.NoError(err)
	assert.NotNil(broker)

	name := newName()
	q, err := broker.Queue(name)
	assert.NoError(err)
	assert.NotNil(q)

	// Send 50 low priority jobs
	sendJobs(assert, 50, PriorityLow, q)

	// Send 50 high priority jobs
	sendJobs(assert, 50, PriorityUrgent, q)

	// Receive and collect priorities
	iter, err := q.Consume(1)
	assert.NoError(err)
	assert.NotNil(iter)

	sumFirst := uint(0)
	sumLast := uint(0)

	for i := 0; i < 100; i++ {
		j, err := iter.Next()
		assert.NoError(err)
		assert.NoError(j.Ack())

		if i < 50 {
			sumFirst += uint(j.Priority)
		} else {
			sumLast += uint(j.Priority)
		}
	}

	assert.True(sumFirst > sumLast)
	assert.Equal(uint(PriorityUrgent)*50, sumFirst)
	assert.Equal(uint(PriorityLow)*50, sumLast)
}
