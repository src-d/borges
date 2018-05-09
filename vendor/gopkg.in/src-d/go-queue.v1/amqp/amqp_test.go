package queue

import (
	"fmt"
	"testing"
	"time"

	"gopkg.in/src-d/go-queue.v1"
	"gopkg.in/src-d/go-queue.v1/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestAMQPSuite(t *testing.T) {
	suite.Run(t, new(AMQPSuite))
}

type AMQPSuite struct {
	test.QueueSuite
}

const testAMQPURI = "amqp://127.0.0.1:5672"

func (s *AMQPSuite) SetupSuite() {
	s.BrokerURI = testAMQPURI
}

func TestDefaultConfig(t *testing.T) {
	assert.Equal(t, DefaultConfiguration.BuriedExchangeSuffix, ".buriedExchange")
}

func TestNewAMQPBroker_bad_url(t *testing.T) {
	assert := assert.New(t)

	b, err := New("badurl")
	assert.Error(err)
	assert.Nil(b)
}

func sendJobs(assert *assert.Assertions, n int, p queue.Priority, q queue.Queue) {
	for i := 0; i < n; i++ {
		j, err := queue.NewJob()
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

	broker, err := New(testAMQPURI)
	assert.NoError(err)
	if !assert.NotNil(broker) {
		return
	}

	name := test.NewName()
	q, err := broker.Queue(name)
	assert.NoError(err)
	assert.NotNil(q)

	// Send 50 low priority jobs
	sendJobs(assert, 50, queue.PriorityLow, q)

	// Send 50 high priority jobs
	sendJobs(assert, 50, queue.PriorityUrgent, q)

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
	assert.Equal(uint(queue.PriorityUrgent)*50, sumFirst)
	assert.Equal(uint(queue.PriorityLow)*50, sumLast)
}

func TestAMQPHeaders(t *testing.T) {
	broker, err := queue.NewBroker(testAMQPURI)
	require.NoError(t, err)
	defer func() { require.NoError(t, broker.Close()) }()

	q, err := broker.Queue(test.NewName())
	require.NoError(t, err)

	tests := []struct {
		name      string
		retries   int32
		errorType string
	}{
		{
			name: fmt.Sprintf("with %s and %s headers",
				DefaultConfiguration.RetriesHeader, DefaultConfiguration.ErrorHeader),
			retries:   int32(10),
			errorType: "error-test",
		},
		{
			name:      fmt.Sprintf("with %s header", DefaultConfiguration.RetriesHeader),
			retries:   int32(10),
			errorType: "",
		},
		{
			name:      fmt.Sprintf("with %s headers", DefaultConfiguration.ErrorHeader),
			retries:   int32(0),
			errorType: "error-test",
		},
		{
			name:      "with no headers",
			retries:   int32(0),
			errorType: "",
		},
	}

	for i, test := range tests {
		job, err := queue.NewJob()
		require.NoError(t, err)

		job.Retries = test.retries
		job.ErrorType = test.errorType

		require.NoError(t, job.Encode(i))
		require.NoError(t, q.Publish(job))
	}

	jobIter, err := q.Consume(len(tests))
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, err := jobIter.Next()
			require.NoError(t, err)
			require.NotNil(t, job)

			require.Equal(t, test.retries, job.Retries)
			require.Equal(t, test.errorType, job.ErrorType)
		})
	}
}

func TestAMQPRepublishBuried(t *testing.T) {
	broker, err := queue.NewBroker(testAMQPURI)
	require.NoError(t, err)
	defer func() { require.NoError(t, broker.Close()) }()

	queueName := test.NewName()
	q, err := broker.Queue(queueName)
	require.NoError(t, err)

	amqpQueue, ok := q.(*Queue)
	require.True(t, ok)

	buried := amqpQueue.buriedQueue

	tests := []struct {
		name    string
		payload string
	}{
		{name: "message 1", payload: "payload 1"},
		{name: "message 2", payload: "republish"},
		{name: "message 3", payload: "payload 3"},
		{name: "message 3", payload: "payload 4"},
	}

	for _, utest := range tests {
		job, err := queue.NewJob()
		require.NoError(t, err)

		job.Raw = []byte(utest.payload)

		err = buried.Publish(job)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
	}

	var condition queue.RepublishConditionFunc = func(j *queue.Job) bool {
		return string(j.Raw) == "republish"
	}

	err = q.RepublishBuried(condition)
	require.NoError(t, err)

	jobIter, err := q.Consume(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, jobIter.Close()) }()

	job, err := jobIter.Next()
	require.NoError(t, err)
	require.Equal(t, string(job.Raw), "republish")
}
