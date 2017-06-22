package borges

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	rmodel "srcd.works/core-retrieval.v0/model"
	"srcd.works/core.v0"
	"srcd.works/framework.v0/queue"
)

const testEndpoint = "https://some.endpoint.com"

func TestProducerSuite(t *testing.T) {
	suite.Run(t, new(ProducerSuite))
}

type ProducerSuite struct {
	BaseQueueSuite
	mentionsQueue queue.Queue
}

func (s *ProducerSuite) SetupSuite() {
	s.BaseQueueSuite.SetupSuite()

	assert := require.New(s.T())
	q, err := s.broker.Queue(fmt.Sprintf("mentions_test_%d", time.Now().UnixNano()))
	assert.NoError(err)

	s.mentionsQueue = q
}

func (s *ProducerSuite) newProducer() *Producer {
	DropTables("repository")
	DropIndexes("idx_endpoints")
	CreateRepositoryTable()
	storer := core.ModelRepositoryStore()

	return NewProducer(NewMentionJobIter(s.mentionsQueue, storer), s.queue)
}

func (s *ProducerSuite) newJob() *queue.Job {
	j := queue.NewJob()
	m := &rmodel.Mention{
		VCS:      rmodel.GIT,
		Provider: "TEST_PROVIDER",
		Endpoint: testEndpoint,
	}
	err := j.Encode(m)
	s.Assert().NoError(err)

	return j
}

func (s *ProducerSuite) TestStartStop() {
	assert := require.New(s.T())
	p := s.newProducer()

	err := s.mentionsQueue.Publish(s.newJob())
	assert.NoError(err)

	var doneCalled int
	p.Notifiers.Done = func(j *Job, err error) {
		doneCalled++
		assert.NoError(err)
	}

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	assert.True(p.IsRunning())

	awnd := 1
	iter, err := s.queue.Consume(awnd)
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	p.Stop()
	assert.False(p.IsRunning())
	assert.True(doneCalled == 1)
}

func (s *ProducerSuite) TestStartStop_TwoEqualsJobs() {
	assert := require.New(s.T())
	p := s.newProducer()

	err := s.mentionsQueue.Publish(s.newJob())
	assert.NoError(err)

	err = s.mentionsQueue.Publish(s.newJob())
	assert.NoError(err)

	var doneCalled int
	p.Notifiers.Done = func(j *Job, err error) {
		doneCalled++
		assert.NoError(err)
	}

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	assert.True(p.IsRunning())
	awnd := 1
	iter, err := s.queue.Consume(awnd)
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	var jobOne Job
	assert.NoError(j.Decode(&jobOne))

	iter, err = s.queue.Consume(awnd)
	assert.NoError(err)
	j, err = iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	var jobTwo Job
	assert.NoError(j.Decode(&jobOne))

	p.Stop()
	assert.False(p.IsRunning())
	assert.True(doneCalled == 2)

	assert.Equal(jobOne.RepositoryID, jobTwo.RepositoryID)
}

func (s *ProducerSuite) TestStartStop_ErrorNotifier() {
	assert := require.New(s.T())
	p := NewProducer(&DummyJobIter{}, s.queue)

	var errorCalled int
	p.Notifiers.QueueError = func(err error) {
		errorCalled++
		assert.Error(err)
	}

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	p.Stop()
	assert.False(p.IsRunning())
	assert.True(errorCalled == 1)
}

func (s *ProducerSuite) TestStartStop_ErrorNoNotifier() {
	assert := require.New(s.T())
	p := NewProducer(&DummyJobIter{}, s.queue)

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	p.Stop()
	assert.False(p.IsRunning())
}

func (s *ProducerSuite) TestStartStop_noNotifier() {
	assert := require.New(s.T())
	p := s.newProducer()

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	assert.True(p.IsRunning())

	awnd := 1
	iter, err := s.queue.Consume(awnd)
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	p.Stop()
	assert.False(p.IsRunning())
}

type DummyJobIter struct{}

func (j DummyJobIter) Close() error        { return errors.New("SOME CLOSE ERROR") }
func (j DummyJobIter) Next() (*Job, error) { return &Job{RepositoryID: uuid.Nil}, nil }
