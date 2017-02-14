package borges

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestProducerSuite(t *testing.T) {
	suite.Run(t, new(ProducerSuite))
}

type ProducerSuite struct {
	BaseQueueSuite
}

func (s *ProducerSuite) newProducer() *Producer {
	return NewProducer(NewMentionJobIter(), s.queue)
}

func (s *ProducerSuite) TestStartStop() {
	assert := assert.New(s.T())
	p := s.newProducer()

	var doneCalled int
	p.Notifiers.Done = func(j *Job, err error) {
		doneCalled++
		assert.NoError(err)
	}

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	assert.True(p.IsRunning())

	iter, err := s.queue.Consume()
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	p.Stop()
	assert.False(p.IsRunning())
	assert.True(doneCalled > 1)
}

func (s *ProducerSuite) TestStartStop_ErrorNotifier() {
	assert := assert.New(s.T())
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
	assert := assert.New(s.T())
	p := NewProducer(&DummyJobIter{}, s.queue)

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	p.Stop()
	assert.False(p.IsRunning())
}

func (s *ProducerSuite) TestStartStop_noNotifier() {
	assert := assert.New(s.T())
	p := s.newProducer()

	go p.Start()

	time.Sleep(time.Millisecond * 100)
	assert.True(p.IsRunning())

	iter, err := s.queue.Consume()
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	p.Stop()
	assert.False(p.IsRunning())
}

type DummyJobIter struct{}

func (j DummyJobIter) Close() error        { return errors.New("SOME CLOSE ERROR") }
func (j DummyJobIter) Next() (*Job, error) { return &Job{RepositoryID: 0, URL: "URL"}, nil }
