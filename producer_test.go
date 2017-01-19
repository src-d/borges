package borges

import (
	"testing"
	"time"

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

func (s *ProducerSuite) TestProducer_StarStop() {
	assert := assert.New(s.T())
	p := s.newProducer()

	var doneCalled int
	p.Notifiers.Done = func(j *Job, err error) {
		doneCalled++
		assert.NoError(err)
	}

	go p.Start()

	time.Sleep(time.Millisecond * 1000)
	assert.True(p.IsRunning())

	iter, err := s.queue.Consume()
	j, err := iter.Next()
	assert.NoError(err)
	assert.NotNil(j)

	p.Stop()
	assert.False(p.IsRunning())
	assert.True(doneCalled > 1)
}
