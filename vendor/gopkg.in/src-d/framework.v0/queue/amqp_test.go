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
