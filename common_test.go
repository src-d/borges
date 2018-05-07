package borges

import (
	"fmt"
	"testing"
	"time"

	"github.com/src-d/borges/storage"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/framework.v0/queue"
	"gopkg.in/src-d/go-kallax.v1"
)

const (
	testBrokerURI   = "amqp://localhost:5672"
	testQueuePrefix = "borges_test_queue"
)

func TestCommon(t *testing.T) {
	suite.Run(t, new(RepositoryIDSuite))
}

type BaseQueueSuite struct {
	test.Suite
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
	s.Suite.Setup()
}

func (s *BaseQueueSuite) TearDownTest() {
	s.NoError(s.broker.Close())
	s.Suite.TearDown()
}

func (s *BaseQueueSuite) connectQueue() {
	var err error
	s.broker, err = queue.NewBroker(testBrokerURI)
	s.NoError(err)
	s.queue, err = s.broker.Queue(s.queueName)
	s.NoError(err)
}

type RepositoryIDSuite struct {
	test.Suite

	storer  RepositoryStore
	isTrue  bool
	isFalse bool
}

func (s *RepositoryIDSuite) TestRepositoryIDSameUrls() {
	id1, err := RepositoryID([]string{"a", "b"}, nil, s.storer)
	s.NoError(err)

	id2, err := RepositoryID([]string{"a", "b"}, nil, s.storer)
	s.NoError(err)

	s.Equal(id1, id2)

}

func (s *RepositoryIDSuite) TestRepositoryIDOtherUrls() {
	id1, err := RepositoryID([]string{"a", "c"}, nil, s.storer)
	s.NoError(err)

	id2, err := RepositoryID([]string{"a", "b"}, nil, s.storer)
	s.NoError(err)

	s.Equal(id1, id2)
	a := s.getRepository(id1).Endpoints
	s.Contains(a, "a")
	s.Contains(a, "b")
	s.Contains(a, "c")

	s.Equal(3, len(a))
}

func (s *RepositoryIDSuite) TestRepositoryIDMoreUrlsSecondStep() {
	id1, err := RepositoryID([]string{"a"}, nil, s.storer)
	s.NoError(err)

	id2, err := RepositoryID([]string{"a", "b", "c"}, nil, s.storer)
	s.NoError(err)

	s.Equal(id1, id2)
	a := s.getRepository(id1).Endpoints
	s.Contains(a, "a")
	s.Contains(a, "b")
	s.Contains(a, "c")

	s.Equal(3, len(a))
}

func (s *RepositoryIDSuite) TestRepositoryIDNotEqualID() {
	id1, err := RepositoryID([]string{"a"}, nil, s.storer)
	s.NoError(err)

	id2, err := RepositoryID([]string{"b", "c"}, nil, s.storer)
	s.NoError(err)

	s.NotEqual(id1, id2)
	a := s.getRepository(id1).Endpoints
	s.Contains(a, "a")
	s.Equal(1, len(a))

	b := s.getRepository(id2).Endpoints
	s.Contains(b, "b")
	s.Contains(b, "c")
	s.Equal(2, len(b))
}

func (s *RepositoryIDSuite) getRepository(id uuid.UUID) *model.Repository {
	repo, err := s.storer.Get(kallax.ULID(id))
	s.NoError(err)
	return repo
}

func (s *RepositoryIDSuite) SetupTest() {
	s.Setup()

	s.storer = storage.FromDatabase(s.DB)

	s.isTrue = true
	s.isFalse = false
}

func (s *RepositoryIDSuite) TearDownTest() {
	s.TearDown()
}
