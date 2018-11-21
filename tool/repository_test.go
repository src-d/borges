package tool

import (
	"context"
	"testing"

	"github.com/src-d/borges"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-queue.v1"
	"gopkg.in/src-d/go-queue.v1/memory"
)

func TestRepository(t *testing.T) {
	suite.Run(t, &RepositorySuite{})
}

type RepositorySuite struct {
	ToolSuite
	database *Database
	queue    queue.Queue
}

func (s *RepositorySuite) SetupTest() {
	s.ToolSuite.SetupTest()
	s.database = NewDatabase(s.DB)

	broker := memory.New()
	var err error
	s.queue, err = broker.Queue("test")
	s.NoError(err)
}

func (s *RepositorySuite) TearDownTest() {
	s.ToolSuite.TearDownTest()
}

func (s *RepositorySuite) TestQueueAll() {
	rep := NewRepository(s.database, s.queue)
	err := rep.Queue(context.TODO(), ulid)
	s.NoError(err)

	iter, err := s.queue.Consume(0)
	s.NoError(err)

	var ids []string
	for {
		j, err := iter.Next()
		if j == nil {
			break
		}
		s.NoError(err)

		job := borges.Job{}
		err = j.Decode(&job)
		s.NoError(err)

		ids = append(ids, job.RepositoryID.String())
	}

	queueable := []string{
		ulid[1],
		ulid[2],
		ulid[6],
	}

	s.ElementsMatch(ids, queueable)

	for _, r := range testRepos {
		repo, err := s.database.Repository(r.uuid)
		s.NoError(err)

		if r.status == model.Fetched || r.status == model.Fetching {
			s.Equal(model.Pending, repo.Status)
		} else {
			s.Equal(r.status, repo.Status)
		}
	}
}
