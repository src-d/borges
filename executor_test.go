package borges

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/src-d/borges/storage"

	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/go-kallax.v1"
	"gopkg.in/src-d/go-log.v1"
	"gopkg.in/src-d/go-queue.v1/memory"
)

type ExecutorSuite struct {
	test.Suite
	p     *Executor
	store RepositoryStore
}

func (s *ExecutorSuite) SetupTest() {
	s.Setup()
	s.store = storage.Local()
}

func (s *ExecutorSuite) TearDownTest() {
	s.TearDown()
}

func (s *ExecutorSuite) TestExecute() {
	jobs, err := s.runExecutor(
		"git://foo.bar/baz",
		"https://foo.bar",
	)
	require := s.Require()
	require.NoError(err)

	require.Len(jobs, 2)
	s.assertRepo("git://foo.bar/baz", jobs[0])
	s.assertRepo("https://foo.bar", jobs[1])
}

func (s *ExecutorSuite) assertRepo(endpoint string, job *Job) {
	require := s.Require()
	repos, err := s.store.GetByEndpoints(endpoint)
	require.NoError(err)
	require.Len(repos, 1)
	require.Equal(kallax.ULID(job.RepositoryID), repos[0].ID)
}

func (s *ExecutorSuite) runExecutor(repos ...string) ([]*Job, error) {
	require := s.Require()
	q, err := memory.New().Queue(kallax.NewULID().String())
	require.NoError(err)

	r := ioutil.NopCloser(strings.NewReader(strings.Join(repos, "\n")))

	var jobs []*Job

	wp := NewWorkerPool(func(ctx context.Context, logger log.Logger, j *Job) error {
		jobs = append(jobs, j)
		return nil
	})
	wp.SetWorkerCount(1)

	e := NewExecutor(q, wp, s.store, NewLineJobIter(r, s.store))

	return jobs, e.Execute()
}

func TestExecutor(t *testing.T) {
	suite.Run(t, new(ExecutorSuite))
}
