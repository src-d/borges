package borges

import (
	"io/ioutil"
	"strings"
	"testing"

	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/framework.v0/queue"
	kallax "gopkg.in/src-d/go-kallax.v1"
	log "gopkg.in/src-d/go-log.v0"

	"github.com/src-d/borges/storage"
	"github.com/stretchr/testify/suite"
)

type ExecutorSuite struct {
	test.Suite
	p     *Executor
	store storage.RepoStore
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
	q, err := queue.NewMemoryBroker().Queue("jobs")
	require.NoError(err)

	r := ioutil.NopCloser(strings.NewReader(strings.Join(repos, "\n")))

	var jobs []*Job

	l, err := log.New()
	s.NoError(err)

	wp := NewWorkerPool(l, func(l log.Logger, j *Job) error {
		jobs = append(jobs, j)
		return nil
	})
	wp.SetWorkerCount(1)

	e := NewExecutor(l, q, wp, s.store, NewLineJobIter(r, s.store))

	return jobs, e.Execute()
}

func TestExecutor(t *testing.T) {
	suite.Run(t, new(ExecutorSuite))
}
