package borges

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/src-d/borges/storage"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/test"
	"gopkg.in/src-d/framework.v0/queue"
	"gopkg.in/src-d/go-kallax.v1"
)

type ExecutorSuite struct {
	test.Suite
	p     *Executor
	store storage.RepositoryStore
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

	log := logrus.NewEntry(logrus.StandardLogger())

	wp := NewWorkerPool(log, func(log *logrus.Entry, j *Job) error {
		jobs = append(jobs, j)
		return nil
	})
	wp.SetWorkerCount(1)

	e := NewExecutor(log, q, wp, s.store, NewLineJobIter(r, s.store))

	return jobs, e.Execute()
}

func TestExecutor(t *testing.T) {
	suite.Run(t, new(ExecutorSuite))
}
