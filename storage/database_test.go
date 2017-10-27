package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	core "gopkg.in/src-d/core-retrieval.v0"
	"gopkg.in/src-d/core-retrieval.v0/model"
	kallax "gopkg.in/src-d/go-kallax.v1"
)

type DatabaseSuite struct {
	suite.Suite
	store    *dbRepoStore
	rawStore *model.RepositoryStore
}

func (s *DatabaseSuite) SetupTest() {
	db := core.Database()
	s.rawStore = model.NewRepositoryStore(db)
	s.store = FromDatabase(db).(*dbRepoStore)
}

func (s *DatabaseSuite) TearDownTest() {
	s.rawStore.RawExec("DELETE FROM repositories")
}

func (s *DatabaseSuite) TestGet() {
	require := s.Require()

	expected := s.createRepo(model.Pending, "foo")
	repo, err := s.store.Get(expected.ID)
	require.NoError(err)
	require.Equal(expected.ID, repo.ID)
	require.Equal(expected.Endpoints, repo.Endpoints)
	require.Equal(expected.Status, repo.Status)

	repo, err = s.store.Get(kallax.NewULID())
	require.Equal(kallax.ErrNotFound, err)
}

func (s *DatabaseSuite) TestGetByEndpoints() {
	require := s.Require()

	repos := []*model.Repository{
		s.createRepo(model.Pending, "foo"),
		s.createRepo(model.Pending, "bar"),
		s.createRepo(model.Pending, "baz", "bar"),
		s.createRepo(model.Pending, "baz"),
	}

	result, err := s.store.GetByEndpoints("bar", "baz")
	require.Len(result, 3)
	require.NoError(err)
	require.Equal(repos[1].ID, result[0].ID)
	require.Equal(repos[2].ID, result[1].ID)
	require.Equal(repos[3].ID, result[2].ID)

	result, err = s.store.GetByEndpoints("notfound")
	require.Len(result, 0)
	require.NoError(err)
}

func (s *DatabaseSuite) TestSetStatus() {
	require := s.Require()
	repo := s.createRepo(model.Pending, "foo")

	err := s.store.SetStatus(repo, model.Fetching)
	require.NoError(err)
	require.Equal(model.Fetching, repo.Status)

	repo, err = s.store.Get(repo.ID)
	require.NoError(err)
	require.Equal(model.Fetching, repo.Status)
}

func (s *DatabaseSuite) TestSetEndpoints() {
	require := s.Require()
	repo := s.createRepo(model.Pending, "foo")

	endpoints := []string{"bar", "baz"}
	err := s.store.SetEndpoints(repo, endpoints...)
	require.NoError(err)
	require.Len(repo.Endpoints, 2)
	require.Equal(endpoints, repo.Endpoints)

	repo, err = s.store.Get(repo.ID)
	require.NoError(err)
	require.Equal(endpoints, repo.Endpoints)
}

func (s *DatabaseSuite) TestUpdateFailed() {
	require := s.Require()
	repo := s.createRepo(model.Fetching, "foo")

	err := s.store.UpdateFailed(repo, model.Pending)
	require.NoError(err)
	require.Equal(model.Pending, repo.Status)

	repo, err = s.store.Get(repo.ID)
	require.NoError(err)
	require.Equal(model.Pending, repo.Status)
}

func (s *DatabaseSuite) TestUpdateFetched() {
	require := s.Require()
	repo := s.createRepo(model.Fetching, "foo")
	time := withoutNs(time.Now())

	err := s.store.UpdateFetched(repo, time)
	require.NoError(err)
	require.Len(repo.Endpoints, 1)
	require.Equal(&time, repo.FetchedAt)
	require.Equal(model.Fetched, repo.Status)

	repo, err = s.store.Get(repo.ID)
	require.NoError(err)
	require.Equal(model.Fetched, repo.Status)
}

func (s *DatabaseSuite) createRepo(status model.FetchStatus, remotes ...string) *model.Repository {
	repo := model.NewRepository()
	repo.Status = status
	repo.Endpoints = remotes
	s.Require().NoError(s.rawStore.Insert(repo))

	repo.CreatedAt = withoutNs(repo.CreatedAt)
	repo.UpdatedAt = withoutNs(repo.UpdatedAt)

	_, err := s.rawStore.Update(repo)
	s.Require().NoError(err)
	return repo
}

func withoutNs(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
}

func TestDatabase(t *testing.T) {
	suite.Run(t, new(DatabaseSuite))
}
