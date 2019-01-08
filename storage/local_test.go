package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/core-retrieval.v0/model"
	"gopkg.in/src-d/go-kallax.v1"
)

type LocalSuite struct {
	suite.Suite
	store *LocalStore
}

func (s *LocalSuite) SetupTest() {
	s.store = Local()
}

func (s *LocalSuite) TestGet() {
	require := s.Require()

	id := kallax.NewULID()
	expected := &localRepository{
		ID:       id,
		Endpoint: "foo",
		Status:   model.Pending,
	}
	s.store.repos[id] = expected
	repo, err := s.store.Get(id)
	require.NoError(err)
	require.Equal(expected.toRepo(), repo)

	_, err = s.store.Get(kallax.NewULID())
	require.Equal(kallax.ErrNotFound, err)
}

func (s *LocalSuite) TestGetByEndpoints() {
	require := s.Require()

	var ids []kallax.ULID
	for i := 0; i < 3; i++ {
		ids = append(ids, kallax.NewULID())
	}
	repos := []*localRepository{
		{ids[0], "foo", model.Pending, nil},
		{ids[1], "bar", model.Pending, nil},
		{ids[2], "baz", model.Pending, nil},
	}

	for i, id := range ids {
		s.store.repos[id] = repos[i]
	}

	result, err := s.store.GetByEndpoints("foo", "baz")
	require.Len(result, 2)
	require.NoError(err)
	var endpoints []string
	for _, repo := range result {
		endpoints = append(endpoints, repo.Endpoints...)
	}
	sort.Strings(endpoints)
	require.Equal([]string{"baz", "foo"}, endpoints)

	result, err = s.store.GetByEndpoints("notfound")
	require.Len(result, 0)
	require.NoError(err)
}

func (s *LocalSuite) TestSetStatus() {
	require := s.Require()
	repo := &localRepository{
		ID:       kallax.NewULID(),
		Endpoint: "foo",
		Status:   model.Pending,
	}
	s.store.repos[repo.ID] = repo
	modelRepo := repo.toRepo()

	err := s.store.SetStatus(modelRepo, model.Fetching)
	require.NoError(err)
	require.Equal(model.Fetching, modelRepo.Status)
	require.Equal(model.Fetching, s.store.repos[repo.ID].Status)
}

func (s *LocalSuite) TestSetEndpoints() {
	require := s.Require()
	repo := &localRepository{
		ID:       kallax.NewULID(),
		Endpoint: "foo",
		Status:   model.Pending,
	}
	s.store.repos[repo.ID] = repo
	modelRepo := repo.toRepo()

	err := s.store.SetEndpoints(modelRepo, "bar")
	require.NoError(err)
	require.Len(modelRepo.Endpoints, 1)
	require.Equal("bar", modelRepo.Endpoints[0])
	require.Equal("bar", s.store.repos[repo.ID].Endpoint)

	err = s.store.SetEndpoints(modelRepo, "bar", "baz")
	require.Error(err)
}

func (s *LocalSuite) TestUpdateFailed() {
	require := s.Require()
	repo := &localRepository{
		ID:       kallax.NewULID(),
		Endpoint: "foo",
		Status:   model.Fetched,
	}
	s.store.repos[repo.ID] = repo
	modelRepo := repo.toRepo()

	err := s.store.UpdateFailed(modelRepo, model.Pending)
	require.NoError(err)
	require.Equal(model.Pending, modelRepo.Status)
	require.Equal(model.Pending, s.store.repos[repo.ID].Status)
}

func (s *LocalSuite) TestUpdateFetched() {
	require := s.Require()
	repo := &localRepository{
		ID:       kallax.NewULID(),
		Endpoint: "foo",
		Status:   model.Pending,
	}
	s.store.repos[repo.ID] = repo
	modelRepo := repo.toRepo()
	time := time.Now()

	err := s.store.UpdateFetched(modelRepo, time)
	require.NoError(err)
	require.Len(modelRepo.Endpoints, 1)
	require.Equal(&time, modelRepo.FetchedAt)
	require.Equal(model.Fetched, modelRepo.Status)
	require.Equal(model.Fetched, s.store.repos[repo.ID].Status)
}

func localRefsFromInits(inits ...model.SHA1) []*localReference {
	var refs []*localReference
	for _, init := range inits {
		refs = append(refs, &localReference{Init: init})
	}

	return refs
}

func (s *LocalSuite) TestGetByInitCommit() {
	require := s.Require()

	h0 := model.NewSHA1("1000")
	h1 := model.NewSHA1("1001")
	h2 := model.NewSHA1("1002")
	h3 := model.NewSHA1("1003")

	r1 := &localRepository{
		ID:         kallax.NewULID(),
		Endpoint:   "foo",
		Status:     model.Pending,
		References: localRefsFromInits(h0, h1),
	}

	r2 := &localRepository{
		ID:         kallax.NewULID(),
		Endpoint:   "bar",
		Status:     model.Pending,
		References: localRefsFromInits(h1, h2),
	}

	s.store.repos[r1.ID] = r1
	s.store.repos[r2.ID] = r2

	// check h0, in r1

	r, err := s.store.GetRefsByInit(h0)
	require.NoError(err)
	require.Len(r, 1)
	require.Equal(r1.ID, r[0].Repository.ID)

	ok, err := s.store.InitHasRefs(h0)
	require.NoError(err)
	require.True(ok)

	// check h1, in r1 and r2

	r, err = s.store.GetRefsByInit(h1)
	require.NoError(err)
	require.Len(r, 2)

	e := []kallax.ULID{r1.ID, r2.ID}
	g := []kallax.ULID{r[0].Repository.ID, r[1].Repository.ID}
	require.ElementsMatch(e, g)

	ok, err = s.store.InitHasRefs(h1)
	require.NoError(err)
	require.True(ok)

	// check h2, in r2

	r, err = s.store.GetRefsByInit(h2)
	require.NoError(err)
	require.Len(r, 1)
	require.Equal(r2.ID, r[0].Repository.ID)

	ok, err = s.store.InitHasRefs(h2)
	require.NoError(err)
	require.True(ok)

	// check h3

	r, err = s.store.GetRefsByInit(h3)
	require.NoError(err)
	require.Len(r, 0)

	ok, err = s.store.InitHasRefs(h3)
	require.NoError(err)
	require.False(ok)
}

func TestLocal(t *testing.T) {
	suite.Run(t, new(LocalSuite))
}
